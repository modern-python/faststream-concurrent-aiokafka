---
status: draft
date: 2026-06-23
slug: pending-commits-deep-module
summary: Extract pending/watermark/owner state into a synchronous PendingCommits module; delete the committer's static test-handle delegators.
supersedes: null
superseded_by: null
pr: null
outcome: null
---

# Design: Give pending-commit bookkeeping a home (`PendingCommits`)

## Summary

`batch_committer.py` owns offset commits, but the *offset bookkeeping* it runs —
the per-partition pending lists, the pending count, the cancellation watermarks,
and the partition owners — has no module of its own. The pure offset math was
extracted to `_pending_state.py` in 0.6.0, but the state those functions mutate
stayed scattered across `_StreamingState` and the streaming loop, and three
static delegators (`_map_offsets_per_partition`, `_extract_ready_prefixes`,
`_insert_sorted`) were kept on `KafkaBatchCommitter` solely so the existing tests
could keep reaching the math through the committer. This change introduces a
synchronous `PendingCommits` module in `_pending_state.py` that owns that state
behind a small interface (`absorb` / `take_ready` / `clear_watermarks` /
`__len__`). The committer loop keeps the queue, backpressure, the
`consumer.commit()` I/O, and the transient-error re-queue. The offset invariants
get one home; the unit tests stop reaching past the committer's interface.

## Motivation

The offset logic is already pure, but the real bugs live in *how* those pure
functions are orchestrated against shared state — and that orchestration had no
interface, so the tests reached past the seam:

- **88 private-attribute pokes** in `test_kafka_committer.py` (1332 lines) —
  calls into `KafkaBatchCommitter._map_offsets_per_partition(...)`,
  `._extract_ready_prefixes(...)`, hand-seeded `committer._cancellation_watermarks[...] = 5`,
  and by-hand `_messages_queue` / `_commit_partitions` driving.
- **0 private pokes** in the 462-line `test_integration.py`, which exercises the
  same guarantees through the public interface. The contrast is the signal: the
  committer's offset behaviour is only reachable by whiteboxing it.
- The 0.6.0 extraction (this change bundle's predecessor,
  `2026-06-13.01-robustness-docs-test-audit`, Task 8) deliberately kept the
  static delegators "to keep tests untouched." That fixed the file split but
  preserved the whitebox coupling. This change finishes the job by moving
  *state*, not just functions, behind the seam.

The fragile invariant `pending_count == sum(len(v) for v in pending.values())`
is maintained today by manual bookkeeping smeared across `_streaming_iteration`
(`state.pending_count += 1`) and `_maybe_commit` (`state.pending_count -= ready_count`).
That invariant should live with the data it describes.

## Non-goals

- Folding `_StreamingState` and the streaming loop into a `CommitLoop` module —
  that is a separate candidate (#2 from the architecture review) that this split
  sets up but does not require.
- Any change to the public interface: `initialize_concurrent_processing`,
  `KafkaConcurrentProcessingMiddleware`, `ConsumerRebalanceListener`,
  `is_kafka_handler_healthy`, and the committer's collaborator surface
  (`spawn` / `close` / `send_task` / `commit_all` / `clear_cancellation_watermarks`
  / `is_healthy`) are unchanged.
- Any change to the at-least-once contract, the cancellation-as-hard-boundary
  semantics, or the rebalance-flush behaviour. This is strictly
  behaviour-preserving.
- Renaming `_pending_state.py` — the class joins the functions it orchestrates;
  the module name still describes its contents.

## Design

### 1. The seam: `PendingCommits` is synchronous, single-owner, I/O-free

`PendingCommits` reads `task.done()` / `task.cancelled()` but never `await`s,
never touches `asyncio.Queue`, and never calls Kafka. The streaming loop is the
sole mutator (one asyncio task), so no locks are needed.

What moves **in**:

| State | Today | After |
|---|---|---|
| `pending: dict[TopicPartition, list[KafkaCommitTask]]` | `_StreamingState.pending` | `PendingCommits` |
| `pending_count` | `_StreamingState.pending_count` | internal, exposed via `__len__` |
| `_cancellation_watermarks: dict[tuple[int, TopicPartition], int]` | committer | `PendingCommits` |
| `_partition_owner: dict[TopicPartition, int]` | committer | `PendingCommits` |
| offset math (`insert_sorted`, `extract_ready_prefixes`, `map_offsets_per_partition`) | `_pending_state` free fns + committer static delegators | `_pending_state` free fns (private impl), driven by `PendingCommits` |

What stays **out** (committer keeps these):

- `asyncio.Queue` (`_messages_queue`) and backpressure (`_uncommitted_count`,
  `_uncommitted_drained`, `max_uncommitted_tasks`).
- `consumer.commit()` I/O, transient-`KafkaError` re-queue, `task_done()`.
- The wakeup machinery (`_task_completed_event`, the done-callback wiring) — these
  are loop liveness, not offset state.

The re-queue path forces this seam: on transient `KafkaError` the committer puts
tasks back on `_messages_queue` and bumps `_uncommitted_count` (flow control,
not offset state); the re-queued tasks simply re-enter `PendingCommits` later
through `absorb`.

### 2. Interface

```python
@dataclasses.dataclass(frozen=True, slots=True)
class ReadyCommit:
    consumer: AIOKafkaConsumer          # recovered from the ready tasks
    offsets: dict[TopicPartition, int]  # watermark-applied, +1 already
    tasks: list[KafkaCommitTask]        # for task_done / uncommitted_count / re-queue


class PendingCommits:
    def absorb(self, ct: KafkaCommitTask) -> None: ...
        # insert sorted into pending[ct.topic_partition], bump count,
        # record partition_owner[tp] = id(ct.consumer)

    def take_ready(self) -> list[ReadyCommit]: ...
        # extract each partition's contiguous-done prefix (cancelled = hard
        # boundary), group by id(consumer), apply watermark floor, return one
        # ReadyCommit per consumer with committable work. [] when nothing ready.

    def clear_watermarks(self, partitions: Iterable[TopicPartition] | None = None) -> None: ...
        # forget watermarks (and partition owners) for `partitions`, or all if None

    def __len__(self) -> int: ...
        # total pending tasks across partitions; replaces the cached pending_count
```

`take_ready()` performs the two halves that today are split across
`_extract_ready_prefixes` (mutates `pending`) and `_map_offsets_per_partition`
(mutates watermarks) — atomically and synchronously, before any I/O — exactly
preserving today's ordering (both mutations happen before the `await
consumer.commit(...)`).

### 3. The committer loop after the change

The commit step collapses to orchestration plus I/O:

```python
new_ct = state.queue_get_task.result()
self._track_user_task(new_ct)   # loop concern: wakeup wiring (unchanged)
self._pending.absorb(new_ct)    # state concern: insert + count + owner
...
# trigger stays in the loop — PendingCommits exposes count, not policy:
if len(self._pending) >= self._commit_batch_size or timeout_fired \
        or flush_in_progress or should_shutdown:
    for rc in self._pending.take_ready():
        ok = await self._call_committer(rc)          # consumer.commit(rc.offsets); re-queue rc.tasks on KafkaError
        for _ in rc.tasks:
            self._messages_queue.task_done()
        self._uncommitted_count -= len(rc.tasks)
    self._uncommitted_drained.set()
```

`_call_committer` is reshaped to take a `ReadyCommit` (consumer + offsets +
tasks) instead of a `(tasks_batch, partitions_to_offsets)` pair. The four-way
commit trigger stays in the loop; `commit_batch_size`, the timeout deadline, the
flush event, and the shutdown flag are loop/config state that `PendingCommits`
has no business knowing.

### 4. Collaborator surfaces preserved

`clear_cancellation_watermarks` stays on `KafkaBatchCommitter` as a one-line
delegator to `self._pending.clear_watermarks(...)`. Unlike the static math
delegators, it is a **real call path** from `rebalance.py` — the deletion test
fails (delete it and `on_partitions_revoked` breaks), so it stays. `rebalance.py`
is **completely unchanged**. `KafkaCommitTask` stays in `_pending_state.py` and
remains re-exported via `batch_committer.KafkaCommitTask` (a real import path used
by `processing.py`).

### 5. What dies

The three static delegators on `KafkaBatchCommitter` —
`_map_offsets_per_partition`, `_extract_ready_prefixes`, `_insert_sorted` —
are deleted. They are shallow test handles: nothing in production calls them, the
deletion test passes (no complexity reappears), and they exist only so tests can
reach the pure functions through the committer. The cached `pending_count` field
on `_StreamingState` is removed (subsumed by `len(self._pending)`).

## Out of scope

- `CommitLoop` (review candidate #2): folding `_StreamingState`'s remaining fields
  (`queue_get_task`, `flush_wait_task`, `task_completed_wait_task`,
  `timeout_deadline`, `should_shutdown`, `flush_in_progress`) and the loop methods
  into a module that owns its own invariants. This change makes that follow-up
  cleaner but does not perform it.

## Testing

- **Pure offset functions** keep tight, isolated unit tests, but called directly
  against `_pending_state.insert_sorted` / `extract_ready_prefixes` /
  `map_offsets_per_partition` — not through the deleted committer delegators.
- **Watermark / state tests** move to `PendingCommits`' interface: instead of
  hand-seeding `committer._cancellation_watermarks[(id, tp)] = 5`, a test
  `absorb`s a cancelled task to *establish* the floor, then asserts `take_ready()`
  withholds the partition's advance and that a `clear_watermarks` + later
  `take_ready()` resumes it — exercising the real mutation path.
- **Committer tests** (`_commit_partitions` / `_messages_queue` driving) shrink to
  the committer's genuine job: "commit these `ReadyCommit`s, re-queue on transient
  error, balance `task_done`/`uncommitted_count`."
- **Regression net:** all of `test_integration.py` (462 lines, real Redpanda),
  `test_rebalance.py`, and `test_middleware.py` pass **untouched** — the public
  interface and at-least-once semantics are unchanged. Green integration is the
  proof of behaviour preservation. `just lint` (ruff + ty) and 100% coverage
  enforced as today.

## Risk

- **Multi-consumer-same-partition regression** (low likelihood × high impact).
  Two consumer groups on one `TopicPartition` share a single pending list keyed by
  `TopicPartition`; today the flat extract is grouped by `id(consumer)` *after* the
  contiguous-prefix walk. `take_ready` must preserve that order: walk the prefix
  per partition first, *then* group by consumer, *then* map offsets per consumer
  with the watermark floor. Covered by `test_commit_partitions_handles_multiple_consumers`
  and the integration multi-subscriber tests.
- **Watermark timing drift** (low × high). The watermark must be recorded
  synchronously *before* the `consumer.commit()` await, as today. `take_ready`
  doing extract+map in one synchronous call preserves this; the risk is only if a
  future edit inserts an `await` inside `take_ready` (the I/O-free invariant
  forbids it).
- **Count desync** (low × medium). `__len__` must stay exact across `absorb`
  (+1) and `take_ready` (−ready_count). Maintaining it inside `PendingCommits`
  alongside the mutations is *less* error-prone than today's split bookkeeping,
  but it is the invariant to assert in unit tests.
