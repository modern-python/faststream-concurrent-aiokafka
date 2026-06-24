---
status: draft
date: 2026-06-23
slug: pending-commits-deep-module
spec: pending-commits-deep-module
pr: null
---

# pending-commits-deep-module — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the committer's pending lists, pending count, cancellation
watermarks, and partition owners behind a synchronous `PendingCommits` module so
the offset invariants have one home and the unit tests stop reaching past the
committer's interface.

**Spec:** [`design.md`](./design.md)

**Branch:** `refactor/pending-commits-deep-module`

**Commit strategy:** Per-task commits.

## Global Constraints

- Python ≥ 3.11. No `from __future__ import annotations`; annotations evaluate
  eagerly; `typing.Self`/`typing.Never` used directly.
- All imports at module level — never inside function bodies.
- Type suppression is `# ty: ignore[rule-name]`, never `# type: ignore`.
- `PendingCommits` is **synchronous and I/O-free**: it reads `task.done()` /
  `task.cancelled()` but never `await`s, never touches `asyncio.Queue`, never
  calls Kafka.
- `PendingCommits` is **internal**: not added to `__init__.py`'s `__all__`.
- Public interface unchanged: `initialize_concurrent_processing`,
  `KafkaConcurrentProcessingMiddleware`, `ConsumerRebalanceListener`,
  `is_kafka_handler_healthy`, and the committer's collaborator surface
  (`spawn` / `close` / `send_task` / `commit_all` /
  `clear_cancellation_watermarks` / `is_healthy`).
- Strictly behaviour-preserving: same offsets committed, same redelivery on
  cancellation, same rebalance flush. `test_integration.py`, `test_rebalance.py`,
  and `test_middleware.py` pass **untouched**.
- 100% coverage enforced (pytest-cov). `just lint` (ruff + ty) must pass.
- Unit tests run without Docker: `uv run --no-sync pytest tests/<file> -v`.
  The full gate (integration vs Redpanda) runs via `just test`.

---

### Task 1: `PendingCommits` + `ReadyCommit` in `_pending_state.py` (TDD, new test file)

**Files:**
- Modify: `faststream_concurrent_aiokafka/_pending_state.py`
- Test: `tests/test_pending_commits.py` (create)

**Interfaces:**
- Consumes: existing module-level `insert_sorted`, `extract_ready_prefixes`,
  `map_offsets_per_partition`, `KafkaCommitTask` from `_pending_state.py`;
  `MockAIOKafkaConsumer` and `MockAsyncioTask` from `tests/mocks.py`.
- Produces (relied on by Task 2):
  - `tests/mocks.py::make_commit_task(...)` — a shared `KafkaCommitTask` builder
    (new; replaces the inline `KafkaCommitTask(asyncio_task=MockAsyncioTask(...))`
    pattern duplicated across `test_kafka_committer.py`).
  - `class ReadyCommit` — frozen dataclass with `consumer: Any`,
    `offsets: dict[TopicPartition, int]`, `tasks: list[KafkaCommitTask]`.
  - `class PendingCommits` with:
    - `absorb(self, ct: KafkaCommitTask) -> None`
    - `take_ready(self) -> list[ReadyCommit]`
    - `clear_watermarks(self, partitions: typing.Iterable[TopicPartition] | None = None) -> None`
    - `__len__(self) -> int`

- [ ] **Step 1: Add a shared `make_commit_task` builder to `tests/mocks.py`**

  `tests/mocks.py` already has `MockAIOKafkaConsumer` and `MockAsyncioTask`
  (`done()`/`cancelled()` controllable), but committer tests build
  `KafkaCommitTask` inline. Add one builder both test files share:

  ```python
  from faststream.kafka import TopicPartition
  from faststream_concurrent_aiokafka._pending_state import KafkaCommitTask


  def make_commit_task(
      consumer: MockAIOKafkaConsumer,
      topic_partition: TopicPartition,
      offset: int,
      *,
      done: bool = False,
      cancelled: bool = False,
  ) -> KafkaCommitTask:
      return KafkaCommitTask(
          asyncio_task=MockAsyncioTask(result="ok", done=done, cancelled=cancelled),  # ty: ignore[invalid-argument-type]
          topic_partition=topic_partition,
          offset=offset,
          consumer=consumer,
      )
  ```

  (The `# ty: ignore[invalid-argument-type]` matches the existing inline pattern —
  `MockAsyncioTask` is not a real `asyncio.Task`.)

- [ ] **Step 2: Write the failing tests for `PendingCommits`**

  Create `tests/test_pending_commits.py`. The first lines define the shared
  `mock_consumer` fixture (the existing one is local to `test_kafka_committer.py`):

  ```python
  @pytest.fixture
  def mock_consumer() -> MockAIOKafkaConsumer:
      return MockAIOKafkaConsumer()
  ```

  Create `tests/test_pending_commits.py`. These tests target the new interface
  directly — no committer, no queue, no I/O.

  ```python
  import asyncio
  import typing

  import pytest
  from faststream.kafka import TopicPartition

  from faststream_concurrent_aiokafka import _pending_state
  from faststream_concurrent_aiokafka._pending_state import PendingCommits, ReadyCommit
  from tests.mocks import MockAIOKafkaConsumer, make_commit_task  # reuse existing helpers


  def _tp(partition: int = 0, topic: str = "t") -> TopicPartition:
      return TopicPartition(topic=topic, partition=partition)


  def test_len_counts_absorbed_tasks(mock_consumer: MockAIOKafkaConsumer) -> None:
      pending = PendingCommits()
      assert len(pending) == 0
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, done=False))
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, done=False))
      assert len(pending) == 2


  def test_take_ready_empty_returns_empty_list() -> None:
      assert PendingCommits().take_ready() == []


  def test_take_ready_commits_contiguous_done_prefix(mock_consumer: MockAIOKafkaConsumer) -> None:
      pending = PendingCommits()
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, done=True))
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, done=True))
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=2, done=False))

      ready = pending.take_ready()

      assert len(ready) == 1
      rc = ready[0]
      assert rc.consumer is mock_consumer
      assert rc.offsets == {_tp(): 2}          # max processed (1) + 1
      assert len(rc.tasks) == 2                 # only the done prefix
      assert len(pending) == 1                  # offset 2 still pending


  def test_take_ready_stops_at_first_not_done(mock_consumer: MockAIOKafkaConsumer) -> None:
      pending = PendingCommits()
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, done=False))
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, done=True))
      assert pending.take_ready() == []         # head not done → nothing ready
      assert len(pending) == 2


  def test_cancelled_task_is_hard_boundary(mock_consumer: MockAIOKafkaConsumer) -> None:
      pending = PendingCommits()
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, done=True))
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, cancelled=True))
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=2, done=True))

      ready = pending.take_ready()
      rc = ready[0]

      assert rc.offsets == {_tp(): 1}           # advance stops at the cancelled task
      assert len(rc.tasks) == 3                 # cancelled + after dropped from pending into ready
      assert len(pending) == 0


  def test_watermark_blocks_advance_until_cleared(mock_consumer: MockAIOKafkaConsumer) -> None:
      pending = PendingCommits()
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, cancelled=True))
      pending.take_ready()                       # records the (consumer, tp) watermark at 0

      # A later done task on the same partition must not advance past the floor.
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, done=True))
      assert pending.take_ready()[0].offsets == {}   # withheld

      pending.clear_watermarks([_tp()])
      pending.absorb(make_commit_task(mock_consumer, _tp(), offset=2, done=True))
      assert pending.take_ready()[0].offsets == {_tp(): 3}   # resumes after clear


  def test_clear_watermarks_all_when_none(mock_consumer: MockAIOKafkaConsumer) -> None:
      pending = PendingCommits()
      pending.absorb(make_commit_task(mock_consumer, _tp(0), offset=0, cancelled=True))
      pending.absorb(make_commit_task(mock_consumer, _tp(1), offset=0, cancelled=True))
      pending.take_ready()
      pending.clear_watermarks()                 # None → clear all
      pending.absorb(make_commit_task(mock_consumer, _tp(0), offset=1, done=True))
      assert pending.take_ready()[0].offsets == {_tp(0): 2}


  def test_two_consumers_same_partition_commit_independently() -> None:
      a, b = MockAIOKafkaConsumer(), MockAIOKafkaConsumer()
      pending = PendingCommits()
      pending.absorb(make_commit_task(a, _tp(), offset=0, done=True))
      pending.absorb(make_commit_task(b, _tp(), offset=1, done=True))

      ready = {id(rc.consumer): rc for rc in pending.take_ready()}

      assert ready[id(a)].offsets == {_tp(): 1}
      assert ready[id(b)].offsets == {_tp(): 2}
  ```

- [ ] **Step 3: Run the tests to verify they fail**

  Run: `uv run --no-sync pytest tests/test_pending_commits.py -v`
  Expected: FAIL — `ImportError: cannot import name 'PendingCommits'`.

- [ ] **Step 4: Implement `ReadyCommit` and `PendingCommits`**

  Append to `faststream_concurrent_aiokafka/_pending_state.py` (the module-level
  `insert_sorted` / `extract_ready_prefixes` / `map_offsets_per_partition` stay as
  the private implementation the class drives):

  ```python
  @dataclasses.dataclass(frozen=True, slots=True)
  class ReadyCommit:
      # consumer typed Any to match KafkaCommitTask.consumer (avoids importing aiokafka at runtime)
      consumer: typing.Any
      offsets: dict[TopicPartition, int]
      tasks: list[KafkaCommitTask]


  class PendingCommits:
      """Owns the per-partition pending commit tasks, the pending count, the
      cancellation watermarks, and the partition owners.

      Synchronous and single-owner: the committer's streaming loop is the sole
      mutator, so no locking is needed. Reads asyncio task state (done/cancelled)
      but never awaits and never performs I/O.
      """

      def __init__(self) -> None:
          self._pending: dict[TopicPartition, list[KafkaCommitTask]] = {}
          self._count: int = 0
          self._watermarks: dict[tuple[int, TopicPartition], int] = {}
          self._partition_owner: dict[TopicPartition, int] = {}

      def __len__(self) -> int:
          return self._count

      def absorb(self, ct: KafkaCommitTask) -> None:
          insert_sorted(self._pending.setdefault(ct.topic_partition, []), ct)
          self._partition_owner[ct.topic_partition] = id(ct.consumer)
          self._count += 1

      def take_ready(self) -> list[ReadyCommit]:
          # Extract each partition's contiguous-done prefix (cancelled = hard
          # boundary), then group by consumer and apply the watermark floor.
          # Atomic and synchronous: pending + watermark mutation both happen here,
          # before any I/O the committer performs on the returned offsets.
          ready, ready_count = extract_ready_prefixes(self._pending)
          self._count -= ready_count
          flat: list[KafkaCommitTask] = [t for tasks in ready.values() for t in tasks]
          if not flat:
              return []
          by_consumer: dict[int, list[KafkaCommitTask]] = {}
          for task in flat:
              by_consumer.setdefault(id(task.consumer), []).append(task)
          result: list[ReadyCommit] = []
          for consumer_id, tasks in by_consumer.items():
              offsets = map_offsets_per_partition(consumer_id, tasks, self._watermarks)
              result.append(ReadyCommit(consumer=tasks[0].consumer, offsets=offsets, tasks=tasks))
          return result

      def clear_watermarks(self, partitions: typing.Iterable[TopicPartition] | None = None) -> None:
          if partitions is None:
              self._watermarks.clear()
              self._partition_owner.clear()
              return
          for partition in partitions:
              owner = self._partition_owner.pop(partition, None)
              if owner is not None:
                  self._watermarks.pop((owner, partition), None)
  ```

- [ ] **Step 5: Run the tests to verify they pass**

  Run: `uv run --no-sync pytest tests/test_pending_commits.py -v`
  Expected: PASS (all tests).

- [ ] **Step 6: Lint**

  Run: `just lint`
  Expected: clean (ruff format/check + ty).

- [ ] **Step 7: Commit**

  ```bash
  git add faststream_concurrent_aiokafka/_pending_state.py tests/test_pending_commits.py tests/mocks.py
  git commit -m "refactor: add PendingCommits module owning pending/watermark state

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Wire `PendingCommits` into the committer; reshape the commit path

**Files:**
- Modify: `faststream_concurrent_aiokafka/batch_committer.py`
- Test: `tests/test_kafka_committer.py` (update committer-direct tests that touch
  removed internals)

**Interfaces:**
- Consumes: `PendingCommits`, `ReadyCommit` from Task 1.
- Produces (relied on by Task 3/4):
  - `KafkaBatchCommitter._call_committer(self, rc: ReadyCommit) -> bool`
  - `KafkaBatchCommitter._commit_ready(self, ready_commits: list[ReadyCommit]) -> bool`
  - `KafkaBatchCommitter.clear_cancellation_watermarks` delegates to
    `self._pending.clear_watermarks`.
  - `self._pending: PendingCommits` replaces `_cancellation_watermarks`,
    `_partition_owner`, and `_StreamingState.pending` / `pending_count`.

This task is atomic: the offset state lives in one place, so the swap and the
commit-path reshape land together. The existing integration/rebalance/middleware
suites are the regression net.

- [ ] **Step 1: Replace committer state and the streaming-state fields**

  In `KafkaBatchCommitter.__init__`, delete `self._cancellation_watermarks` and
  `self._partition_owner`; add:

  ```python
  self._pending: typing.Final = _pending_state.PendingCommits()
  ```

  In `_StreamingState`, delete the `pending` and `pending_count` fields (and the
  two lines of their docstring invariants — the count invariant now lives inside
  `PendingCommits`).

- [ ] **Step 2: Point absorb/length/commit-trigger at `self._pending`**

  In `_run_commit_process`, change the loop guard:

  ```python
  while not (state.should_shutdown and not self._pending):
  ```

  In `_streaming_iteration`, the absorb block:

  ```python
  if not state.should_shutdown and state.queue_get_task.done():
      new_ct = state.queue_get_task.result()
      self._track_user_task(new_ct)
      self._pending.absorb(new_ct)
      state.queue_get_task = asyncio.create_task(self._messages_queue.get())
      if state.timeout_deadline is None:
          state.timeout_deadline = now + self._commit_batch_timeout_sec
  ```

  And the flush/deadline tail of `_streaming_iteration`:

  ```python
  ready = await self._maybe_commit(state, timeout_fired)
  if state.flush_in_progress and not self._pending:
      state.flush_in_progress = False

  if ready or timeout_fired:
      state.timeout_deadline = (loop.time() + self._commit_batch_timeout_sec) if self._pending else None
  ```

  In `_handle_flush_fired`, the drain loop:

  ```python
  while True:
      try:
          ct = self._messages_queue.get_nowait()
      except asyncio.QueueEmpty:
          break
      self._track_user_task(ct)
      self._pending.absorb(ct)
  ```

- [ ] **Step 3: Reshape `_maybe_commit` to use `take_ready`**

  ```python
  async def _maybe_commit(self, state: "_StreamingState", timeout_fired: bool) -> list[_pending_state.ReadyCommit]:
      commit_triggered: typing.Final = (
          len(self._pending) >= self._commit_batch_size
          or timeout_fired
          or state.flush_in_progress
          or state.should_shutdown
      )
      if not commit_triggered:
          return []
      ready_commits: typing.Final = self._pending.take_ready()
      if ready_commits:
          await self._commit_ready(ready_commits)
      return ready_commits
  ```

- [ ] **Step 4: Replace `_commit_partitions` with `_commit_ready` and reshape `_call_committer`**

  Delete `_commit_partitions`. Add `_commit_ready`, which keeps the queue/count
  bookkeeping and the concurrent per-consumer commit:

  ```python
  async def _commit_ready(self, ready_commits: list[_pending_state.ReadyCommit]) -> bool:
      # One commit per consumer, concurrently — each AIOKafkaConsumer commits its
      # own partitions. task_done()/uncommitted_count balance the queue regardless
      # of commit success (re-queued tasks are re-counted inside _call_committer).
      results: typing.Final = await asyncio.gather(*(self._call_committer(rc) for rc in ready_commits))
      for rc in ready_commits:
          for _ in rc.tasks:
              self._messages_queue.task_done()
      self._uncommitted_count -= sum(len(rc.tasks) for rc in ready_commits)
      self._uncommitted_drained.set()
      return all(results)
  ```

  Reshape `_call_committer` to take a `ReadyCommit`:

  ```python
  async def _call_committer(self, rc: _pending_state.ReadyCommit) -> bool:
      if not rc.offsets:
          return True
      try:
          await rc.consumer.commit(rc.offsets)
      except (CommitFailedError, IllegalStateError):
          logger.exception("Cannot commit due to partition loss or rebalancing, ignoring batch")
          return False
      except KafkaError:
          logger.exception("Error during commit to kafka, re-queuing batch")
          for task in rc.tasks:
              self._uncommitted_count += 1
              await self._messages_queue.put(task)
          return False
      else:
          return True
  ```

- [ ] **Step 5: Delegate watermark clearing; drop the static math delegators' call sites**

  Replace the body of `clear_cancellation_watermarks` (keep the method — it is a
  real call path from `rebalance.py`):

  ```python
  def clear_cancellation_watermarks(self, partitions: typing.Iterable[TopicPartition] | None = None) -> None:
      self._pending.clear_watermarks(partitions)
  ```

  Leave the three static delegators (`_insert_sorted` module function,
  `_extract_ready_prefixes`, `_map_offsets_per_partition` static methods) in place
  for now — they are dead after this task but deleting them and repointing their
  tests is Task 3. (This keeps Task 2 focused on behaviour, Task 3 on the seam.)

- [ ] **Step 6: Update the committer-direct tests that referenced removed internals**

  In `tests/test_kafka_committer.py`:
  - **`test_call_committer_*`** (the `_call_committer([...], {...})` tests): rewrite
    to pass a `ReadyCommit`. Canonical transform:

    ```python
    # before: result = await committer._call_committer([sample_task], partitions_to_offsets)
    rc = _pending_state.ReadyCommit(consumer=sample_task.consumer, offsets=partitions_to_offsets, tasks=[sample_task])
    result = await committer._call_committer(rc)
    ```
    The transient-`KafkaError` re-queue assertion (`committer._messages_queue` gets
    the task back) is unchanged — `_call_committer` still re-queues `rc.tasks`.
  - **`test_commit_partitions_*`**: rename target to `_commit_ready` and pass a
    `list[ReadyCommit]`. Canonical transform:

    ```python
    # before: await committer._commit_partitions({tp: tasks})
    rc = _pending_state.ReadyCommit(
        consumer=tasks[0].consumer,
        offsets=_pending_state.map_offsets_per_partition(id(tasks[0].consumer), tasks, {}),
        tasks=tasks,
    )
    await committer._commit_ready([rc])
    ```
    Keep the multi-consumer test (`test_commit_partitions_handles_multiple_consumers`)
    — build one `ReadyCommit` per consumer and pass the list; it guards the
    same-partition/two-consumer path.
  - **`test_clear_cancellation_watermarks_*`**: these hand-seed
    `committer._cancellation_watermarks[...]`, which no longer exists. Delete them
    from this file — equivalent coverage now lives in
    `tests/test_pending_commits.py` (`test_watermark_blocks_advance_until_cleared`,
    `test_clear_watermarks_all_when_none`).

- [ ] **Step 7: Run the unit suite**

  Run: `uv run --no-sync pytest tests/test_kafka_committer.py tests/test_pending_commits.py -v`
  Expected: PASS. If any test still references `committer._cancellation_watermarks`,
  `_partition_owner`, `_commit_partitions`, or `state.pending`, fix per Step 6.

- [ ] **Step 8: Full gate (integration must be untouched and green)**

  Run: `just test`
  Expected: PASS, including all of `test_integration.py`, `test_rebalance.py`,
  `test_middleware.py` **with no edits to those files**. This is the proof the
  refactor is behaviour-preserving.

- [ ] **Step 9: Lint + commit**

  ```bash
  just lint
  git add faststream_concurrent_aiokafka/batch_committer.py tests/test_kafka_committer.py
  git commit -m "refactor: route committer offset bookkeeping through PendingCommits

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Delete the static math delegators; repoint their tests to `_pending_state`

**Files:**
- Modify: `faststream_concurrent_aiokafka/batch_committer.py`
- Test: `tests/test_kafka_committer.py`

The three delegators are shallow test handles: nothing in production calls them
after Task 2. The deletion test passes — removing them concentrates no
complexity; it only severs the committer from being the test door for the offset
math.

- [ ] **Step 1: Confirm the delegators are dead in production**

  Run: `rg -n "_insert_sorted|_extract_ready_prefixes|_map_offsets_per_partition" faststream_concurrent_aiokafka/`
  Expected: matches only in their own definitions in `batch_committer.py` (no
  remaining call sites in production code). If `take_ready`/`absorb` call the
  module-level `_pending_state.*` functions, the committer copies are unused.

- [ ] **Step 2: Repoint the pure-function tests to `_pending_state` directly**

  In `tests/test_kafka_committer.py`, the `_map_offsets_per_partition` and
  `_extract_ready_prefixes` test groups currently call the static methods.
  Mechanical repoint (same args, same assertions):

  ```python
  # before: KafkaBatchCommitter._map_offsets_per_partition(id(mock_consumer), tasks, {})
  _pending_state.map_offsets_per_partition(id(mock_consumer), tasks, {})

  # before: KafkaBatchCommitter._extract_ready_prefixes(pending)
  _pending_state.extract_ready_prefixes(pending)
  ```

  Apply to every test in the `# ---------- _map_offsets_per_partition ----------`
  and `# ---------- _extract_ready_prefixes ----------` sections. These tests keep
  the intricate offset-math coverage (earliest-cancelled-wins, contiguous prefix,
  `max+1`, per-consumer watermark isolation) — now through the function that owns
  it, not the committer.

- [ ] **Step 3: Delete the three static delegators**

  Remove from `batch_committer.py`:
  - the module-level `_insert_sorted` function,
  - the `_map_offsets_per_partition` static method,
  - the `_extract_ready_prefixes` static method.

  Remove any now-unused imports flagged by ruff.

- [ ] **Step 4: Run the unit suite**

  Run: `uv run --no-sync pytest tests/test_kafka_committer.py tests/test_pending_commits.py -v`
  Expected: PASS. Failures here mean a test still calls a deleted delegator — fix
  per Step 2.

- [ ] **Step 5: Lint + commit**

  ```bash
  just lint
  git add faststream_concurrent_aiokafka/batch_committer.py tests/test_kafka_committer.py
  git commit -m "refactor: delete committer static delegators; test offset math directly

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Seal — coverage gate, full integration, architecture promotion

**Files:**
- Modify: `architecture/batch-committer.md`
- (Verification only on the rest.)

Per the project workflow, promoting conclusions into `architecture/` is the
ship-time hand-edit that keeps it true.

- [ ] **Step 1: Confirm no whitebox pokes remain past the seam**

  Run: `rg -n "committer\._cancellation_watermarks|committer\._partition_owner|committer\._commit_partitions|\._StreamingState|state\.pending" tests/`
  Expected: no matches. Remaining `committer._messages_queue` references in the
  `_commit_ready` / `send_task` tests are fine — the queue is genuinely the
  committer's, tested at the committer's interface.

- [ ] **Step 2: 100% coverage gate**

  Run: `just test-branch`
  Expected: PASS at 100% coverage. If `PendingCommits` has an uncovered branch
  (e.g. `clear_watermarks(None)` vs the partition path, or the empty-`offsets`
  early-return in `_call_committer`), add the missing case to
  `tests/test_pending_commits.py` and re-run.

- [ ] **Step 3: Promote conclusions into `architecture/batch-committer.md`**

  Update the doc to reflect the shipped invariants:
  - The committer delegates pending/watermark/owner state to a synchronous
    `PendingCommits` (`_pending_state.py`); the committer keeps the queue,
    backpressure, the `consumer.commit()` I/O, and the transient-error re-queue.
  - `take_ready()` performs extraction + per-consumer offset mapping atomically
    before any I/O, returning `ReadyCommit`s; the contiguous-prefix and
    cancelled-as-hard-boundary rules now live in `PendingCommits`.
  - `clear_cancellation_watermarks` is a thin delegator to
    `PendingCommits.clear_watermarks` on the rebalance path.

- [ ] **Step 4: Final lint + full gate**

  Run: `just lint && just test`
  Expected: clean + all green.

- [ ] **Step 5: Commit**

  ```bash
  git add architecture/batch-committer.md
  git commit -m "docs(architecture): record PendingCommits seam in batch-committer

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

## Self-review notes (for the executor)

- **Spec coverage:** Task 1 = `PendingCommits` interface (design §1–§2); Task 2 =
  loop wiring + commit-path reshape + delegator preservation (§3–§4); Task 3 =
  static-delegator deletion (§5); Task 4 = test-door migration verification +
  coverage + architecture promotion (Testing/Risk). `rebalance.py` is intentionally
  never edited (Non-goal).
- **Behaviour preservation hinge:** `take_ready` must do extract → group-by-consumer
  → `map_offsets_per_partition` with **no `await`** between them, exactly as today's
  `_extract_ready_prefixes` → `_commit_partitions` ordering. The I/O-free constraint
  enforces this.
- **Watermark timing:** recorded inside `take_ready` (synchronous) before the
  committer's `consumer.commit()` await — same as today's record-before-commit.
- **Count invariant:** `__len__` decremented by the full `ready_count` from
  `extract_ready_prefixes` (includes cancelled+after dropped into ready), matching
  today's `state.pending_count -= ready_count`.
