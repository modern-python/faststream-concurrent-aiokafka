---
status: draft
date: 2026-06-24
slug: commit-scheduler-deep-module
summary: Extract the streaming loop's when-to-commit decision state into a pure synchronous CommitScheduler; the committer keeps the async driver.
supersedes: null
superseded_by: null
pr: null
outcome: null
---

# Design: Extract the commit-timing decisions into `CommitScheduler`

## Summary

`KafkaBatchCommitter`'s streaming loop owns *when* to commit — the timeout
deadline, the flush lifecycle, the shutdown lifecycle, and the batch-size
trigger — as a `_StreamingState` dataclass whose four invariants are documented
in a docstring and maintained from outside, inline in `_streaming_iteration`.
This change extracts those decisions into a new **pure synchronous**
`CommitScheduler` (`_commit_scheduler.py`) that owns the decision state and the
methods that transition it, so the invariants are *enforced* rather than
annotated. The committer keeps the async driver (the `asyncio.wait` select, the
three wait-tasks, the queue, backpressure, the commit I/O). It is the *when*
counterpart to `PendingCommits` (the *what*), and completes the split of
`batch_committer.py` that the [pending-commits-deep-module](../2026-06-23.01-pending-commits-deep-module/design.md)
change set up.

## Motivation

The [pending-commits change](../2026-06-23.01-pending-commits-deep-module/design.md)
moved *what to commit* behind an interface but explicitly left *when to commit*
(`_StreamingState` + the loop methods) as a follow-up. That residue is the
last fragile part of `batch_committer.py`:

- `_StreamingState` exposes nine mutable fields and documents four invariants in
  a docstring (`pending empty ⇒ timeout_deadline is None`; `flush_in_progress`
  set only on flush-without-stop, cleared on drain; `should_shutdown` set only
  on flush-with-stop; the trigger predicate). The fields are written **inline**
  in `_streaming_iteration` / `_handle_flush_fired` / `_maybe_commit`, so the
  invariants depend on the *order* of those inline writes — a future edit can
  silently break one.
- The decision logic has **no unit tests**. Today it is covered only by the
  integration (Redpanda) suite and the committer-level behavioral tests; there
  is one reference to a loop method in the unit tests and it merely mocks
  `_run_commit_process` out. Extracting the decider creates brand-new
  unit-test surface for logic that currently has none — with **no** whitebox
  pokes to repoint (unlike the 88 in the pending-commits change).

## Non-goals

- Moving the async machinery. The `asyncio.wait` select, the three wait-tasks
  (`queue_get` / `flush_wait` / `task_completed_wait`), the `_run_commit_process`
  driver, and the `_streaming_iteration` orchestration stay in
  `batch_committer.py`. They carry no documented invariant and gain no
  testability from relocation.
- Touching the produce side, backpressure, queue, or commit I/O.
  `send_task`, `_uncommitted_count` / `_uncommitted_drained` /
  `_max_uncommitted_tasks`, `_messages_queue`, `_call_committer` / `_commit_ready`,
  `spawn` / `close` / `is_healthy`, and the public API are unchanged.
- Changing the `task_completed` event handling. It is a pure wakeup with no
  decision content; it stays entirely in the driver and never crosses the seam.
- Any behavior change. Same iteration semantics, same commit timing, same
  shutdown sequence. Strictly behavior-preserving.

## Design

### 1. The seam: a pure synchronous decider

`CommitScheduler` is synchronous, I/O-free, and single-owner (the driver is its
sole caller, on one asyncio task). It reads no clock and touches no asyncio
object — the driver passes `now = loop.time()` in. It owns exactly the three
decision fields that carry invariants:

| Field | Today | After |
|---|---|---|
| `timeout_deadline: float \| None` | `_StreamingState` | `CommitScheduler` |
| `should_shutdown: bool` | `_StreamingState` | `CommitScheduler` |
| `flush_in_progress: bool` | `_StreamingState` | `CommitScheduler` |

The three `*_wait_task` fields stay in the driver (they have no invariant). The
**driver never writes a decision field** — it only feeds observations and acts
on the returned `Decision`. That is what turns the four invariants from
documented-and-externally-maintained into enforced-in-one-place.

### 2. Interface

```python
@dataclasses.dataclass(frozen=True, slots=True)
class Decision:
    should_commit: bool     # the batch-size/timeout/flush/shutdown trigger fired
    drain_queue_now: bool   # flush fired with stop_requested → driver drains the queue into pending
    timeout_fired: bool     # surfaced so the driver can hand it back to note_committed


class CommitScheduler:
    def __init__(self, *, commit_batch_size: int, commit_batch_timeout_sec: float) -> None: ...

    # --- pre-await reads (driver builds the select) ---
    def accepts_new_work(self) -> bool:
        # not should_shutdown → driver includes queue_get in the wait targets
    def wait_timeout(self, now: float) -> float | None:
        # max(deadline - now, 0.0) if a deadline is armed else None → asyncio.wait timeout

    # --- observe → decide (every decision-field write happens here) ---
    def evaluate(self, *, now: float, absorbed: bool, flush_fired: bool,
                 stop_requested: bool, pending_len: int) -> Decision

    # --- post-commit settle (deadline reset + flush_in_progress clear) ---
    def note_committed(self, *, now: float, committed: bool,
                       timeout_fired: bool, pending_empty: bool) -> None

    # --- loop condition ---
    def is_finished(self, *, pending_empty: bool) -> bool   # should_shutdown and pending_empty
```

`evaluate` performs, in order: arm the deadline if `absorbed` and none is armed
(`deadline = now + batch_timeout`); compute `timeout_fired`; on `flush_fired`,
transition the flags (`stop_requested` → `should_shutdown = True` and signal
`drain_queue_now`; else `flush_in_progress = True`); compute the trigger
`pending_len >= batch_size or timeout_fired or flush_in_progress or should_shutdown`.
`note_committed` clears `flush_in_progress` when `pending_empty`, then resets the
deadline (`now + batch_timeout` if pending remains, else `None`) when
`committed or timeout_fired`.

`Decision.timeout_fired` is handed back to `note_committed` rather than stashed
as transient cross-call state — keeping the data flow explicit and the scheduler
free of hidden ordering coupling between the two calls.

### 3. The driver after the change

`_streaming_iteration` becomes pure glue with no flag writes. One iteration:

| Step | Owner | Call |
|---|---|---|
| build wait targets | driver | `[flush_wait, task_completed_wait] + ([queue_get] if sched.accepts_new_work() else [])` |
| compute select timeout | driver→sched | `sched.wait_timeout(loop.time())` |
| `await asyncio.wait(...)` | driver | — |
| absorb a queue item | driver | `track`; `pending.absorb`; recreate `queue_get` |
| re-arm `task_completed` | driver | (pure wakeup) |
| decide | sched | `dec = sched.evaluate(now, absorbed, flush_fired, stop_requested, len(pending))` |
| flush event re-arm | driver | clear + recreate `flush_wait` |
| shutdown drain | driver | if `dec.drain_queue_now`: drain queue into pending; cancel `queue_get` |
| commit | driver | if `dec.should_commit`: `ready = pending.take_ready()`; `await _commit_ready(ready)`; `committed = bool(ready)` |
| settle | sched | `sched.note_committed(loop.time(), committed, dec.timeout_fired, not pending)` |

The driver loop condition becomes `while not sched.is_finished(pending_empty=not pending)`.
`_StreamingState` shrinks to the three wait-tasks (or is inlined into the driver
as locals/`_run_commit_process` state); `_maybe_commit` collapses into the driver
step above.

### 4. Behaviour preservation

The decomposition reproduces today's `_streaming_iteration` exactly, including
the subtleties:

- The deadline is armed inside `evaluate` using the **post-await** `now`, only
  when a queue item was absorbed and no deadline is armed — same as the current
  absorb-block arm.
- The deadline reset uses a **fresh** post-commit `now` and fires on
  `committed OR timeout_fired` — hence `timeout_fired` is threaded through
  `Decision` into `note_committed`.
- `flush_in_progress` is cleared on `pending_empty` measured **after** the
  commit, before the deadline reset — same order as lines 191–197 today.
- The flush wait-task is always cleared-and-recreated when `flush_fired`, in
  both the stop and non-stop branches; the queue drain and `queue_get`
  cancellation happen only on the stop branch (`drain_queue_now`).

## Out of scope

- A `run()`-owning async `CommitLoop`. Q1 rejected moving the async loop into the
  module; the wait-tasks gain no testability from relocation and would force the
  module to be async, weakening the unit-test story.

## Testing

- New `tests/test_commit_scheduler.py` unit-tests the decider by feeding
  observation sequences with **zero asyncio**: deadline arm-on-first-absorb and
  the `pending empty ⇒ deadline None` invariant; `timeout_fired` at/before/after
  the deadline; the flush transition for stop vs non-stop; `flush_in_progress`
  set-then-cleared-on-drain; `should_shutdown` set only on flush-with-stop and
  `is_finished` only when also `pending_empty`; the trigger predicate across all
  four causes; the `committed OR timeout_fired` reset condition.
- The async driver stays integration-tested. `test_integration.py`,
  `test_rebalance.py`, `test_middleware.py`, and the committer-level tests in
  `test_kafka_committer.py` pass **untouched** — the proof of behaviour
  preservation. `CommitScheduler` is internal (not in `__init__.py`'s `__all__`).
  100% coverage enforced; `just lint` (ruff + ty) clean.

## Risk

- **Iteration-order drift** (low likelihood × high impact). The invariants
  depend on the exact order of arm → timeout_fired → flush-transition → trigger
  within `evaluate`, and clear-flush → reset-deadline within `note_committed`.
  Mitigation: the ordering now lives in one place (the two methods), unit-tested
  directly; the integration suite catches any behavioral regression.
- **Split-call coupling** (low × medium). `evaluate` and `note_committed` must be
  called once each per iteration, in order, with the commit between. The driver
  is the sole caller and the `Decision` return threads the dependency explicitly
  (`timeout_fired`), but a future driver edit could call them out of order.
  Mitigation: unit tests assert the paired-call behavior; the driver is small
  and the two calls bracket the single `await _commit_ready`.
- **Hidden reliance on `_StreamingState` field defaults** (low × low). Moving the
  three decision fields must preserve their initial values (`timeout_deadline
  None`, both flags `False`). Mitigation: `CommitScheduler.__init__` sets them
  explicitly; covered by the first unit test.
