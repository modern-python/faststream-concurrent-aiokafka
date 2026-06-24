---
status: shipped
date: 2026-06-24
slug: commit-scheduler-deep-module
spec: commit-scheduler-deep-module
pr: "40"
---

# commit-scheduler-deep-module — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the streaming loop's when-to-commit decision state (timeout
deadline, flush lifecycle, shutdown lifecycle, batch-size trigger) out of
`KafkaBatchCommitter` into a pure synchronous `CommitScheduler`, so the four
loop invariants are enforced in one place and unit-testable without asyncio.

**Spec:** [`design.md`](./design.md)

**Branch:** `refactor/commit-scheduler-deep-module`

**Commit strategy:** Per-task commits.

## Global Constraints

- Python ≥ 3.11. No `from __future__ import annotations`; eager annotations;
  `typing.Self`/`typing.Never` used directly.
- All imports at module level — never inside function bodies.
- Type suppression is `# ty: ignore[rule-name]`, never `# type: ignore`.
- `CommitScheduler` is **synchronous and I/O-free**: it reads no clock (the
  driver passes `now = loop.time()` in) and touches no asyncio object.
- `CommitScheduler` is **internal**: not added to `__init__.py`'s `__all__`.
- Public interface unchanged: `initialize_concurrent_processing`,
  `KafkaConcurrentProcessingMiddleware`, `ConsumerRebalanceListener`,
  `is_kafka_handler_healthy`, and the committer's surface
  (`spawn` / `close` / `send_task` / `commit_all` /
  `clear_cancellation_watermarks` / `is_healthy`).
- Strictly behaviour-preserving: same iteration semantics, same commit timing,
  same shutdown sequence. `test_integration.py`, `test_rebalance.py`,
  `test_middleware.py`, and the committer-level tests in
  `test_kafka_committer.py` pass **untouched**.
- 100% coverage enforced (pytest-cov). `just lint` (ruff + ty) must pass.
- Unit tests run without Docker: `uv run --no-sync pytest tests/<file> -v`.
  The full gate (integration vs Redpanda) runs via `just test`. Lint gate is
  check-only: `just lint-ci` (eof-fixer may flag scratch files outside
  `faststream_concurrent_aiokafka/` and `tests/` — only those two trees must be
  ruff + ty clean).

---

### Task 1: `CommitScheduler` + `Decision` in `_commit_scheduler.py` (TDD, new test file)

**Files:**
- Create: `faststream_concurrent_aiokafka/_commit_scheduler.py`
- Test: `tests/test_commit_scheduler.py` (create)

**Interfaces:**
- Consumes: nothing (pure, no project imports beyond stdlib `dataclasses`/`typing`).
- Produces (relied on by Task 2):
  - `class Decision` — frozen dataclass: `should_commit: bool`,
    `drain_queue_now: bool`, `timeout_fired: bool`.
  - `class CommitScheduler` with:
    - `__init__(self, *, commit_batch_size: int, commit_batch_timeout_sec: float)`
    - `accepts_new_work(self) -> bool`
    - `wait_timeout(self, now: float) -> float | None`
    - `evaluate(self, *, now: float, absorbed: bool, flush_fired: bool, stop_requested: bool, pending_len: int) -> Decision`
    - `note_committed(self, *, now: float, committed: bool, timeout_fired: bool, pending_empty: bool) -> None`
    - `is_finished(self, *, pending_empty: bool) -> bool`

- [ ] **Step 1: Write the failing tests**

  Create `tests/test_commit_scheduler.py`. These feed observation sequences with
  zero asyncio.

  ```python
  from faststream_concurrent_aiokafka._commit_scheduler import CommitScheduler, Decision


  def _sched(batch_size: int = 10, timeout: float = 10.0) -> CommitScheduler:
      return CommitScheduler(commit_batch_size=batch_size, commit_batch_timeout_sec=timeout)


  def test_initial_state_accepts_work_no_deadline_not_finished() -> None:
      s = _sched()
      assert s.accepts_new_work() is True
      assert s.wait_timeout(now=100.0) is None
      assert s.is_finished(pending_empty=True) is False


  def test_deadline_arms_on_first_absorb_and_does_not_rearm() -> None:
      s = _sched(timeout=10.0)
      s.evaluate(now=100.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=0)
      assert s.wait_timeout(now=100.0) is None          # nothing absorbed → no deadline
      s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
      assert s.wait_timeout(now=100.0) == 10.0          # armed at now + timeout
      s.evaluate(now=103.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=2)
      assert s.wait_timeout(now=103.0) == 7.0           # ticks down; not re-armed


  def test_wait_timeout_floors_at_zero_past_deadline() -> None:
      s = _sched(timeout=10.0)
      s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
      assert s.wait_timeout(now=115.0) == 0.0


  def test_timeout_fires_at_deadline_and_triggers_commit() -> None:
      s = _sched(batch_size=10, timeout=10.0)
      s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
      d = s.evaluate(now=105.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=1)
      assert d.timeout_fired is False
      assert d.should_commit is False
      d = s.evaluate(now=110.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=1)
      assert d.timeout_fired is True
      assert d.should_commit is True


  def test_batch_size_triggers_commit() -> None:
      s = _sched(batch_size=3, timeout=10.0)
      d = s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=3)
      assert d.should_commit is True
      assert d.timeout_fired is False
      assert d.drain_queue_now is False


  def test_flush_without_stop_commits_until_pending_drains() -> None:
      s = _sched(batch_size=10, timeout=10.0)
      d = s.evaluate(now=100.0, absorbed=False, flush_fired=True, stop_requested=False, pending_len=1)
      assert d.should_commit is True       # flush_in_progress drives commit
      assert d.drain_queue_now is False
      d2 = s.evaluate(now=101.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=1)
      assert d2.should_commit is True      # keeps committing while flush_in_progress
      s.note_committed(now=102.0, committed=True, timeout_fired=False, pending_empty=True)
      d3 = s.evaluate(now=103.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=0)
      assert d3.should_commit is False     # flag cleared once pending drained


  def test_flush_with_stop_sets_shutdown_and_drain() -> None:
      s = _sched()
      d = s.evaluate(now=100.0, absorbed=False, flush_fired=True, stop_requested=True, pending_len=2)
      assert d.drain_queue_now is True
      assert d.should_commit is True
      assert s.accepts_new_work() is False
      assert s.is_finished(pending_empty=False) is False
      assert s.is_finished(pending_empty=True) is True


  def test_deadline_reset_keeps_ticking_when_pending_remains() -> None:
      s = _sched(timeout=10.0)
      s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=5)
      s.note_committed(now=104.0, committed=True, timeout_fired=False, pending_empty=False)
      assert s.wait_timeout(now=104.0) == 10.0     # re-armed at fresh now + timeout


  def test_deadline_cleared_when_pending_drains() -> None:
      s = _sched(timeout=10.0)
      s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
      s.note_committed(now=104.0, committed=True, timeout_fired=False, pending_empty=True)
      assert s.wait_timeout(now=104.0) is None      # invariant: pending empty ⇒ no deadline


  def test_note_committed_resets_on_timeout_even_without_commit() -> None:
      s = _sched(timeout=10.0)
      s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
      s.note_committed(now=110.0, committed=False, timeout_fired=True, pending_empty=False)
      assert s.wait_timeout(now=110.0) == 10.0


  def test_note_committed_no_reset_when_neither_committed_nor_timeout() -> None:
      s = _sched(timeout=10.0)
      s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
      s.note_committed(now=103.0, committed=False, timeout_fired=False, pending_empty=False)
      assert s.wait_timeout(now=103.0) == 7.0      # deadline left ticking, not reset


  def test_no_trigger_when_idle_below_batch() -> None:
      s = _sched(batch_size=10, timeout=10.0)
      s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
      d = s.evaluate(now=102.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=2)
      assert d.should_commit is False
  ```

- [ ] **Step 2: Run the tests to verify they fail**

  Run: `uv run --no-sync pytest tests/test_commit_scheduler.py -v`
  Expected: FAIL — `ModuleNotFoundError: No module named '..._commit_scheduler'`.

- [ ] **Step 3: Implement `Decision` and `CommitScheduler`**

  Create `faststream_concurrent_aiokafka/_commit_scheduler.py`:

  ```python
  import dataclasses


  @dataclasses.dataclass(frozen=True, slots=True)
  class Decision:
      should_commit: bool     # the batch-size / timeout / flush / shutdown trigger fired
      drain_queue_now: bool   # flush fired with stop_requested → driver drains the queue into pending
      timeout_fired: bool     # surfaced so the driver hands it back to note_committed for the deadline reset


  class CommitScheduler:
      """Owns the streaming loop's when-to-commit decision state: the timeout
      deadline, the flush lifecycle, and the shutdown lifecycle.

      Synchronous, I/O-free, single-owner: the committer's async driver is the
      sole caller, on one asyncio task. Reads no clock and touches no asyncio
      object — the driver passes ``now = loop.time()`` in. The driver never
      writes a decision field; it only feeds observations (evaluate) and acts on
      the returned Decision, so the invariants below are enforced here, not
      annotated.

      Invariants:
        * pending empty ⇒ timeout_deadline is None.
        * flush_in_progress is set only when a flush fired without a stop request,
          and cleared once pending drains.
        * should_shutdown is set only when a flush fired with a stop request; once
          set, is_finished() returns True as soon as pending drains.
      """

      def __init__(self, *, commit_batch_size: int, commit_batch_timeout_sec: float) -> None:
          self._batch_size = commit_batch_size
          self._batch_timeout = commit_batch_timeout_sec
          self._timeout_deadline: float | None = None
          self._should_shutdown: bool = False
          self._flush_in_progress: bool = False

      def accepts_new_work(self) -> bool:
          # While shutting down, the driver stops pulling new items from the queue.
          return not self._should_shutdown

      def wait_timeout(self, now: float) -> float | None:
          # Remaining time until the batch-timeout fires, for asyncio.wait. None when
          # no deadline is armed (pending empty), so the select blocks until an event.
          if self._timeout_deadline is None:
              return None
          return max(self._timeout_deadline - now, 0.0)

      def evaluate(
          self,
          *,
          now: float,
          absorbed: bool,
          flush_fired: bool,
          stop_requested: bool,
          pending_len: int,
      ) -> Decision:
          # Arm the deadline on the first pending item (no-op once armed).
          if absorbed and self._timeout_deadline is None:
              self._timeout_deadline = now + self._batch_timeout

          timeout_fired = self._timeout_deadline is not None and now >= self._timeout_deadline

          drain_queue_now = False
          if flush_fired:
              if stop_requested:
                  self._should_shutdown = True
                  drain_queue_now = True
              else:
                  self._flush_in_progress = True

          should_commit = (
              pending_len >= self._batch_size
              or timeout_fired
              or self._flush_in_progress
              or self._should_shutdown
          )
          return Decision(
              should_commit=should_commit,
              drain_queue_now=drain_queue_now,
              timeout_fired=timeout_fired,
          )

      def note_committed(
          self,
          *,
          now: float,
          committed: bool,
          timeout_fired: bool,
          pending_empty: bool,
      ) -> None:
          # An active commit_all (flush without stop) keeps committing until pending
          # drains; clear the flag once it does so messages_queue.join() can return.
          if self._flush_in_progress and pending_empty:
              self._flush_in_progress = False
          # Re-arm the deadline after a commit round or a timeout firing; otherwise
          # let it keep ticking. Invariant: pending empty ⇒ deadline None.
          if committed or timeout_fired:
              self._timeout_deadline = (now + self._batch_timeout) if not pending_empty else None

      def is_finished(self, *, pending_empty: bool) -> bool:
          return self._should_shutdown and pending_empty
  ```

- [ ] **Step 4: Run the tests to verify they pass**

  Run: `uv run --no-sync pytest tests/test_commit_scheduler.py -v`
  Expected: PASS (all tests).

- [ ] **Step 5: Confirm 100% coverage of the new module**

  Run: `uv run --no-sync pytest tests/test_commit_scheduler.py --cov=faststream_concurrent_aiokafka/_commit_scheduler --cov-report=term-missing`
  Expected: `_commit_scheduler.py` at 100% (no missing lines/branches). If a
  branch is uncovered, add the missing case before committing.

- [ ] **Step 6: Lint + commit**

  ```bash
  just lint-ci   # faststream_concurrent_aiokafka/ + tests/ must be ruff + ty clean
  git add faststream_concurrent_aiokafka/_commit_scheduler.py tests/test_commit_scheduler.py
  git commit -m "refactor: add CommitScheduler owning the loop's when-to-commit state

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Wire `CommitScheduler` into the committer's driver

**Files:**
- Modify: `faststream_concurrent_aiokafka/batch_committer.py`

**Interfaces:**
- Consumes: `CommitScheduler`, `Decision` from Task 1.
- Produces: the committer's async driver now delegates every when-to-commit
  decision to `self._scheduler`; `_StreamingState` is replaced by `_LoopTasks`
  holding only the three wait-tasks.

This task is atomic — the loop's decision state lives in one place, so the swap
lands together. The integration/middleware/rebalance suites are the regression
net. NOT a TDD task; preserve behaviour, verify by keeping existing tests green.

- [ ] **Step 1: Import the scheduler and construct it; drop the moved config fields**

  In `batch_committer.py`, extend the import:

  ```python
  from faststream_concurrent_aiokafka import _commit_scheduler, _pending_state, consts
  ```

  In `KafkaBatchCommitter.__init__`, delete the two lines
  `self._commit_batch_timeout_sec = commit_batch_timeout_sec` and
  `self._commit_batch_size = commit_batch_size`, and add (next to `self._pending`):

  ```python
  self._scheduler: typing.Final = _commit_scheduler.CommitScheduler(
      commit_batch_size=commit_batch_size,
      commit_batch_timeout_sec=commit_batch_timeout_sec,
  )
  ```

  (The constructor still accepts `commit_batch_size` / `commit_batch_timeout_sec`
  — public API unchanged — it just forwards them to the scheduler.)

- [ ] **Step 2: Replace `_StreamingState` with `_LoopTasks`**

  Replace the `_StreamingState` dataclass (the decision fields and the invariant
  docstring move into `CommitScheduler`):

  ```python
  @dataclasses.dataclass(kw_only=True, slots=True)
  class _LoopTasks:
      """The three asyncio wait-tasks the streaming select multiplexes."""

      queue_get_task: asyncio.Task[KafkaCommitTask]
      flush_wait_task: asyncio.Task[bool]
      task_completed_wait_task: asyncio.Task[bool]

      def cancel_outstanding(self) -> None:
          for task in (self.queue_get_task, self.flush_wait_task, self.task_completed_wait_task):
              if not task.done():
                  task.cancel()
  ```

- [ ] **Step 3: Rewrite `_run_commit_process` to drive the scheduler**

  ```python
  async def _run_commit_process(self) -> None:
      tasks: typing.Final = _LoopTasks(
          queue_get_task=asyncio.create_task(self._messages_queue.get()),
          flush_wait_task=asyncio.create_task(self._flush_batch_event.wait()),
          task_completed_wait_task=asyncio.create_task(self._task_completed_event.wait()),
      )
      try:
          while not self._scheduler.is_finished(pending_empty=not self._pending):
              await self._streaming_iteration(tasks)
      finally:
          tasks.cancel_outstanding()
          self._uncommitted_drained.set()
  ```

- [ ] **Step 4: Rewrite `_streaming_iteration` as pure glue (no flag writes)**

  ```python
  async def _streaming_iteration(self, tasks: "_LoopTasks") -> None:
      loop: typing.Final = asyncio.get_running_loop()

      wait_targets: list[asyncio.Future[typing.Any]] = [
          tasks.flush_wait_task,
          tasks.task_completed_wait_task,
      ]
      if self._scheduler.accepts_new_work():
          wait_targets.append(tasks.queue_get_task)

      remaining: typing.Final = self._scheduler.wait_timeout(loop.time())
      await asyncio.wait(wait_targets, return_when=asyncio.FIRST_COMPLETED, timeout=remaining)

      # Capture once after the wait — used for both the deadline arm and timeout_fired.
      now: typing.Final = loop.time()

      absorbed: typing.Final = self._scheduler.accepts_new_work() and tasks.queue_get_task.done()
      if absorbed:
          new_ct = tasks.queue_get_task.result()
          self._track_user_task(new_ct)
          self._pending.absorb(new_ct)
          tasks.queue_get_task = asyncio.create_task(self._messages_queue.get())

      # Re-arm the completion event before deciding, so a task finishing during this
      # iteration is captured next time instead of being lost between clear and re-wait.
      if tasks.task_completed_wait_task.done():
          self._task_completed_event.clear()
          tasks.task_completed_wait_task = asyncio.create_task(self._task_completed_event.wait())

      flush_fired: typing.Final = tasks.flush_wait_task.done()

      decision: typing.Final = self._scheduler.evaluate(
          now=now,
          absorbed=absorbed,
          flush_fired=flush_fired,
          stop_requested=self._stop_requested,
          pending_len=len(self._pending),
      )

      if flush_fired:
          self._handle_flush_fired(tasks, drain_queue=decision.drain_queue_now)

      committed = False
      if decision.should_commit:
          ready = self._pending.take_ready()
          if ready:
              await self._commit_ready(ready)
              committed = True

      self._scheduler.note_committed(
          now=loop.time(),
          committed=committed,
          timeout_fired=decision.timeout_fired,
          pending_empty=not self._pending,
      )
  ```

  Ordering note (behaviour-preserving): `evaluate` sets the flush flags **then**
  computes the trigger in one call, so flags set this iteration drive this
  iteration's commit — exactly as the old `_handle_flush_fired`-before-`_maybe_commit`
  order. The shutdown drain runs (via `_handle_flush_fired`) before `take_ready`,
  so drained items commit in the same iteration as today.

- [ ] **Step 5: Reduce `_handle_flush_fired` to driver plumbing and delete `_maybe_commit`**

  `_handle_flush_fired` no longer writes any flag — it only drains (on shutdown)
  and re-arms the flush wait-task:

  ```python
  def _handle_flush_fired(self, tasks: "_LoopTasks", *, drain_queue: bool) -> None:
      if drain_queue:
          # Drain anything still buffered in messages_queue into pending so close()
          # can commit it. Without this, items put before close() but not yet
          # absorbed by queue_get would be silently dropped (offsets stay
          # uncommitted; redelivered on restart, but close() callers expect
          # everything enqueued to be processed).
          while True:
              try:
                  ct = self._messages_queue.get_nowait()
              except asyncio.QueueEmpty:
                  break
              self._track_user_task(ct)
              self._pending.absorb(ct)
          if not tasks.queue_get_task.done():
              tasks.queue_get_task.cancel()
      self._flush_batch_event.clear()
      tasks.flush_wait_task = asyncio.create_task(self._flush_batch_event.wait())
  ```

  Delete the `_maybe_commit` method entirely (its trigger logic moved into
  `CommitScheduler.evaluate`; its commit execution is inlined in
  `_streaming_iteration` Step 4).

- [ ] **Step 6: Run the committer unit suite**

  Run: `uv run --no-sync pytest tests/test_kafka_committer.py tests/test_commit_scheduler.py -v`
  Expected: PASS. The one loop reference in the unit tests
  (`patch.object(committer, "_run_commit_process", ...)`) still resolves —
  `_run_commit_process` remains on the committer. If anything references the
  removed `_StreamingState` / `_maybe_commit` / `_commit_batch_size`, fix it.

- [ ] **Step 7: Full gate (integration must be untouched and green)**

  Run: `just test`
  Expected: PASS, including all of `test_integration.py`, `test_rebalance.py`,
  `test_middleware.py` **with no edits to those files**. This is the proof the
  refactor is behaviour-preserving. (Requires Docker; if unavailable, STOP and
  report DONE_WITH_CONCERNS naming exactly that, with the unit results you have —
  do not claim the integration gate passed if you did not run it.)

- [ ] **Step 8: Lint + commit**

  ```bash
  just lint-ci
  git add faststream_concurrent_aiokafka/batch_committer.py
  git commit -m "refactor: drive commit timing through CommitScheduler

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Seal — coverage gate, full integration, architecture promotion

**Files:**
- Modify: `architecture/batch-committer.md`
- (Verification only on the rest.)

- [ ] **Step 1: Confirm the loop's decision state is no longer reachable from tests**

  Run: `grep -rnE "_StreamingState|_maybe_commit|flush_in_progress|should_shutdown|timeout_deadline" tests/`
  Expected: no matches. (A surviving `patch.object(committer, "_run_commit_process")`
  reference is fine — it mocks the driver, not the decision state.)

- [ ] **Step 2: 100% coverage gate**

  Run: `just test-branch`
  Expected: PASS at 100% coverage. If `CommitScheduler` or the rewritten driver
  has an uncovered branch, add the missing case (to `tests/test_commit_scheduler.py`
  for decider gaps) and re-run.

- [ ] **Step 3: Promote conclusions into `architecture/batch-committer.md`**

  Read the current doc, then add/adjust to record the shipped split (minimal,
  accurate edits — do not rewrite wholesale):
  - The committer's `_run_commit_process` is the **async driver**: it owns the
    `asyncio.wait` select over the three wait-tasks (queue-get / flush / task-done)
    and the queue, but delegates every when-to-commit decision to a pure
    synchronous `CommitScheduler` (`_commit_scheduler.py`).
  - `CommitScheduler` owns the timeout deadline, the flush lifecycle
    (`flush_in_progress`), and the shutdown lifecycle (`should_shutdown`); the
    driver feeds it observations (`evaluate`) and acts on the returned `Decision`,
    never writing a decision field itself.
  - The commit triggers are unchanged (pending ≥ `commit_batch_size`, the
    `commit_batch_timeout_sec` deadline, or a `commit_all`/`close` flush); they
    are now computed inside `CommitScheduler.evaluate`.

- [ ] **Step 4: Final gate**

  Run: `just lint && just test`
  Expected: clean + all green.

- [ ] **Step 5: Commit**

  ```bash
  git add architecture/batch-committer.md
  git commit -m "docs(architecture): record CommitScheduler / async-driver split

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

## Self-review notes (for the executor)

- **Spec coverage:** Task 1 = `CommitScheduler` interface + invariants (design
  §1–§2); Task 2 = driver rewrite delegating to the scheduler (§3–§4); Task 3 =
  seal + architecture promotion (Testing/Risk). The async machinery, produce
  side, backpressure, queue, and commit I/O are intentionally untouched (Non-goals).
- **Behaviour-preservation hinges (Task 2):** `now` is captured once after the
  await and used for both the deadline arm and `timeout_fired`; `note_committed`
  uses a **fresh** `loop.time()` and resets on `committed OR timeout_fired`;
  `flush_in_progress` clears on post-commit `pending_empty` before the reset;
  flush flags set in `evaluate` drive the same-iteration commit; the shutdown
  drain runs before `take_ready`.
- **No whitebox migration:** the loop decision logic had no unit tests, so no
  test repointing is needed — Task 1 adds net-new coverage.
