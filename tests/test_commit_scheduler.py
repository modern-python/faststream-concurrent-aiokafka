import ast
import inspect
import pathlib

import pytest

from faststream_concurrent_aiokafka._commit_scheduler import CommitScheduler


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
    assert s.wait_timeout(now=100.0) is None  # nothing absorbed → no deadline
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    assert s.wait_timeout(now=100.0) == 10.0  # armed at now + timeout
    s.evaluate(now=103.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=2)
    assert s.wait_timeout(now=103.0) == 7.0  # ticks down; not re-armed


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
    assert d.should_commit is True  # flush_in_progress drives commit
    assert d.drain_queue_now is False
    d2 = s.evaluate(now=101.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=1)
    assert d2.should_commit is True  # keeps committing while flush_in_progress
    s.note_committed(now=102.0, committed=True, timeout_fired=False, pending_empty=True, transient_error=False)
    d3 = s.evaluate(now=103.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=0)
    assert d3.should_commit is False  # flag cleared once pending drained


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
    s.note_committed(now=104.0, committed=True, timeout_fired=False, pending_empty=False, transient_error=False)
    assert s.wait_timeout(now=104.0) == 10.0  # re-armed at fresh now + timeout


def test_deadline_cleared_when_pending_drains() -> None:
    s = _sched(timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    s.note_committed(now=104.0, committed=True, timeout_fired=False, pending_empty=True, transient_error=False)
    assert s.wait_timeout(now=104.0) is None  # invariant: pending empty ⇒ no deadline


def test_note_committed_resets_on_timeout_even_without_commit() -> None:
    s = _sched(timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    s.note_committed(now=110.0, committed=False, timeout_fired=True, pending_empty=False, transient_error=False)
    assert s.wait_timeout(now=110.0) == 10.0


def test_note_committed_no_reset_when_neither_committed_nor_timeout() -> None:
    s = _sched(timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    s.note_committed(now=103.0, committed=False, timeout_fired=False, pending_empty=False, transient_error=False)
    assert s.wait_timeout(now=103.0) == 7.0  # deadline left ticking, not reset


def test_no_trigger_when_idle_below_batch() -> None:
    s = _sched(batch_size=10, timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    d = s.evaluate(now=102.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=2)
    assert d.should_commit is False


def _flush(s: CommitScheduler, *, now: float, released: bool = False, fired: bool = False) -> bool:
    if released:
        s.release_flush()
    return s.evaluate(now=now, absorbed=False, flush_fired=fired, stop_requested=False, pending_len=1).should_commit


def _fail(s: CommitScheduler, *, now: float) -> None:
    s.note_committed(now=now, committed=True, timeout_fired=False, pending_empty=False, transient_error=True)


def test_transient_error_backs_off_the_next_flush_commit() -> None:
    s = _sched()
    assert _flush(s, now=100.0, fired=True) is True
    _fail(s, now=100.0)
    assert _flush(s, now=100.05) is False
    assert s.wait_timeout(now=100.05) == pytest.approx(0.05)
    assert _flush(s, now=100.1) is True


def test_backoff_doubles_up_to_the_cap() -> None:
    s = _sched()
    _flush(s, now=0.0, fired=True)
    delays = []
    now = 0.0
    for _ in range(7):
        _fail(s, now=now)
        delay = s.wait_timeout(now=now)
        assert delay is not None
        delays.append(delay)
        now += delay
    assert delays == pytest.approx([0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0])


def test_a_round_without_transient_error_resets_the_backoff() -> None:
    s = _sched()
    _flush(s, now=100.0, fired=True)
    _fail(s, now=100.0)
    _fail(s, now=100.1)
    s.note_committed(now=100.3, committed=True, timeout_fired=False, pending_empty=False, transient_error=False)
    assert _flush(s, now=100.3) is True
    _fail(s, now=100.3)
    assert s.wait_timeout(now=100.3) == pytest.approx(0.1)


def test_backoff_gates_the_shutdown_flush() -> None:
    s = _sched()
    d = s.evaluate(now=100.0, absorbed=False, flush_fired=True, stop_requested=True, pending_len=1)
    assert d.should_commit is True
    _fail(s, now=100.0)
    d = s.evaluate(now=100.05, absorbed=False, flush_fired=False, stop_requested=True, pending_len=1)
    assert d.should_commit is False
    assert s.wait_timeout(now=100.05) == pytest.approx(0.05)


def test_retry_time_does_not_shorten_the_wait_outside_a_flush() -> None:
    s = _sched(timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    _fail(s, now=110.0)
    assert s.wait_timeout(now=110.0) == 10.0


def test_released_flush_stops_committing() -> None:
    s = _sched()
    _flush(s, now=100.0, fired=True)
    assert _flush(s, now=101.0, released=True) is False
    assert _flush(s, now=102.0) is False


def test_flush_fired_with_release_reopens_the_flush() -> None:
    s = _sched()
    _flush(s, now=100.0, fired=True)
    assert _flush(s, now=101.0, released=True, fired=True) is True


def test_the_commit_scheduler_reads_no_clock_and_touches_no_asyncio() -> None:
    """INVARIANT: `_commit_scheduler` decides synchronously, from arguments alone.

    The driver in `batch_committer.py` passes `now = loop.time()` in, and that is the module's
    only source of time. What breaks this is reaching for convenience: a `time.monotonic()` to
    avoid threading `now` through one more call, an `asyncio.Event` to signal the driver back, an
    `async def` on a method that grew an await. Any of those pulls the whole decision surface onto
    the event loop and costs the reason the split exists — every scheduler test here runs by
    feeding observation sequences with no loop, no fake clock and no wait-task doubles. See
    docs/adr/0003-commit-scheduler-decides-the-driver-awaits.md.
    """
    tree = ast.parse(pathlib.Path(inspect.getfile(CommitScheduler)).read_text(encoding="utf-8"))

    imported = {
        alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    } | {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) if node.module}
    assert not imported & {"asyncio", "time", "datetime"}, f"clock or asyncio import: {sorted(imported)}"

    awaited = [
        type(node).__name__
        for node in ast.walk(tree)
        if isinstance(node, (ast.Await, ast.AsyncFunctionDef, ast.AsyncFor, ast.AsyncWith))
    ]
    assert not awaited, f"asynchronous construct in the decider: {awaited}"
