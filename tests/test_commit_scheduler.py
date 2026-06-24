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
    s.note_committed(now=102.0, committed=True, timeout_fired=False, pending_empty=True)
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
    s.note_committed(now=104.0, committed=True, timeout_fired=False, pending_empty=False)
    assert s.wait_timeout(now=104.0) == 10.0  # re-armed at fresh now + timeout


def test_deadline_cleared_when_pending_drains() -> None:
    s = _sched(timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    s.note_committed(now=104.0, committed=True, timeout_fired=False, pending_empty=True)
    assert s.wait_timeout(now=104.0) is None  # invariant: pending empty ⇒ no deadline


def test_note_committed_resets_on_timeout_even_without_commit() -> None:
    s = _sched(timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    s.note_committed(now=110.0, committed=False, timeout_fired=True, pending_empty=False)
    assert s.wait_timeout(now=110.0) == 10.0


def test_note_committed_no_reset_when_neither_committed_nor_timeout() -> None:
    s = _sched(timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    s.note_committed(now=103.0, committed=False, timeout_fired=False, pending_empty=False)
    assert s.wait_timeout(now=103.0) == 7.0  # deadline left ticking, not reset


def test_no_trigger_when_idle_below_batch() -> None:
    s = _sched(batch_size=10, timeout=10.0)
    s.evaluate(now=100.0, absorbed=True, flush_fired=False, stop_requested=False, pending_len=1)
    d = s.evaluate(now=102.0, absorbed=False, flush_fired=False, stop_requested=False, pending_len=2)
    assert d.should_commit is False
