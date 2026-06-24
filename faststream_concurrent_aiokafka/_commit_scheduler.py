import dataclasses


@dataclasses.dataclass(frozen=True, slots=True)
class Decision:
    should_commit: bool  # the batch-size / timeout / flush / shutdown trigger fired
    drain_queue_now: bool  # flush fired with stop_requested → driver drains the queue into pending
    timeout_fired: bool  # surfaced so the driver hands it back to note_committed for the deadline reset


class CommitScheduler:
    """Owns the streaming loop's when-to-commit decision state.

    Manages the timeout deadline, the flush lifecycle, and the shutdown lifecycle.

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
            pending_len >= self._batch_size or timeout_fired or self._flush_in_progress or self._should_shutdown
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
