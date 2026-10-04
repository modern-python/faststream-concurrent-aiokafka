import dataclasses

from faststream_concurrent_aiokafka import consts


@dataclasses.dataclass(frozen=True, slots=True)
class Decision:
    should_commit: bool  # the batch-size / timeout / flush / shutdown trigger fired
    drain_queue_now: bool  # flush fired with stop_requested → driver drains the queue into pending
    timeout_fired: bool  # surfaced so the driver hands it back to note_committed for the deadline reset


class CommitScheduler:
    """Owns the streaming loop's when-to-commit decision state.

    Manages the timeout deadline, the flush lifecycle, the shutdown lifecycle,
    and the backoff between commit rounds that hit a transient error.

    Synchronous, I/O-free, single-owner: the committer's async driver is the
    sole caller, on one asyncio task. Reads no clock and touches no asyncio
    object — the driver passes ``now = loop.time()`` in. The driver never
    writes a decision field; it only feeds observations (evaluate) and acts on
    the returned Decision, so the invariants below are enforced here, not
    annotated.

    Invariants:
      * pending empty ⇒ timeout_deadline is None.
      * flush_in_progress is set only when a flush fired without a stop request,
        and cleared once pending drains or the flush is released.
      * retry_not_before is set only by a round that hit a transient error, and
        cleared by the next round that did not; until it passes, no trigger commits.
      * should_shutdown is set only when a flush fired with a stop request; once
        set, is_finished() returns True as soon as pending drains.
    """

    def __init__(self, *, commit_batch_size: int, commit_batch_timeout_sec: float) -> None:
        self._batch_size = commit_batch_size
        self._batch_timeout = commit_batch_timeout_sec
        self._timeout_deadline: float | None = None
        self._should_shutdown: bool = False
        self._flush_in_progress: bool = False
        self._retry_attempts: int = 0
        self._retry_not_before: float | None = None

    @property
    def retrying(self) -> bool:
        return self._retry_attempts > 0

    def accepts_new_work(self) -> bool:
        # While shutting down, the driver stops pulling new items from the queue.
        return not self._should_shutdown

    def wait_timeout(self, now: float) -> float | None:
        # Time until the batch-timeout fires or, during a flush, a backed-off retry is due.
        # None when neither is armed, so the select blocks until an event.
        wake_at = [] if self._timeout_deadline is None else [self._timeout_deadline]
        urgent = self._flush_in_progress or self._should_shutdown
        if urgent and self._retry_not_before is not None and self._retry_not_before > now:
            wake_at.append(self._retry_not_before)
        if not wake_at:
            return None
        return max(min(wake_at) - now, 0.0)

    def release_flush(self) -> None:
        # Fed before evaluate(), so a flush that fired since the release still opens.
        self._flush_in_progress = False

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

        backing_off = self._retry_not_before is not None and now < self._retry_not_before
        should_commit = not backing_off and (
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
        transient_error: bool,
    ) -> None:
        # An active commit_all (flush without stop) keeps committing until pending
        # drains; clear the flag once it does so messages_queue.join() can return.
        if self._flush_in_progress and pending_empty:
            self._flush_in_progress = False
        # Re-arm the deadline after a commit round or a timeout firing; otherwise
        # let it keep ticking. Invariant: pending empty ⇒ deadline None.
        if committed or timeout_fired:
            self._timeout_deadline = (now + self._batch_timeout) if not pending_empty else None
        if transient_error:
            self._retry_attempts += 1
            delay = consts.COMMIT_RETRY_BACKOFF_BASE_SEC * 2 ** (self._retry_attempts - 1)
            self._retry_not_before = now + min(delay, consts.COMMIT_RETRY_BACKOFF_MAX_SEC)
        elif committed:
            self._retry_attempts = 0
            self._retry_not_before = None

    def is_finished(self, *, pending_empty: bool) -> bool:
        return self._should_shutdown and pending_empty
