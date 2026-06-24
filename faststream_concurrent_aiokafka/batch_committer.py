import asyncio
import contextlib
import dataclasses
import logging
import typing

from aiokafka.errors import CommitFailedError, IllegalStateError, KafkaError
from faststream.kafka import TopicPartition

from faststream_concurrent_aiokafka import _pending_state, consts
from faststream_concurrent_aiokafka._pending_state import KafkaCommitTask


logger = logging.getLogger(__name__)


class CommitterIsDeadError(Exception): ...


@dataclasses.dataclass(kw_only=True, slots=True)
class _StreamingState:
    """Mutable state for the streaming committer loop.

    Invariants maintained by `_streaming_iteration`:
      * `pending` empty ⇒ `timeout_deadline is None`.
      * `flush_in_progress` is set only when a flush event fired *without*
        `_stop_requested`; cleared once `pending` drains.
      * `should_shutdown` is set only when a flush event fired *with*
        `_stop_requested`; once set, the loop exits as soon as `pending`
        drains.
    """

    queue_get_task: asyncio.Task[KafkaCommitTask]
    flush_wait_task: asyncio.Task[bool]
    task_completed_wait_task: asyncio.Task[bool]
    # Absolute loop-time deadline for the next commit_batch_timeout firing. None when pending
    # is empty (no timer needed). Passed as `timeout=` to asyncio.wait — no Task allocation.
    timeout_deadline: float | None = None
    should_shutdown: bool = False
    # Active commit_all (flush event seen, _stop_requested is False): keep committing every
    # iteration until pending drains, so messages_queue.join() can return.
    flush_in_progress: bool = False

    def cancel_outstanding(self) -> None:
        for task in (self.queue_get_task, self.flush_wait_task, self.task_completed_wait_task):
            if not task.done():
                task.cancel()


class KafkaBatchCommitter:
    def __init__(
        self,
        commit_batch_timeout_sec: float = consts.DEFAULT_COMMIT_BATCH_TIMEOUT_SEC,
        commit_batch_size: int = consts.DEFAULT_COMMIT_BATCH_SIZE,
        shutdown_timeout_sec: float = consts.DEFAULT_SHUTDOWN_TIMEOUT_SEC,
        max_uncommitted_tasks: int | None = consts.DEFAULT_MAX_UNCOMMITTED_TASKS,
    ) -> None:
        self._messages_queue: asyncio.Queue[KafkaCommitTask] = asyncio.Queue()
        self._commit_task: asyncio.Task[typing.Any] | None = None
        self._flush_batch_event = asyncio.Event()
        # Set from each user task's done-callback (registered in handle_task). Wakes the
        # streaming loop without us having to add per-task callbacks via asyncio.wait every
        # iteration. Fan-in cost is O(1) regardless of partition count or pending depth.
        self._task_completed_event = asyncio.Event()
        self._stop_requested: bool = False

        self._commit_batch_timeout_sec = commit_batch_timeout_sec
        self._commit_batch_size = commit_batch_size
        self._shutdown_timeout = shutdown_timeout_sec
        # Owns per-partition pending commit tasks, count, and cancellation watermarks.
        self._pending: typing.Final = _pending_state.PendingCommits()

        # Backpressure: count of tasks admitted via send_task but not yet finally
        # committed/dropped (in _messages_queue + pending). When it reaches the
        # ceiling, send_task blocks so the consume path stalls and the consumer stops
        # fetching until commits catch up. None disables the bound.
        self._max_uncommitted_tasks = max_uncommitted_tasks
        self._uncommitted_count: int = 0
        # Set whenever the count drops (a commit round) or the loop exits; wakes a
        # send_task blocked at the ceiling so it re-checks.
        self._uncommitted_drained = asyncio.Event()
        self._uncommitted_drained.set()

    def _on_user_task_done(self, _task: asyncio.Future[typing.Any]) -> None:
        """Done-callback target for user tasks; wakes the streaming loop."""
        self._task_completed_event.set()

    def _track_user_task(self, ct: KafkaCommitTask) -> None:
        # If the task is already done by the time we absorb it, add_done_callback still
        # schedules _on_user_task_done via loop.call_soon — it fires on the next tick and
        # wakes the streaming loop, so a task that completed between create_task and
        # absorb still triggers the wakeup.
        ct.asyncio_task.add_done_callback(self._on_user_task_done)

    def _check_is_commit_task_running(self) -> None:
        if not self._commit_task or self._commit_task.done():
            msg: typing.Final = "Committer main task is not running"
            raise CommitterIsDeadError(msg)

    async def _call_committer(self, rc: _pending_state.ReadyCommit) -> bool:
        if not rc.offsets:
            return True
        try:
            await rc.consumer.commit(rc.offsets)
        except (CommitFailedError, IllegalStateError):
            # Partition no longer assigned (rebalance/revocation) — discard batch, not retryable
            logger.exception("Cannot commit due to partition loss or rebalancing, ignoring batch")
            return False
        except KafkaError:
            # Transient error — re-queue batch for retry on next cycle
            logger.exception("Error during commit to kafka, re-queuing batch")
            for task in rc.tasks:
                self._uncommitted_count += 1
                await self._messages_queue.put(task)
            return False
        else:
            return True

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

    async def _run_commit_process(self) -> None:
        # Streaming committer: one loop continuously absorbs queue items into per-partition
        # pending state and commits each partition's contiguous-done prefix when total pending
        # crosses commit_batch_size, when the timeout fires, or when commit_all/close sets the
        # flush event. Queue depth no longer correlates with stuck-batch wait time.
        state: typing.Final = _StreamingState(
            queue_get_task=asyncio.create_task(self._messages_queue.get()),
            flush_wait_task=asyncio.create_task(self._flush_batch_event.wait()),
            task_completed_wait_task=asyncio.create_task(self._task_completed_event.wait()),
        )

        try:
            while not (state.should_shutdown and not self._pending):
                await self._streaming_iteration(state)
        finally:
            state.cancel_outstanding()
            self._uncommitted_drained.set()

    async def _streaming_iteration(self, state: "_StreamingState") -> None:
        wait_targets: list[asyncio.Future[typing.Any]] = [
            state.flush_wait_task,
            state.task_completed_wait_task,
        ]
        if not state.should_shutdown:
            wait_targets.append(state.queue_get_task)

        loop: typing.Final = asyncio.get_running_loop()
        remaining: float | None = None
        if state.timeout_deadline is not None:
            remaining = max(state.timeout_deadline - loop.time(), 0.0)

        await asyncio.wait(wait_targets, return_when=asyncio.FIRST_COMPLETED, timeout=remaining)

        # Capture once after the wait — clock may have advanced past the deadline even if no
        # future fired (the asyncio.wait timeout is what made us return).
        now: typing.Final = loop.time()

        if not state.should_shutdown and state.queue_get_task.done():
            new_ct = state.queue_get_task.result()
            self._track_user_task(new_ct)
            self._pending.absorb(new_ct)
            state.queue_get_task = asyncio.create_task(self._messages_queue.get())
            if state.timeout_deadline is None:
                state.timeout_deadline = now + self._commit_batch_timeout_sec

        # Re-arm completion event before extract, so any task finishing during extract is
        # captured by the next iteration instead of being lost between clear and re-wait.
        if state.task_completed_wait_task.done():
            self._task_completed_event.clear()
            state.task_completed_wait_task = asyncio.create_task(self._task_completed_event.wait())

        timeout_fired: typing.Final = state.timeout_deadline is not None and now >= state.timeout_deadline
        flush_fired: typing.Final = state.flush_wait_task.done()

        if flush_fired:
            self._handle_flush_fired(state)

        ready: typing.Final = await self._maybe_commit(state, timeout_fired)
        if state.flush_in_progress and not self._pending:
            state.flush_in_progress = False

        # Reset the deadline after any commit OR on timeout firing. Let it tick otherwise.
        # Invariant: pending empty ⇒ timeout_deadline is None.
        if ready or timeout_fired:
            state.timeout_deadline = (loop.time() + self._commit_batch_timeout_sec) if self._pending else None

    def _handle_flush_fired(self, state: "_StreamingState") -> None:
        if self._stop_requested:
            state.should_shutdown = True
            # Drain anything still buffered in messages_queue into pending so close()
            # can commit it. Without this, items put before close() but not yet absorbed
            # by queue_get would be silently dropped (offsets stay uncommitted; redelivered
            # on restart, but commit_all/close() callers expect everything enqueued to be
            # processed).
            while True:
                try:
                    ct = self._messages_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._track_user_task(ct)
                self._pending.absorb(ct)
            if not state.queue_get_task.done():
                state.queue_get_task.cancel()
        else:
            state.flush_in_progress = True
        self._flush_batch_event.clear()
        state.flush_wait_task = asyncio.create_task(self._flush_batch_event.wait())

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

    async def commit_all(self, flush_timeout_sec: float = consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC) -> None:
        """Flush and commit pending tasks without stopping the committer loop.

        Bounded by ``flush_timeout_sec``: on timeout, already-completed offsets are
        committed and any still-in-flight tasks stay uncommitted (redelivered after
        reassignment — at-least-once). Safe to call during rebalance
        (on_partitions_revoked); the committer keeps running after this returns.
        """
        self._flush_batch_event.set()
        try:
            await asyncio.wait_for(self._messages_queue.join(), timeout=flush_timeout_sec)
        except TimeoutError:
            logger.warning(
                "Kafka middleware. commit_all flush timed out after %.1fs; "
                "in-flight offsets will be redelivered on restart/reassignment",
                flush_timeout_sec,
            )

    def clear_cancellation_watermarks(self, partitions: typing.Iterable[TopicPartition] | None = None) -> None:
        """Forget cancellation watermarks for ``partitions`` (or all if ``None``).

        Called on partition revocation by the rebalance listener — the partition's
        next assignment starts fresh, with no inherited "do not advance" floor.
        """
        self._pending.clear_watermarks(partitions)

    async def send_task(self, new_task: KafkaCommitTask) -> None:
        self._check_is_commit_task_running()
        while self._max_uncommitted_tasks is not None and self._uncommitted_count >= self._max_uncommitted_tasks:
            self._uncommitted_drained.clear()
            # Re-check liveness before parking: if the loop died we must raise, not hang.
            self._check_is_commit_task_running()
            # Signal the committer loop to flush now so it can drain the count and unblock us.
            self._flush_batch_event.set()
            await self._uncommitted_drained.wait()
        self._uncommitted_count += 1
        await self._messages_queue.put(new_task)

    def spawn(self) -> None:
        if not self._commit_task:
            self._commit_task = asyncio.create_task(self._run_commit_process())
        else:
            logger.error("Committer main task already running")

    async def close(self) -> None:
        """Flush all pending tasks and shut down the committer."""
        if not self._commit_task:
            logger.error("Committer main task is not running, cannot close committer properly")
            return

        if self._commit_task.done():
            # Task already terminated (cancelled or raised). Nothing to wait on; surface
            # any non-cancellation exception so it gets logged, then continue shutdown.
            if not self._commit_task.cancelled():
                exc = self._commit_task.exception()
                if exc is not None:
                    logger.warning("Committer task had already died before close()", exc_info=exc)
            return

        self._stop_requested = True
        self._flush_batch_event.set()
        try:
            await asyncio.wait_for(self._commit_task, timeout=self._shutdown_timeout)
        except TimeoutError:
            logger.exception("Committer main task shutdown timed out, forcing cancellation")
            self._commit_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._commit_task
        except Exception:
            logger.exception("Committer task failed during shutdown")
            raise

    @property
    def is_healthy(self) -> bool:
        return self._commit_task is not None and not self._commit_task.done()
