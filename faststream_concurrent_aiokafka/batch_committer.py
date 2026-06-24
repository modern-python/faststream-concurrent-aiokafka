import asyncio
import contextlib
import dataclasses
import logging
import typing

from aiokafka.errors import CommitFailedError, IllegalStateError, KafkaError
from faststream.kafka import TopicPartition

from faststream_concurrent_aiokafka import _commit_scheduler, _pending_state, consts
from faststream_concurrent_aiokafka._pending_state import KafkaCommitTask


logger = logging.getLogger(__name__)


class CommitterIsDeadError(Exception): ...


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

        self._shutdown_timeout = shutdown_timeout_sec
        # Owns per-partition pending commit tasks, count, and cancellation watermarks.
        self._pending: typing.Final = _pending_state.PendingCommits()
        self._scheduler: typing.Final = _commit_scheduler.CommitScheduler(
            commit_batch_size=commit_batch_size,
            commit_batch_timeout_sec=commit_batch_timeout_sec,
        )

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
        committed_count = 0
        for rc in ready_commits:
            committed_count += len(rc.tasks)
            for _ in rc.tasks:
                self._messages_queue.task_done()
        self._uncommitted_count -= committed_count
        self._uncommitted_drained.set()
        return all(results)

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
            # Drop the outstanding queue_get; we will not read more items while shutting
            # down. cancel() is a harmless no-op if it already completed.
            tasks.queue_get_task.cancel()
        self._flush_batch_event.clear()
        tasks.flush_wait_task = asyncio.create_task(self._flush_batch_event.wait())

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
