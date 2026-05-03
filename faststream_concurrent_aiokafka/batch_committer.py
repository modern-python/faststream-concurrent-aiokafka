import asyncio
import bisect
import contextlib
import dataclasses
import logging
import operator
import typing

from aiokafka.errors import CommitFailedError, IllegalStateError, KafkaError
from faststream.kafka import TopicPartition


if typing.TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer


logger = logging.getLogger(__name__)


DEFAULT_SHUTDOWN_TIMEOUT_SEC: typing.Final = 20.0
_OFFSET_KEY: typing.Final = operator.attrgetter("offset")


class CommitterIsDeadError(Exception): ...


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class KafkaCommitTask:
    asyncio_task: asyncio.Task[typing.Any]
    topic_partition: TopicPartition
    offset: int
    consumer: typing.Any


@dataclasses.dataclass(kw_only=True, slots=True)
class _StreamingState:
    queue_get_task: asyncio.Task[KafkaCommitTask]
    flush_wait_task: asyncio.Task[bool]
    task_completed_wait_task: asyncio.Task[bool]
    timeout_task: asyncio.Task[None] | None = None
    pending: dict[TopicPartition, list[KafkaCommitTask]] = dataclasses.field(default_factory=dict)
    # Cached count of all tasks in `pending` across partitions; kept in sync with
    # _insert_sorted callers and post-extract. Lets _maybe_commit avoid an O(P) sum every loop.
    pending_count: int = 0
    should_shutdown: bool = False
    # Active commit_all (flush event seen, _stop_requested is False): keep committing every
    # iteration until pending drains, so messages_queue.join() can return.
    flush_in_progress: bool = False

    def cancel_outstanding(self) -> None:
        for task in (self.queue_get_task, self.flush_wait_task, self.task_completed_wait_task):
            if not task.done():
                task.cancel()
        if self.timeout_task is not None and not self.timeout_task.done():
            self.timeout_task.cancel()


def _insert_sorted(partition_pending: list[KafkaCommitTask], new_ct: KafkaCommitTask) -> None:
    # Common case: tasks arrive from the broker in offset order, so append is correct and
    # the list stays sorted. Out-of-order arrivals only happen when _call_committer
    # re-queues a batch on transient KafkaError; bisect handles the rare case in O(log N).
    if not partition_pending or partition_pending[-1].offset <= new_ct.offset:
        partition_pending.append(new_ct)
    else:
        bisect.insort(partition_pending, new_ct, key=_OFFSET_KEY)


class KafkaBatchCommitter:
    def __init__(
        self,
        commit_batch_timeout_sec: float = 10.0,
        commit_batch_size: int = 10,
        shutdown_timeout_sec: float = DEFAULT_SHUTDOWN_TIMEOUT_SEC,
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

    def _on_user_task_done(self, _task: asyncio.Future[typing.Any]) -> None:
        """Done-callback target for user tasks; wakes the streaming loop."""
        self._task_completed_event.set()

    def _track_user_task(self, ct: KafkaCommitTask) -> None:
        # add_done_callback fires the callback synchronously if the future is already done,
        # so a task that completed between create_task and absorb still triggers the wakeup.
        ct.asyncio_task.add_done_callback(self._on_user_task_done)

    def _check_is_commit_task_running(self) -> None:
        if not self._commit_task or self._commit_task.done():
            msg: typing.Final = "Committer main task is not running"
            raise CommitterIsDeadError(msg)

    async def _call_committer(
        self, tasks_batch: list[KafkaCommitTask], partitions_to_offsets: dict[TopicPartition, int]
    ) -> bool:
        if not partitions_to_offsets:
            return True
        consumer: typing.Final[AIOKafkaConsumer] = tasks_batch[0].consumer
        try:
            await consumer.commit(partitions_to_offsets)
        except (CommitFailedError, IllegalStateError):
            # Partition no longer assigned (rebalance/revocation) — discard batch, not retryable
            logger.exception("Cannot commit due to partition loss or rebalancing, ignoring batch")
            return False
        except KafkaError:
            # Transient error — re-queue batch for retry on next cycle
            logger.exception("Error during commit to kafka, re-queuing batch")
            for task in tasks_batch:
                await self._messages_queue.put(task)
            return False
        else:
            return True

    @staticmethod
    def _map_offsets_per_partition(consumer_tasks: list[KafkaCommitTask]) -> dict[TopicPartition, int]:
        by_partition: dict[TopicPartition, list[KafkaCommitTask]] = {}
        for task in consumer_tasks:
            by_partition.setdefault(task.topic_partition, []).append(task)

        partitions_to_offsets: dict[TopicPartition, int] = {}
        for partition, tasks in by_partition.items():
            max_offset: int | None = None
            for task in sorted(tasks, key=_OFFSET_KEY):
                if task.asyncio_task.cancelled():
                    break  # stop committing at first cancelled task — message was not processed
                max_offset = task.offset
            if max_offset is not None:
                # Kafka commits the *next* offset to fetch, so committed = processed_max + 1
                partitions_to_offsets[partition] = max_offset + 1
        return partitions_to_offsets

    @staticmethod
    def _extract_ready_prefixes(
        pending: dict[TopicPartition, list[KafkaCommitTask]],
    ) -> dict[TopicPartition, list[KafkaCommitTask]]:
        # Pending lists are maintained in offset order by _insert_sorted. Per partition, find
        # the first not-done task; tasks before it form the contiguous-done prefix and become
        # "ready". A cancelled task is treated as a hard boundary: cancelled + everything after
        # is dropped from pending and added to ready (so task_done() balances
        # messages_queue.join), while _map_offsets_per_partition stops the offset advance at
        # the cancelled task so the uncommitted offsets get redelivered on restart
        # (at-least-once).
        ready: dict[TopicPartition, list[KafkaCommitTask]] = {}
        empty_partitions: list[TopicPartition] = []
        for partition, partition_pending in pending.items():
            prefix_end = 0
            for index, task in enumerate(partition_pending):
                if task.asyncio_task.cancelled():
                    prefix_end = len(partition_pending)
                    break
                if not task.asyncio_task.done():
                    prefix_end = index
                    break
                prefix_end = index + 1

            if prefix_end > 0:
                ready[partition] = partition_pending[:prefix_end]
                del partition_pending[:prefix_end]
            if not partition_pending:
                empty_partitions.append(partition)

        for k in empty_partitions:
            del pending[k]
        return ready

    async def _commit_partitions(self, ready: dict[TopicPartition, list[KafkaCommitTask]]) -> bool:
        # Task exception logging is handled by the handler's _finish_task done-callback so
        # it fires once per task at completion time. We intentionally do NOT log here:
        # transient KafkaError re-queues a task, and a per-commit log would emit duplicates.
        flat: typing.Final[list[KafkaCommitTask]] = [t for tasks in ready.values() for t in tasks]

        # Group by consumer instance — each AIOKafkaConsumer can only commit its own partitions.
        # With more than one consumer (router with multiple subscribers sharing the handler),
        # each commit is an independent network round-trip and can run concurrently.
        consumers_tasks: dict[int, list[KafkaCommitTask]] = {}
        for task in flat:
            consumers_tasks.setdefault(id(task.consumer), []).append(task)

        results: typing.Final = await asyncio.gather(
            *(self._call_committer(ct, self._map_offsets_per_partition(ct)) for ct in consumers_tasks.values())
        )

        for _ in flat:
            self._messages_queue.task_done()
        return all(results)

    def _reset_timeout(
        self,
        timeout_task: asyncio.Task[None] | None,
        pending_non_empty: bool,
    ) -> asyncio.Task[None] | None:
        if timeout_task is not None and not timeout_task.done():
            timeout_task.cancel()
        if pending_non_empty:
            return asyncio.create_task(asyncio.sleep(self._commit_batch_timeout_sec))
        return None

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
            while not (state.should_shutdown and not state.pending):
                await self._streaming_iteration(state)
        finally:
            state.cancel_outstanding()

    async def _streaming_iteration(self, state: "_StreamingState") -> None:
        wait_targets: list[asyncio.Future[typing.Any]] = [
            state.flush_wait_task,
            state.task_completed_wait_task,
        ]
        if not state.should_shutdown:
            wait_targets.append(state.queue_get_task)
        if state.timeout_task is not None:
            wait_targets.append(state.timeout_task)

        await asyncio.wait(wait_targets, return_when=asyncio.FIRST_COMPLETED)

        if not state.should_shutdown and state.queue_get_task.done():
            new_ct = state.queue_get_task.result()
            self._track_user_task(new_ct)
            _insert_sorted(state.pending.setdefault(new_ct.topic_partition, []), new_ct)
            state.pending_count += 1
            state.queue_get_task = asyncio.create_task(self._messages_queue.get())
            if state.timeout_task is None:
                state.timeout_task = asyncio.create_task(asyncio.sleep(self._commit_batch_timeout_sec))

        # Re-arm completion event before extract, so any task finishing during extract is
        # captured by the next iteration instead of being lost between clear and re-wait.
        if state.task_completed_wait_task.done():
            self._task_completed_event.clear()
            state.task_completed_wait_task = asyncio.create_task(self._task_completed_event.wait())

        timeout_fired: typing.Final = state.timeout_task is not None and state.timeout_task.done()
        flush_fired: typing.Final = state.flush_wait_task.done()

        if flush_fired:
            self._handle_flush_fired(state)

        ready: typing.Final = await self._maybe_commit(state, timeout_fired)
        if state.flush_in_progress and not state.pending:
            state.flush_in_progress = False

        # Reset the timer after any commit OR on timeout firing. Let it tick otherwise.
        # Invariant: pending empty ⇒ timeout_task is None (guaranteed by _reset_timeout
        # always being called when pending is mutated to empty), so no separate cleanup is needed.
        if ready or timeout_fired:
            state.timeout_task = self._reset_timeout(state.timeout_task, bool(state.pending))

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
                _insert_sorted(state.pending.setdefault(ct.topic_partition, []), ct)
                state.pending_count += 1
            if not state.queue_get_task.done():
                state.queue_get_task.cancel()
        else:
            state.flush_in_progress = True
        self._flush_batch_event.clear()
        state.flush_wait_task = asyncio.create_task(self._flush_batch_event.wait())

    async def _maybe_commit(
        self, state: "_StreamingState", timeout_fired: bool
    ) -> dict[TopicPartition, list[KafkaCommitTask]]:
        commit_triggered: typing.Final = (
            state.pending_count >= self._commit_batch_size
            or timeout_fired
            or state.flush_in_progress
            or state.should_shutdown
        )
        if not commit_triggered:
            return {}
        ready: typing.Final = self._extract_ready_prefixes(state.pending)
        if ready:
            state.pending_count -= sum(len(v) for v in ready.values())
            await self._commit_partitions(ready)
        return ready

    async def commit_all(self) -> None:
        """Flush and commit all pending tasks without stopping the committer loop.

        Safe to call during Kafka rebalance (on_partitions_revoked). The committer
        continues running after this returns.
        """
        self._flush_batch_event.set()
        await self._messages_queue.join()

    async def send_task(self, new_task: KafkaCommitTask) -> None:
        self._check_is_commit_task_running()
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
        except Exception as exc:
            logger.exception("Committer task failed during shutdown", exc_info=exc)
            raise

    @property
    def is_healthy(self) -> bool:
        return self._commit_task is not None and not self._commit_task.done()
