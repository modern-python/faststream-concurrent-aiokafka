import asyncio
import contextlib
import dataclasses
import logging
import typing

from aiokafka.errors import CommitFailedError, IllegalStateError, KafkaError
from faststream.kafka import TopicPartition


if typing.TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer


logger = logging.getLogger(__name__)


DEFAULT_SHUTDOWN_TIMEOUT_SEC: typing.Final = 20.0


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
    timeout_task: asyncio.Task[None] | None = None
    pending: dict[TopicPartition, list[KafkaCommitTask]] = dataclasses.field(default_factory=dict)
    should_shutdown: bool = False
    # Active commit_all (flush event seen, _stop_requested is False): keep committing every
    # iteration until pending drains, so messages_queue.join() can return.
    flush_in_progress: bool = False

    def cancel_outstanding(self) -> None:
        for task in (self.queue_get_task, self.flush_wait_task):
            if not task.done():
                task.cancel()
        if self.timeout_task is not None and not self.timeout_task.done():
            self.timeout_task.cancel()


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
        self._stop_requested: bool = False

        self._commit_batch_timeout_sec = commit_batch_timeout_sec
        self._commit_batch_size = commit_batch_size
        self._shutdown_timeout = shutdown_timeout_sec

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
            for task in sorted(tasks, key=lambda x: x.offset):
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
        # Per partition (sorted by offset), find the first task that is not-done. Tasks before
        # it form the contiguous-done prefix and become "ready". A cancelled task is treated
        # as a hard boundary: cancelled + everything after is dropped from pending and added
        # to ready (so task_done() balances messages_queue.join), while
        # _map_offsets_per_partition stops the offset advance at the cancelled task so the
        # uncommitted offsets get redelivered on restart (at-least-once).
        ready: dict[TopicPartition, list[KafkaCommitTask]] = {}
        empty_partitions: list[TopicPartition] = []
        for partition, partition_pending in pending.items():
            # Re-queued-on-transient-error tasks land at the queue tail and may arrive
            # out of offset order with respect to newer same-partition tasks. Sort here.
            partition_pending.sort(key=lambda t: t.offset)

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
        flat: typing.Final[list[KafkaCommitTask]] = [t for tasks in ready.values() for t in tasks]
        for task in flat:
            if task.asyncio_task.cancelled():
                continue
            exc = task.asyncio_task.exception()
            if exc is not None:
                logger.error("Task has finished with an exception", exc_info=exc)

        # Group by consumer instance — each AIOKafkaConsumer can only commit its own partitions
        consumers_tasks: dict[int, list[KafkaCommitTask]] = {}
        for task in flat:
            consumers_tasks.setdefault(id(task.consumer), []).append(task)

        all_succeeded = True
        for consumer_tasks in consumers_tasks.values():
            partitions_to_offsets = self._map_offsets_per_partition(consumer_tasks)
            if not await self._call_committer(consumer_tasks, partitions_to_offsets):
                all_succeeded = False

        for _ in flat:
            self._messages_queue.task_done()
        return all_succeeded

    @staticmethod
    def _pending_head_tasks(
        pending: dict[TopicPartition, list[KafkaCommitTask]],
    ) -> list[asyncio.Task[typing.Any]]:
        # Watch only the first not-done task per partition. A cancelled head is treated as
        # done by _extract_ready_prefixes, so it is intentionally not watched (would busy-loop).
        heads: list[asyncio.Task[typing.Any]] = []
        for partition_pending in pending.values():
            for ct in partition_pending:
                if ct.asyncio_task.cancelled():
                    break
                if not ct.asyncio_task.done():
                    heads.append(ct.asyncio_task)
                    break
        return heads

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
        )

        try:
            while not (state.should_shutdown and not state.pending):
                await self._streaming_iteration(state)
        finally:
            state.cancel_outstanding()

    async def _streaming_iteration(self, state: "_StreamingState") -> None:
        wait_targets: list[asyncio.Future[typing.Any]] = [state.flush_wait_task]
        if not state.should_shutdown:
            wait_targets.append(state.queue_get_task)
        if state.timeout_task is not None:
            wait_targets.append(state.timeout_task)
        wait_targets.extend(self._pending_head_tasks(state.pending))

        await asyncio.wait(wait_targets, return_when=asyncio.FIRST_COMPLETED)

        if not state.should_shutdown and state.queue_get_task.done():
            new_ct = state.queue_get_task.result()
            state.pending.setdefault(new_ct.topic_partition, []).append(new_ct)
            state.queue_get_task = asyncio.create_task(self._messages_queue.get())
            if state.timeout_task is None:
                state.timeout_task = asyncio.create_task(asyncio.sleep(self._commit_batch_timeout_sec))

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
                state.pending.setdefault(ct.topic_partition, []).append(ct)
            if not state.queue_get_task.done():
                state.queue_get_task.cancel()
        else:
            state.flush_in_progress = True
        self._flush_batch_event.clear()
        state.flush_wait_task = asyncio.create_task(self._flush_batch_event.wait())

    async def _maybe_commit(
        self, state: "_StreamingState", timeout_fired: bool
    ) -> dict[TopicPartition, list[KafkaCommitTask]]:
        total_pending: typing.Final = sum(len(p) for p in state.pending.values())
        commit_triggered: typing.Final = (
            total_pending >= self._commit_batch_size
            or timeout_fired
            or state.flush_in_progress
            or state.should_shutdown
        )
        if not commit_triggered:
            return {}
        ready: typing.Final = self._extract_ready_prefixes(state.pending)
        if ready:
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
