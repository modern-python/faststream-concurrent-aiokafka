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
        is_commit_task_running = bool(
            self._commit_task and not self._commit_task.cancelled() and not self._commit_task.done(),
        )
        if not is_commit_task_running:
            msg: typing.Final = "Committer main task is not running"
            raise CommitterIsDeadError(msg)

    def _flush_tasks_queue(self) -> list[KafkaCommitTask]:
        tasks_to_return: typing.Final[list[KafkaCommitTask]] = []
        while not self._messages_queue.empty():
            tasks_to_return.append(self._messages_queue.get_nowait())
        return tasks_to_return

    async def _populate_commit_batch(self) -> tuple[list[KafkaCommitTask], bool]:
        uncommited_tasks: typing.Final[list[KafkaCommitTask]] = []
        should_shutdown = False
        queue_get_task: asyncio.Task[typing.Any] | None = None
        # Create timeout and flush-wait tasks once; reused across queue-get iterations.
        timeout_task: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(self._commit_batch_timeout_sec))
        flush_wait_task: asyncio.Task[bool] = asyncio.create_task(self._flush_batch_event.wait())
        try:
            while len(uncommited_tasks) < self._commit_batch_size:
                queue_get_task = asyncio.create_task(self._messages_queue.get())
                done, _ = await asyncio.wait(
                    [queue_get_task, flush_wait_task, timeout_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if queue_get_task in done:
                    uncommited_tasks.append(queue_get_task.result())
                else:
                    queue_get_task.cancel()

                # flush event — drain remaining queue items; stop only if close() was called
                if flush_wait_task in done:
                    uncommited_tasks.extend(self._flush_tasks_queue())
                    self._flush_batch_event.clear()
                    should_shutdown = self._stop_requested
                    break

                if timeout_task in done:
                    logger.debug("Timeout exceeded, batch contains %s elements", len(uncommited_tasks))
                    break

            logger.debug("Batch condition reached with %s elements", len(uncommited_tasks))
        except asyncio.CancelledError:
            should_shutdown = True
            uncommited_tasks.extend(self._flush_tasks_queue())

        for task in (queue_get_task, flush_wait_task, timeout_task):
            if task:
                task.cancel()

        return uncommited_tasks, should_shutdown

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
    def _partition_ready(
        pending: list[KafkaCommitTask],
    ) -> tuple[list[KafkaCommitTask], list[KafkaCommitTask]]:
        # Per partition (sorted by offset), find the first task that is either cancelled or
        # not-done. Tasks before that boundary are ready. A cancelled boundary means
        # graceful-shutdown is in progress: the cancelled task and all later same-partition
        # tasks are added to ready too — _map_offsets_per_partition stops at the cancelled
        # offset (so nothing past it commits) and task_done() is called on all of them.
        # A not-done boundary keeps that task and everything after it on its partition blocked.
        by_partition: dict[TopicPartition, list[KafkaCommitTask]] = {}
        for task in pending:
            by_partition.setdefault(task.topic_partition, []).append(task)

        ready: list[KafkaCommitTask] = []
        still_blocked: list[KafkaCommitTask] = []
        for tasks in by_partition.values():
            tasks.sort(key=lambda t: t.offset)
            cancelled_at: int | None = None
            blocked_at: int | None = None
            for index, task in enumerate(tasks):
                if task.asyncio_task.cancelled():
                    cancelled_at = index
                    break
                if not task.asyncio_task.done():
                    blocked_at = index
                    break
            if cancelled_at is not None:
                ready.extend(tasks)
            elif blocked_at is not None:
                ready.extend(tasks[:blocked_at])
                still_blocked.extend(tasks[blocked_at:])
            else:
                ready.extend(tasks)
        return ready, still_blocked

    async def _commit_ready_slice(self, ready: list[KafkaCommitTask]) -> bool:
        for task in ready:
            if task.asyncio_task.cancelled():
                continue
            exc = task.asyncio_task.exception()
            if exc is not None:
                logger.error("Task has finished with an exception", exc_info=exc)

        # Group by consumer instance — each AIOKafkaConsumer can only commit its own partitions
        consumers_tasks: dict[int, list[KafkaCommitTask]] = {}
        for task in ready:
            consumers_tasks.setdefault(id(task.consumer), []).append(task)

        all_succeeded = True
        for consumer_tasks in consumers_tasks.values():
            partitions_to_offsets = self._map_offsets_per_partition(consumer_tasks)
            if not await self._call_committer(consumer_tasks, partitions_to_offsets):
                all_succeeded = False

        for _ in ready:
            self._messages_queue.task_done()
        return all_succeeded

    async def _commit_tasks_batch(self, tasks_batch: list[KafkaCommitTask]) -> bool:
        pending: list[KafkaCommitTask] = list(tasks_batch)
        all_succeeded = True

        while pending:
            ready, still_blocked = self._partition_ready(pending)
            if ready:
                if not await self._commit_ready_slice(ready):
                    all_succeeded = False
                pending = still_blocked
                continue

            # _partition_ready places every done/cancelled task in ready, so an empty
            # ready implies every pending task is still in-flight.
            await asyncio.wait([t.asyncio_task for t in pending], return_when=asyncio.FIRST_COMPLETED)

        return all_succeeded

    async def _run_commit_process(self) -> None:
        should_shutdown = False
        while not should_shutdown:
            commit_batch, should_shutdown = await self._populate_commit_batch()
            if commit_batch:
                await self._commit_tasks_batch(commit_batch)

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
