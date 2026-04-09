import asyncio
import contextlib
import dataclasses
import itertools
import logging
import typing

from faststream.kafka import TopicPartition


if typing.TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer


logger = logging.getLogger(__name__)


GRACEFUL_TIMEOUT_SEC: typing.Final = 20


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
    ) -> None:
        self._messages_queue: asyncio.Queue[KafkaCommitTask] = asyncio.Queue()
        self._commit_task: asyncio.Task[typing.Any] | None = None
        self._flush_batch_event = asyncio.Event()

        self._commit_batch_timeout_sec = commit_batch_timeout_sec
        self._commit_batch_size = commit_batch_size
        self._shutdown_timeout = GRACEFUL_TIMEOUT_SEC

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

                # commit_all was called — flush remaining queue items and stop
                if flush_wait_task in done:
                    uncommited_tasks.extend(self._flush_tasks_queue())
                    self._flush_batch_event.clear()
                    should_shutdown = True
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
        commit_succeeded = True
        consumer: typing.Final[AIOKafkaConsumer] = tasks_batch[0].consumer
        try:
            await consumer.commit(partitions_to_offsets)
        except Exception as exc:
            commit_succeeded = False
            logger.exception("Error during commit to kafka", exc_info=exc)
            for task in tasks_batch:
                await self._messages_queue.put(task)
        return commit_succeeded

    @staticmethod
    def _map_offsets_per_partition(consumer_tasks: list[KafkaCommitTask]) -> dict[TopicPartition, int]:
        partitions_to_tasks = itertools.groupby(
            sorted(consumer_tasks, key=lambda x: x.topic_partition), lambda x: x.topic_partition
        )
        partitions_to_offsets: dict[TopicPartition, int] = {}
        for partition, partition_tasks in partitions_to_tasks:
            max_offset = max((task.offset for task in partition_tasks), default=None)
            if max_offset is not None:
                # Kafka commits the *next* offset to fetch, so committed = processed_max + 1
                partitions_to_offsets[partition] = max_offset + 1
        return partitions_to_offsets

    async def _commit_tasks_batch(self, tasks_batch: list[KafkaCommitTask]) -> bool:
        results: typing.Final = await asyncio.gather(
            *[task.asyncio_task for task in tasks_batch], return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                logger.error("Task has finished with an exception", exc_info=result)

        # Group by consumer instance — each AIOKafkaConsumer can only commit its own partitions
        consumers_tasks: dict[int, list[KafkaCommitTask]] = {}
        for task in tasks_batch:
            consumers_tasks.setdefault(id(task.consumer), []).append(task)

        all_succeeded = True
        for consumer_tasks in consumers_tasks.values():
            partitions_to_offsets = self._map_offsets_per_partition(consumer_tasks)
            if not await self._call_committer(consumer_tasks, partitions_to_offsets):
                all_succeeded = False

        for _ in tasks_batch:
            self._messages_queue.task_done()
        return all_succeeded

    async def _run_commit_process(self) -> None:
        should_shutdown = False
        while not should_shutdown:
            commit_batch, should_shutdown = await self._populate_commit_batch()
            if commit_batch:
                await self._commit_tasks_batch(commit_batch)

    async def commit_all(self) -> None:
        """Flush and commit all pending tasks, then stop the committer loop."""
        self._flush_batch_event.set()
        await self._messages_queue.join()

    async def send_task(self, new_task: KafkaCommitTask) -> None:
        self._check_is_commit_task_running()
        await self._messages_queue.put(
            new_task,
        )

    def spawn(self) -> None:
        if not self._commit_task:
            self._commit_task = asyncio.create_task(self._run_commit_process())
        else:
            logger.error("Committer main task already running")

    async def close(self) -> None:
        """Close committer."""
        if not self._commit_task:
            logger.error("Committer main task is not running, cannot close committer properly")
            return

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
