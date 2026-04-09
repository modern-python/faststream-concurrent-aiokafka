import asyncio
import contextlib
import dataclasses
import itertools
import logging
import threading
import typing

from faststream.kafka import TopicPartition


if typing.TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer


logger = logging.getLogger(__name__)


SHUTDOWN_TIMEOUT_SEC: typing.Final = 20


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
        self._asyncio_commit_process_task: asyncio.Task[typing.Any] | None = None
        self._flush_batch_event = asyncio.Event()

        self._commit_batch_timeout_sec = commit_batch_timeout_sec
        self._commit_batch_size = commit_batch_size
        self._shutdown_timeout = SHUTDOWN_TIMEOUT_SEC

        self._spawn_lock = threading.Lock()

    def _check_is_commit_task_running(self) -> None:
        is_commit_task_running: typing.Final[bool] = bool(
            self._asyncio_commit_process_task
            and not self._asyncio_commit_process_task.cancelled()
            and not self._asyncio_commit_process_task.done(),
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
        flush_wait_task: asyncio.Task[typing.Any] | None = None
        timeout_task: asyncio.Task[typing.Any] | None = None
        try:
            timeout_task = asyncio.create_task(asyncio.sleep(self._commit_batch_timeout_sec))
            while len(uncommited_tasks) < self._commit_batch_size:
                queue_get_task = asyncio.create_task(self._messages_queue.get())
                flush_wait_task = asyncio.create_task(self._flush_batch_event.wait())
                await asyncio.wait([queue_get_task, flush_wait_task, timeout_task], return_when=asyncio.FIRST_COMPLETED)

                if queue_get_task.done():
                    uncommited_tasks.append(queue_get_task.result())
                else:
                    queue_get_task.cancel()

                # commit_all is called
                if flush_wait_task.done():
                    queue_get_task.cancel()
                    uncommited_tasks.extend(self._flush_tasks_queue())
                    self._flush_batch_event.clear()
                    timeout_task.cancel()
                    should_shutdown = True
                    break
                flush_wait_task.cancel()

                if timeout_task.done():
                    logger.debug("Timeout exceeded, batch contains %s elements", len(uncommited_tasks))
                    break

            logger.debug("Batch condition reached with %s elements", len(uncommited_tasks))
        except asyncio.CancelledError:
            should_shutdown = True
            uncommited_tasks.extend(self._flush_tasks_queue())

        for task in (queue_get_task, flush_wait_task, timeout_task):
            task and task.cancel()

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

    async def _commit_tasks_batch(self, tasks_batch: list[KafkaCommitTask]) -> bool:
        partitions_to_tasks: typing.Final = itertools.groupby(
            sorted(tasks_batch, key=lambda x: x.topic_partition), lambda x: x.topic_partition
        )

        results: typing.Final = await asyncio.gather(
            *[task.asyncio_task for task in tasks_batch], return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                logger.error("Task has finished with an exception", exc_info=result)

        partitions_to_offsets: typing.Final[dict[TopicPartition, int]] = {}
        partition: TopicPartition
        tasks: typing.Iterator[KafkaCommitTask]
        for partition, tasks in partitions_to_tasks:
            max_message_offset: int | None = None
            for task in tasks:
                if max_message_offset is None or task.offset > max_message_offset:
                    max_message_offset = task.offset

            if max_message_offset is not None:
                partitions_to_offsets[partition] = max_message_offset + 1

        commit_succeeded: typing.Final = await self._call_committer(tasks_batch, partitions_to_offsets)
        for _ in tasks_batch:
            self._messages_queue.task_done()
        return commit_succeeded

    async def _run_commit_process(self) -> None:
        should_shutdown = False
        while not should_shutdown:
            commit_batch, should_shutdown = await self._populate_commit_batch()
            if commit_batch:
                await self._commit_tasks_batch(commit_batch)

    async def commit_all(self) -> None:
        """Commit all without shutting down the main process."""
        self._flush_batch_event.set()
        await self._messages_queue.join()

    async def send_task(self, new_task: KafkaCommitTask) -> None:
        self._check_is_commit_task_running()
        await self._messages_queue.put(
            new_task,
        )

    def spawn(self) -> None:
        with self._spawn_lock:
            if not self._asyncio_commit_process_task:
                self._asyncio_commit_process_task = asyncio.create_task(self._run_commit_process())
            else:
                logger.error("Committer main task already running")

    async def close(self) -> None:
        """Close committer."""
        if not self._asyncio_commit_process_task:
            logger.error("Committer main task is not running, cannot close committer properly")
            return

        self._flush_batch_event.set()
        try:
            await asyncio.wait_for(self._asyncio_commit_process_task, timeout=self._shutdown_timeout)
        except TimeoutError:
            logger.exception("Committer main task shutdown timed out, forcing cancellation")
            self._asyncio_commit_process_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._asyncio_commit_process_task
        except Exception as exc:
            logger.exception("Committer task failed during shutdown", exc_info=exc)
            raise

    @property
    def is_healthy(self) -> bool:
        return self._asyncio_commit_process_task is not None and not self._asyncio_commit_process_task.done()
