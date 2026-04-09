import asyncio
import contextlib
import functools
import logging
import signal
import threading
import typing

from faststream.kafka import ConsumerRecord, TopicPartition
from faststream.kafka.message import KafkaAckableMessage

from faststream_concurrent_aiokafka import batch_committer
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter


logger = logging.getLogger(__name__)


TOPIC_GROUP_KEY: typing.Final = "topic_group"
SIGNALS: typing.Final = (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)
GRACEFUL_TIMEOUT_SEC: typing.Final[int] = 10
DEFAULT_OBSERVER_INTERVAL_SEC: typing.Final[float] = 5.0
DEFAULT_CONCURRENCY_LIMIT: typing.Final = 10


class KafkaConcurrentHandler:
    _instance: typing.ClassVar["typing.Self | None"] = None
    _lock: typing.ClassVar[threading.Lock] = threading.Lock()
    _initialized: bool = False

    def __new__(cls, *args: typing.Any, **kwargs: typing.Any) -> typing.Self:  # noqa: ARG004, ANN401
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(
        self,
        concurrency_limit: int = DEFAULT_CONCURRENCY_LIMIT,
        commit_batch_timeout_sec: float = 10.0,
        commit_batch_size: int = 10,
        enable_batch_commit: bool = False,
        observer_interval: float = DEFAULT_OBSERVER_INTERVAL_SEC,
    ) -> None:
        if self._initialized:
            return

        self.limiter = asyncio.Semaphore(concurrency_limit) if concurrency_limit != 0 else None
        self._current_tasks: set[asyncio.Task[typing.Any]] = set()

        self._observer_task: asyncio.Task[typing.Any] | None = None
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._observer_interval: float = observer_interval
        self._is_running: bool = False

        self.enable_batch_commit = enable_batch_commit
        self._commit_batch_timeout_sec: float = commit_batch_timeout_sec
        self._commit_batch_size: int = commit_batch_size

        self._committer: KafkaBatchCommitter | None = None
        self._initialized = True

    def _is_need_to_process_message(self, message: KafkaAckableMessage) -> bool:
        headers_topic_group: typing.Final[str | None] = message.headers.get(TOPIC_GROUP_KEY)
        group_id: typing.Final[str] = getattr(message.consumer, "_group_id", "")
        return headers_topic_group is None or headers_topic_group == group_id

    async def wait_for_subtasks(self) -> None:
        logger.info("Kafka middleware. Gracefully waiting for tasks to end...")
        try:
            await asyncio.wait_for(asyncio.gather(*self._current_tasks, return_exceptions=True), GRACEFUL_TIMEOUT_SEC)
        except TimeoutError:
            logger.exception("Kafka middleware. Whoops, some tasks haven't finished in graceful time, sorry")

    def _finish_task(self, task: asyncio.Task[typing.Any]) -> None:
        if self.limiter:
            self.limiter.release()
        exc: typing.Final[BaseException | None] = task.exception()
        if exc:
            logger.error("Kafka middleware. Kafka middleware. Task has failed with the exception", exc_info=exc)
        self._current_tasks.discard(task)

    async def handle_task(
        self,
        coroutine: typing.Coroutine[typing.Any, typing.Any, typing.Any],
        record: ConsumerRecord,
        kafka_message: KafkaAckableMessage,
    ) -> None:
        if self.limiter:
            await self.limiter.acquire()
        if self._is_need_to_process_message(kafka_message):
            task: typing.Final = asyncio.create_task(coroutine)
            self._current_tasks.add(task)
            task.add_done_callback(self._finish_task)
            if self.enable_batch_commit and self._committer:
                try:
                    await self._committer.send_task(
                        batch_committer.KafkaCommitTask(
                            asyncio_task=task,
                            offset=record.offset,
                            consumer=kafka_message.consumer,
                            topic_partition=TopicPartition(topic=record.topic, partition=record.partition),
                        )
                    )
                except batch_committer.CommitterIsDeadError:
                    logger.exception("Kafka middleware. Committer is dead")
                    await self.stop()
                    raise
        elif self.limiter:
            self.limiter.release()

    async def _check_tasks_health(self) -> None:
        to_discard: typing.Final = []
        for task in self._current_tasks:
            if task.done():
                to_discard.append(task)
        for task in to_discard:
            self._current_tasks.discard(task)
            if self.limiter:
                self.limiter.release()
        if to_discard:
            logger.info(f"Kafka middleware. Found completed but not discarded tasks, amount: {len(to_discard)}")

    async def observer(self) -> None:
        """Background observer task that monitors system health."""
        logger.info("Kafka middleware. Observer task started")

        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self._observer_interval,
                )
            except TimeoutError:
                await self._check_tasks_health()

    def _setup_signal_handlers(self) -> None:
        loop: typing.Final = asyncio.get_running_loop()
        for sig in SIGNALS:
            loop.add_signal_handler(
                sig,
                functools.partial(self._signal_handler, sig),
            )
            logger.debug(f"Kafka middleware. Registered handler for {sig.name}")

    def _signal_handler(self, sig: signal.Signals) -> None:
        logger.info(f"Kafka middleware. Received signal {sig.name}, initiating graceful shutdown...")
        asyncio.create_task(self.stop())  # noqa: RUF006

    async def start(self) -> None:
        if self._is_running:
            return
        logger.info("Kafka middleware. Start middleware handler")
        self._is_running = True
        self._shutdown_event.clear()

        if self.enable_batch_commit:
            self._committer = KafkaBatchCommitter(self._commit_batch_timeout_sec, self._commit_batch_size)
            self._committer.spawn()
        self._observer_task = asyncio.create_task(self.observer())
        self._setup_signal_handlers()
        logger.info("Kafka middleware is ready to process messages.")

    async def stop(self) -> None:
        if not self._is_running:
            return
        logger.info("Kafka middleware. Shutting down middleware handler")
        self._is_running = False
        self._shutdown_event.set()

        if self._committer:
            await self._committer.close()
        if self._observer_task and not self._observer_task.done():
            self._observer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._observer_task
        await self.wait_for_subtasks()

        try:
            loop: typing.Final = asyncio.get_running_loop()
            for sig in SIGNALS:
                loop.remove_signal_handler(sig)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Kafka middleware. Exception raised: {exc}")
        logger.info("Kafka middleware. Complete shutting down middleware handler")

    async def force_cancel_all(self) -> None:
        logger.warning("Kafka middleware. Force cancelling all tasks!")
        self._is_running = False

        for task in self._current_tasks:
            task.cancel()
        if self._observer_task and not self._observer_task.done():
            self._observer_task.cancel()
        await asyncio.sleep(0.5)
        self._current_tasks.clear()

    @property
    def is_healthy(self) -> bool:
        status = self._is_running and self._observer_task is not None and not self._observer_task.done()
        if self._committer:
            status = status and self._committer.is_healthy
        return status

    @property
    def is_running(self) -> bool:
        return self._is_running
