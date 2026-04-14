import asyncio
import functools
import logging
import signal
import typing

from faststream.kafka import ConsumerRecord, TopicPartition
from faststream.kafka.message import KafkaAckableMessage

from faststream_concurrent_aiokafka import batch_committer
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter
from faststream_concurrent_aiokafka.rebalance import ConsumerRebalanceListener


logger = logging.getLogger(__name__)


SIGNALS: typing.Final = (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)
GRACEFUL_TIMEOUT_SEC: typing.Final[int] = 10
DEFAULT_CONCURRENCY_LIMIT: typing.Final = 10


class KafkaConcurrentHandler:
    def __init__(
        self,
        committer: KafkaBatchCommitter,
        concurrency_limit: int = DEFAULT_CONCURRENCY_LIMIT,
    ) -> None:
        if concurrency_limit < 1:
            msg = f"concurrency_limit must be >= 1, got {concurrency_limit}"
            raise ValueError(msg)

        self._limiter = asyncio.Semaphore(concurrency_limit)
        self._current_tasks: set[asyncio.Task[typing.Any]] = set()
        self._is_running: bool = False
        self._committer: KafkaBatchCommitter = committer
        self._stop_task: asyncio.Task[typing.Any] | None = None

    async def wait_for_subtasks(self) -> None:
        logger.info("Kafka middleware. Gracefully waiting for tasks to end...")
        try:
            await asyncio.wait_for(asyncio.gather(*self._current_tasks, return_exceptions=True), GRACEFUL_TIMEOUT_SEC)
        except TimeoutError:
            logger.exception("Kafka middleware. Whoops, some tasks haven't finished in graceful time, sorry")

    def _finish_task(self, task: asyncio.Task[typing.Any]) -> None:
        self._limiter.release()
        if not task.cancelled():
            exc: typing.Final[BaseException | None] = task.exception()
            if exc:
                logger.error("Kafka middleware. Task has failed with the exception", exc_info=exc)
        self._current_tasks.discard(task)

    async def handle_task(
        self,
        coroutine: typing.Coroutine[typing.Any, typing.Any, typing.Any],
        record: ConsumerRecord,
        kafka_message: KafkaAckableMessage,
    ) -> None:
        if self._limiter:
            await self._limiter.acquire()
        task: typing.Final = asyncio.create_task(coroutine)
        self._current_tasks.add(task)
        task.add_done_callback(self._finish_task)
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
        self._stop_task = asyncio.create_task(self.stop())

    async def start(self) -> None:
        if self._is_running:
            return

        logger.info("Kafka middleware. Start middleware handler")
        self._is_running = True

        self._committer.spawn()
        self._setup_signal_handlers()
        logger.info("Kafka middleware is ready to process messages.")

    async def stop(self) -> None:
        if not self._is_running:
            return
        logger.info("Kafka middleware. Shutting down middleware handler")
        self._is_running = False

        await self._committer.close()
        await self.wait_for_subtasks()

        try:
            loop = asyncio.get_running_loop()
            for sig in SIGNALS:
                loop.remove_signal_handler(sig)
        except Exception:  # noqa: BLE001
            logger.warning("Kafka middleware. Exception raised while removing signal handlers", exc_info=True)
        logger.info("Kafka middleware. Complete shutting down middleware handler")

    async def force_cancel_all(self) -> None:
        logger.warning("Kafka middleware. Force cancelling all tasks!")
        self._is_running = False

        tasks = list(self._current_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._current_tasks.clear()

    def create_rebalance_listener(self) -> ConsumerRebalanceListener:
        """Return a ConsumerRebalanceListener that flushes pending commits on partition revocation.

        Pass the returned listener to ``@broker.subscriber(..., listener=listener)`` so that
        in-flight offsets are committed before Kafka hands the partition to another consumer.
        """
        return ConsumerRebalanceListener(self._committer)

    @property
    def is_healthy(self) -> bool:
        return self._is_running and self._committer.is_healthy

    @property
    def is_running(self) -> bool:
        return self._is_running
