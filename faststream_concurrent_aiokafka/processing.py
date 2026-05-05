import asyncio
import logging
import typing

from faststream.kafka import ConsumerRecord, TopicPartition
from faststream.kafka.message import KafkaAckableMessage

from faststream_concurrent_aiokafka import batch_committer
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter
from faststream_concurrent_aiokafka.rebalance import ConsumerRebalanceListener


logger = logging.getLogger(__name__)


DEFAULT_CONCURRENCY_LIMIT: typing.Final = 10
DEFAULT_SHUTDOWN_TIMEOUT_SEC: typing.Final = 20.0


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
        # Tracked only so stop() can cancel them. The committer is the source of truth for
        # offset progress; this set just lets us reach in-flight tasks at shutdown.
        self._tracked_tasks: set[asyncio.Task[typing.Any]] = set()
        self._is_running: bool = False
        self._committer: KafkaBatchCommitter = committer

    def _finish_task(self, task: asyncio.Task[typing.Any]) -> None:
        self._limiter.release()
        self._tracked_tasks.discard(task)
        if not task.cancelled():
            exc: typing.Final[BaseException | None] = task.exception()
            if exc:
                logger.error("Kafka middleware. Task has failed with the exception", exc_info=exc)

    async def handle_task(
        self,
        coroutine: typing.Awaitable[typing.Any],
        record: ConsumerRecord,
        kafka_message: KafkaAckableMessage,
    ) -> None:
        await self._limiter.acquire()
        task: typing.Final = asyncio.ensure_future(coroutine)
        self._tracked_tasks.add(task)
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

    async def start(self) -> None:
        if self._is_running:
            return

        logger.info("Kafka middleware. Start middleware handler")
        self._is_running = True

        self._committer.spawn()
        logger.info("Kafka middleware is ready to process messages.")

    async def stop(self) -> None:
        if not self._is_running:
            return
        logger.info("Kafka middleware. Shutting down middleware handler")
        self._is_running = False

        # Cancel in-flight user tasks. The committer treats cancelled tasks as a hard
        # offset boundary (batch_committer._extract_ready_prefixes / _map_offsets_per_partition):
        # cancelled-and-after offsets stay uncommitted and get redelivered on restart.
        for task in list(self._tracked_tasks):
            if not task.done():
                task.cancel()

        await self._committer.close()

        logger.info("Kafka middleware. Complete shutting down middleware handler")

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
