import logging
import typing

from faststream import BaseMiddleware, ContextRepo
from faststream.kafka.message import KafkaAckableMessage

from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter
from faststream_concurrent_aiokafka.processing import DEFAULT_CONCURRENCY_LIMIT, KafkaConcurrentHandler


_PROCESSING_CONTEXT_KEY: typing.Final = "concurrent_processing"
logger = logging.getLogger(__name__)


class KafkaConcurrentProcessingMiddleware(BaseMiddleware):
    async def consume_scope(  # ty: ignore[invalid-method-override]
        self,
        call_next: typing.Callable[[KafkaAckableMessage], typing.Awaitable[typing.Any]],
        msg: KafkaAckableMessage,
    ) -> typing.Any:  # noqa: ANN401
        concurrent_processing: typing.Final[KafkaConcurrentHandler] = self.context.get(_PROCESSING_CONTEXT_KEY)
        if not concurrent_processing:
            logger.error("Kafka middleware. There is no concurrent processing instance in the context")
            info = "No concurrent processing instance in the context"
            raise RuntimeError(info)

        if not concurrent_processing.is_running:
            logger.error(
                "Kafka middleware. Concurrent processing is not running. Maybe `initialize_concurrent_processing`"
                " was forgotten?"
            )
            info = "Concurrent processing is not running"
            raise RuntimeError(info)

        kafka_message: typing.Final = self.context.get("message")
        if concurrent_processing.has_batch_commit and not kafka_message:
            logger.error("Kafka middleware. No kafka message in the middleware, it means no consumer to commit batch.")
            info = "No kafka message in the middleware"
            raise RuntimeError(info)

        try:

            async def handler_wrapper() -> typing.Any:  # noqa: ANN401
                return await call_next(msg)

            await concurrent_processing.handle_task(handler_wrapper(), self.msg, kafka_message)
        except Exception as exc:
            raise RuntimeError(f"Kafka middleware. An error while sending task {msg}") from exc


async def initialize_concurrent_processing(
    context: ContextRepo,
    concurrency_limit: int = DEFAULT_CONCURRENCY_LIMIT,
    commit_batch_size: int = 10,
    commit_batch_timeout_sec: float = 10.0,
) -> KafkaConcurrentHandler:
    existing: KafkaConcurrentHandler | None = context.get(_PROCESSING_CONTEXT_KEY)
    if existing and existing.is_running:
        logger.warning("Kafka middleware. Processing is already active")
        return existing

    concurrent_processing: typing.Final = KafkaConcurrentHandler(
        concurrency_limit=concurrency_limit,
        committer=KafkaBatchCommitter(commit_batch_timeout_sec, commit_batch_size),
    )
    await concurrent_processing.start()
    context.set_global(_PROCESSING_CONTEXT_KEY, concurrent_processing)
    logger.info("Kafka middleware. Concurrent processing is active")
    return concurrent_processing


async def stop_concurrent_processing(
    context: ContextRepo,
) -> None:
    concurrent_processing: typing.Final[KafkaConcurrentHandler | None] = context.get(_PROCESSING_CONTEXT_KEY)
    if not concurrent_processing or not concurrent_processing.is_healthy:
        logger.warning("Kafka middleware. Concurrent processing is not running. Cannot stop")
        return

    await concurrent_processing.stop()
    context.set_global(_PROCESSING_CONTEXT_KEY, None)
