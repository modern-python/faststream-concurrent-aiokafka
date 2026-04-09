import logging
import typing

from faststream import BaseMiddleware, ContextRepo
from faststream.kafka.message import KafkaAckableMessage

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
            logger.exception("Kafka middleware. There is no concurrent processing instance in the context")
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
        if concurrent_processing.enable_batch_commit and not kafka_message:
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
    commit_batch_size: int,
    commit_batch_timeout_sec: int,
    concurrency_limit: int = DEFAULT_CONCURRENCY_LIMIT,
    enable_batch_commit: bool = False,
) -> None:
    concurrent_processing: typing.Final = KafkaConcurrentHandler(
        commit_batch_size=commit_batch_size,
        commit_batch_timeout_sec=commit_batch_timeout_sec,
        concurrency_limit=concurrency_limit,
        enable_batch_commit=enable_batch_commit,
    )
    if concurrent_processing.is_running:
        logger.warning("Kafka middleware. Processing is already active")
        return
    try:
        await concurrent_processing.start()
    except Exception as exc:
        logger.exception("Kafka middleware. Cannot start concurrent processing")
        msg: typing.Final = "Kafka middleware. Cannot start concurrent processing"
        raise RuntimeError(msg) from exc

    context.set_global(_PROCESSING_CONTEXT_KEY, concurrent_processing)
    logger.info("Kafka middleware. Concurrent processing is active")


async def stop_concurrent_processing(
    context: ContextRepo,
) -> None:
    concurrent_processing: typing.Final = KafkaConcurrentHandler()
    if not concurrent_processing.is_healthy:
        logger.warning("Kafka middleware. Concurrent processing is not running. Cannot stop")
        return

    await concurrent_processing.stop()
    context.set_global(_PROCESSING_CONTEXT_KEY, None)

    KafkaConcurrentHandler._initialized = False  #  noqa: SLF001
    KafkaConcurrentHandler._instance = None  #  noqa: SLF001
