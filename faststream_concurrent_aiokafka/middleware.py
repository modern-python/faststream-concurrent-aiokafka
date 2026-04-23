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
        kafka_message: typing.Final = self.context.get("message")
        if not kafka_message:
            err = "No Kafka message found in context. Ensure the middleware is used with a Kafka subscriber."
            raise RuntimeError(err)

        if type(kafka_message.consumer).__name__ == "FakeConsumer":
            return await call_next(msg)

        # KafkaAckableMessage (AckPolicy.MANUAL) starts with committed=None.
        # KafkaMessage (any auto-ack policy) starts with committed=AckStatus.ACKED.
        # Non-MANUAL subscribers have offsets managed by FastStream's own
        # AcknowledgementMiddleware; firing them as background tasks would cause
        # FastStream to ack before the task completes, risking message loss on crash.
        if kafka_message.committed is not None:
            return await call_next(msg)

        if not concurrent_processing:
            err = "Concurrent processing is not running. Call `initialize_concurrent_processing` on app startup."
            raise RuntimeError(err)

        if not concurrent_processing.is_running:
            # Handler is shutting down. Skip processing — offset is not committed, so the
            # message will be redelivered on restart (at-least-once). Committing sequentially
            # here would jump ahead of in-flight task offsets and risk silent message loss.
            logger.warning("Kafka middleware. Handler is shutting down, skipping message")
            return None

        if getattr(kafka_message.consumer, "_enable_auto_commit", False):
            err = (
                "KafkaConcurrentProcessingMiddleware requires ack_policy=AckPolicy.MANUAL on all subscribers. "
                "Auto-commit is enabled on this consumer, which commits offsets before processing tasks "
                "complete and can cause message loss on crash. "
                "Add ack_policy=AckPolicy.MANUAL to your @broker.subscriber(...) decorator."
            )
            raise RuntimeError(err)

        await concurrent_processing.handle_task(call_next(msg), self.msg, kafka_message)
        return None


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
        committer=KafkaBatchCommitter(
            commit_batch_timeout_sec=commit_batch_timeout_sec,
            commit_batch_size=commit_batch_size,
        ),
        concurrency_limit=concurrency_limit,
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
