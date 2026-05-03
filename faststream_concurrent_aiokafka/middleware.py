import contextlib
import dataclasses
import logging
import typing
import weakref

from faststream import BaseMiddleware, ContextRepo
from faststream.kafka.message import KafkaAckableMessage

from faststream_concurrent_aiokafka.batch_committer import CommitterIsDeadError, KafkaBatchCommitter
from faststream_concurrent_aiokafka.processing import (
    DEFAULT_CONCURRENCY_LIMIT,
    DEFAULT_SHUTDOWN_TIMEOUT_SEC,
    KafkaConcurrentHandler,
)


if typing.TYPE_CHECKING:
    from faststream.kafka import ConsumerRecord


_PROCESSING_CONTEXT_KEY: typing.Final = "concurrent_processing"
logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, slots=True)
class _ConsumerAttrs:
    is_fake: bool
    auto_commit: bool


# Static, per-consumer flags that drive the per-message branch in consume_scope. Reading
# them on every message via type().__name__ and getattr was visible in profiles. WeakKey
# keeps the cache empty when consumers are GC'd; tests that build many MagicMock consumers
# don't leak.
_consumer_attrs_cache: typing.Final[weakref.WeakKeyDictionary[typing.Any, _ConsumerAttrs]] = weakref.WeakKeyDictionary()


def _consumer_attrs(consumer: typing.Any) -> _ConsumerAttrs:  # noqa: ANN401
    cached: typing.Final = _consumer_attrs_cache.get(consumer)
    if cached is not None:
        return cached
    attrs: typing.Final = _ConsumerAttrs(
        is_fake=type(consumer).__name__ == "FakeConsumer",
        auto_commit=bool(getattr(consumer, "_enable_auto_commit", False)),
    )
    # Consumer may not be weakreferable (rare, e.g. exotic mock subclasses); fall through.
    with contextlib.suppress(TypeError):
        _consumer_attrs_cache[consumer] = attrs
    return attrs


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

        attrs: typing.Final = _consumer_attrs(kafka_message.consumer)
        if attrs.is_fake:
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

        if attrs.auto_commit:
            err = (
                "KafkaConcurrentProcessingMiddleware requires ack_policy=AckPolicy.MANUAL on all subscribers. "
                "Auto-commit is enabled on this consumer, which commits offsets before processing tasks "
                "complete and can cause message loss on crash. "
                "Add ack_policy=AckPolicy.MANUAL to your @broker.subscriber(...) decorator."
            )
            raise RuntimeError(err)

        try:
            await concurrent_processing.handle_task(
                call_next(msg),
                typing.cast("ConsumerRecord", self.msg),
                kafka_message,
            )
        except CommitterIsDeadError:
            # Race with shutdown: stop() ran between our is_running check and send_task.
            # The user handler already fired; the offset stays uncommitted, so the message
            # will be redelivered on restart (at-least-once).
            logger.warning("Kafka middleware. Handler is shutting down, skipping message")
        return None


async def initialize_concurrent_processing(
    context: ContextRepo,
    concurrency_limit: int = DEFAULT_CONCURRENCY_LIMIT,
    commit_batch_size: int = 10,
    commit_batch_timeout_sec: float = 10.0,
    shutdown_timeout_sec: float = DEFAULT_SHUTDOWN_TIMEOUT_SEC,
) -> KafkaConcurrentHandler:
    existing: KafkaConcurrentHandler | None = context.get(_PROCESSING_CONTEXT_KEY)
    if existing and existing.is_running:
        logger.warning("Kafka middleware. Processing is already active")
        return existing

    concurrent_processing: typing.Final = KafkaConcurrentHandler(
        committer=KafkaBatchCommitter(
            commit_batch_timeout_sec=commit_batch_timeout_sec,
            commit_batch_size=commit_batch_size,
            shutdown_timeout_sec=shutdown_timeout_sec,
        ),
        concurrency_limit=concurrency_limit,
        shutdown_timeout_sec=shutdown_timeout_sec,
    )
    await concurrent_processing.start()
    context.set_global(_PROCESSING_CONTEXT_KEY, concurrent_processing)
    logger.info("Kafka middleware. Concurrent processing is active")
    return concurrent_processing


async def stop_concurrent_processing(
    context: ContextRepo,
) -> None:
    concurrent_processing: typing.Final[KafkaConcurrentHandler | None] = context.get(_PROCESSING_CONTEXT_KEY)
    if concurrent_processing is None or not concurrent_processing.is_running:
        logger.warning("Kafka middleware. Concurrent processing is not running. Cannot stop")
        return

    await concurrent_processing.stop()
    context.set_global(_PROCESSING_CONTEXT_KEY, None)
