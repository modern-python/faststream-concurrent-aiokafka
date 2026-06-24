import asyncio
import contextlib
import dataclasses
import logging
import typing
import weakref

from faststream import BaseMiddleware, ContextRepo
from faststream.kafka.message import KafkaAckableMessage

from faststream_concurrent_aiokafka import consts
from faststream_concurrent_aiokafka.batch_committer import CommitterIsDeadError, KafkaBatchCommitter
from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler


if typing.TYPE_CHECKING:
    from faststream.kafka import ConsumerRecord


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


@dataclasses.dataclass(frozen=True, slots=True)
class _PassThrough:
    """Process the message normally — a fake consumer, or a non-MANUAL-ack subscriber."""


@dataclasses.dataclass(frozen=True, slots=True)
class _Dispatch:
    """Fire the user handler as a concurrent task and defer the offset commit."""


@dataclasses.dataclass(frozen=True, slots=True)
class _Skip:
    """Handler is shutting down — drop the message (offset stays uncommitted, redelivered)."""


@dataclasses.dataclass(frozen=True, slots=True)
class _Refuse:
    """Misconfiguration — raise ``RuntimeError(reason)``."""

    reason: str


_Route = _PassThrough | _Dispatch | _Skip | _Refuse


def _classify(  # noqa: PLR0911 — a flat ordered classifier; one return per branch is the readable form
    *,
    committed: object | None,
    attrs: _ConsumerAttrs,
    handler: KafkaConcurrentHandler | None,
    is_batch: bool,
) -> _Route:
    """Decide how a message routes, as a pure function of its observable signals.

    The branch order is load-bearing: a multiply-misconfigured subscriber gets the error of
    the first matching branch (e.g. a batch subscriber is reported before auto-commit).
    """
    if attrs.is_fake:
        return _PassThrough()
    # KafkaAckableMessage (AckPolicy.MANUAL) starts with committed=None. KafkaMessage (any
    # auto-ack policy) starts with committed=AckStatus.ACKED. Non-MANUAL subscribers have
    # offsets managed by FastStream's own AcknowledgementMiddleware; firing them as background
    # tasks would ack before the task completes, risking message loss on crash.
    if committed is not None:
        return _PassThrough()
    if is_batch:
        return _Refuse(
            "KafkaConcurrentProcessingMiddleware does not support batch subscribers (batch=True). "
            "Use a non-batch subscriber, or remove the middleware from this subscriber."
        )
    if not handler:
        return _Refuse("Concurrent processing is not running. Call `initialize_concurrent_processing` on app startup.")
    if not handler.is_running:
        return _Skip()
    if attrs.auto_commit:
        return _Refuse(
            "KafkaConcurrentProcessingMiddleware requires ack_policy=AckPolicy.MANUAL on all subscribers. "
            "Auto-commit is enabled on this consumer, which commits offsets before processing tasks "
            "complete and can cause message loss on crash. "
            "Add ack_policy=AckPolicy.MANUAL to your @broker.subscriber(...) decorator."
        )
    return _Dispatch()


class KafkaConcurrentProcessingMiddleware(BaseMiddleware):
    # KafkaAckableMessage narrowing documents the MANUAL-ack design center; auto-ack/Fake paths
    # short-circuit before any narrowed access. Override widens to StreamMessage[Any] semantically
    # but ty flags the parameter narrowing as a Liskov violation.
    async def consume_scope(  # ty: ignore[invalid-method-override]
        self,
        call_next: typing.Callable[[KafkaAckableMessage], typing.Awaitable[typing.Any]],
        msg: KafkaAckableMessage,
    ) -> typing.Any:  # noqa: ANN401
        kafka_message: typing.Final = self.context.get("message")
        if not kafka_message:
            err = "No Kafka message found in context. Ensure the middleware is used with a Kafka subscriber."
            raise RuntimeError(err)

        concurrent_processing: typing.Final[KafkaConcurrentHandler] = self.context.get(consts.PROCESSING_CONTEXT_KEY)
        route: typing.Final = _classify(
            committed=kafka_message.committed,
            attrs=_consumer_attrs(kafka_message.consumer),
            handler=concurrent_processing,
            is_batch=isinstance(self.msg, (list, tuple)),
        )

        match route:
            case _PassThrough():
                return await call_next(msg)
            case _Refuse(reason):
                raise RuntimeError(reason)
            case _Skip():
                # Offset stays uncommitted, so the message is redelivered on restart
                # (at-least-once). Committing sequentially here would jump ahead of in-flight
                # task offsets and risk silent message loss.
                logger.warning("Kafka middleware. Handler is shutting down, skipping message")
                return None
            case _Dispatch():
                try:
                    await concurrent_processing.handle_task(
                        call_next(msg),
                        typing.cast("ConsumerRecord", self.msg),
                        kafka_message,
                    )
                except CommitterIsDeadError:
                    # Race with shutdown: stop() ran between the is_running check and send_task.
                    # The user handler already fired; the offset stays uncommitted, so the
                    # message will be redelivered on restart (at-least-once).
                    logger.warning("Kafka middleware. Handler is shutting down, skipping message")
                except asyncio.CancelledError:
                    # stop() cancelled this task while handle_task was awaiting send_task. Offset
                    # stays uncommitted → redelivered on restart. Propagate so FastStream's chain
                    # can run its own cleanup.
                    logger.warning("Kafka middleware. Task cancelled during shutdown")
                    raise
                return None
            case _:  # pragma: no cover - exhaustiveness guard; ty verifies _Route is fully covered
                typing.assert_never(route)


async def initialize_concurrent_processing(  # noqa: PLR0913
    context: ContextRepo,
    concurrency_limit: int = consts.DEFAULT_CONCURRENCY_LIMIT,
    commit_batch_size: int = consts.DEFAULT_COMMIT_BATCH_SIZE,
    commit_batch_timeout_sec: float = consts.DEFAULT_COMMIT_BATCH_TIMEOUT_SEC,
    shutdown_timeout_sec: float = consts.DEFAULT_SHUTDOWN_TIMEOUT_SEC,
    max_uncommitted_tasks: int | None = consts.DEFAULT_MAX_UNCOMMITTED_TASKS,
) -> KafkaConcurrentHandler:
    existing: KafkaConcurrentHandler | None = context.get(consts.PROCESSING_CONTEXT_KEY)
    if existing and existing.is_running:
        logger.warning("Kafka middleware. Processing is already active")
        return existing

    concurrent_processing: typing.Final = KafkaConcurrentHandler(
        committer=KafkaBatchCommitter(
            commit_batch_timeout_sec=commit_batch_timeout_sec,
            commit_batch_size=commit_batch_size,
            shutdown_timeout_sec=shutdown_timeout_sec,
            max_uncommitted_tasks=max_uncommitted_tasks,
        ),
        concurrency_limit=concurrency_limit,
    )
    await concurrent_processing.start()
    context.set_global(consts.PROCESSING_CONTEXT_KEY, concurrent_processing)
    logger.info("Kafka middleware. Concurrent processing is active")
    return concurrent_processing


async def stop_concurrent_processing(
    context: ContextRepo,
) -> None:
    concurrent_processing: typing.Final[KafkaConcurrentHandler | None] = context.get(consts.PROCESSING_CONTEXT_KEY)
    if concurrent_processing is None or not concurrent_processing.is_running:
        logger.warning("Kafka middleware. Concurrent processing is not running. Cannot stop")
        return

    await concurrent_processing.stop()
    context.set_global(consts.PROCESSING_CONTEXT_KEY, None)
