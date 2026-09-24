import inspect
import logging
import typing

from aiokafka import ConsumerRebalanceListener as BaseConsumerRebalanceListener
from faststream.kafka.subscriber.usecase import BatchSubscriber, LogicSubscriber
from faststream.middlewares import AckPolicy

from faststream_concurrent_aiokafka import consts
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter


if typing.TYPE_CHECKING:
    from aiokafka.structs import TopicPartition
    from faststream import ContextRepo

    from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler


logger = logging.getLogger(__name__)


class ConsumerRebalanceListener(BaseConsumerRebalanceListener):
    """Commits all pending offsets when Kafka revokes partitions during rebalance.

    Without this listener, in-flight message tasks whose offsets have not yet been
    batch-committed will be redelivered to another consumer after a rebalance, causing
    duplicate processing.

    ``initialize_concurrent_processing`` attaches one to every concurrent subscriber (see
    ``attach_rebalance_listeners``). Where that is not possible, pass one explicitly; the
    context form looks the handler up on each revocation, so it can be declared before the
    lifespan creates the handler::

        @broker.subscriber(
            "my-topic",
            group_id="my-group",
            ack_policy=AckPolicy.MANUAL,
            listener=ConsumerRebalanceListener.from_context(broker.context),
        )
        async def handle(msg: str) -> None:
            ...

    """

    def __init__(
        self,
        committer: KafkaBatchCommitter,
        flush_timeout_sec: float = consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC,
    ) -> None:
        self._committer = committer
        self._flush_timeout_sec = flush_timeout_sec

    @classmethod
    def from_context(
        cls,
        context: "ContextRepo",
        flush_timeout_sec: float = consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC,
    ) -> "ConsumerRebalanceListener":
        """Return a listener that finds the running concurrent handler in ``context`` on each revocation.

        A revocation while concurrent processing is not running (before
        ``initialize_concurrent_processing`` or after ``stop_concurrent_processing``) is a no-op.
        """
        return _ContextConsumerRebalanceListener(context, flush_timeout_sec)

    def _resolve_committer(self) -> KafkaBatchCommitter | None:
        return self._committer

    async def on_partitions_assigned(self, _assigned: object) -> None:  # ty: ignore[invalid-method-override]
        pass

    async def on_partitions_revoked(self, revoked: object) -> None:
        committer: typing.Final = self._resolve_committer()
        if committer is None:
            return
        await committer.commit_all(self._flush_timeout_sec)
        # The revoked partitions' next assignment (possibly to another consumer) starts
        # fresh, so the cancellation floor — if any was set — must not carry over.
        committer.clear_cancellation_watermarks(typing.cast("typing.Iterable[TopicPartition]", revoked))


class _ContextConsumerRebalanceListener(ConsumerRebalanceListener):
    def __init__(
        self,
        context: "ContextRepo",
        flush_timeout_sec: float,
        chained: BaseConsumerRebalanceListener | None = None,
    ) -> None:
        self._context = context
        self._flush_timeout_sec = flush_timeout_sec
        self._chained = chained

    def _resolve_committer(self) -> KafkaBatchCommitter | None:
        handler: typing.Final[KafkaConcurrentHandler | None] = self._context.get(consts.PROCESSING_CONTEXT_KEY)
        if handler is None or not handler.is_running:
            return None
        return handler.committer

    async def on_partitions_assigned(self, assigned: object) -> None:  # ty: ignore[invalid-method-override]
        if self._chained is not None:
            await _call_or_await(self._chained.on_partitions_assigned(assigned))

    async def on_partitions_revoked(self, revoked: object) -> None:
        await super().on_partitions_revoked(revoked)
        if self._chained is not None:
            await _call_or_await(self._chained.on_partitions_revoked(revoked))


async def _call_or_await(result: object) -> None:
    if inspect.isawaitable(result):
        await result


def attach_rebalance_listeners(context: "ContextRepo", flush_timeout_sec: float) -> None:
    """Give every subscriber this library processes a listener that flushes on revocation.

    Must run before the broker starts: FastStream hands the listener to aiokafka when the
    consumer subscribes. A listener the user passed is kept and called after the flush.
    """
    brokers: typing.Final = getattr(context.get("app"), "brokers", None)
    if not brokers:
        logger.error(
            "Kafka middleware. No FastStream application in the context, so no rebalance listener was attached; "
            "pass listener=ConsumerRebalanceListener.from_context(broker.context) to each concurrent subscriber"
        )
        return
    for broker in brokers:
        for subscriber in broker.subscribers:
            _attach(subscriber, context, flush_timeout_sec, broker_running=broker.running)


def _attach(subscriber: object, context: "ContextRepo", flush_timeout_sec: float, *, broker_running: bool) -> None:
    if (
        not isinstance(subscriber, LogicSubscriber)
        or isinstance(subscriber, BatchSubscriber)
        or subscriber.ack_policy is not AckPolicy.MANUAL
        or not (subscriber.topics or subscriber.pattern)
        or isinstance(subscriber._listener, ConsumerRebalanceListener)  # noqa: SLF001
    ):
        return
    if broker_running:
        logger.error(
            "Kafka middleware. Broker already started, so no rebalance listener was attached to the subscriber "
            "for topics %s; call initialize_concurrent_processing before the broker starts (in the lifespan)",
            subscriber.topics or subscriber.pattern,
        )
        return
    subscriber._listener = _ContextConsumerRebalanceListener(  # noqa: SLF001
        context,
        flush_timeout_sec,
        chained=subscriber._listener,  # noqa: SLF001
    )
