import typing

from aiokafka import ConsumerRebalanceListener as BaseConsumerRebalanceListener

from faststream_concurrent_aiokafka import consts
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter


if typing.TYPE_CHECKING:
    from aiokafka.structs import TopicPartition
    from faststream import ContextRepo

    from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler


class ConsumerRebalanceListener(BaseConsumerRebalanceListener):
    """Commits all pending offsets when Kafka revokes partitions during rebalance.

    Without this listener, in-flight message tasks whose offsets have not yet been
    batch-committed will be redelivered to another consumer after a rebalance, causing
    duplicate processing.

    Subscribers are usually declared before the lifespan creates the concurrent handler,
    so build the listener from the broker's context; the handler is looked up on each
    revocation::

        broker = KafkaBroker(...)
        broker.add_middleware(KafkaConcurrentProcessingMiddleware)

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
    def __init__(self, context: "ContextRepo", flush_timeout_sec: float) -> None:
        self._context = context
        self._flush_timeout_sec = flush_timeout_sec

    def _resolve_committer(self) -> KafkaBatchCommitter | None:
        handler: typing.Final[KafkaConcurrentHandler | None] = self._context.get(consts.PROCESSING_CONTEXT_KEY)
        if handler is None or not handler.is_running:
            return None
        return handler.committer
