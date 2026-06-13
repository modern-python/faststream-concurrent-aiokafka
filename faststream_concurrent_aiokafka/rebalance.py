import typing

from aiokafka import ConsumerRebalanceListener as BaseConsumerRebalanceListener

from faststream_concurrent_aiokafka import consts
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter


if typing.TYPE_CHECKING:
    from faststream.kafka import TopicPartition


class ConsumerRebalanceListener(BaseConsumerRebalanceListener):
    """Commits all pending offsets when Kafka revokes partitions during rebalance.

    Without this listener, in-flight message tasks whose offsets have not yet been
    batch-committed will be redelivered to another consumer after a rebalance, causing
    duplicate processing.

    Usage::

        @asynccontextmanager
        async def lifespan(context: ContextRepo) -> AsyncIterator[None]:
            handler = await initialize_concurrent_processing(context, ...)
            listener = handler.create_rebalance_listener()

            @broker.subscriber("my-topic", listener=listener)
            async def handle(msg: str) -> None:
                ...

    Yield:
            await stop_concurrent_processing(context)

    """

    def __init__(
        self,
        committer: KafkaBatchCommitter,
        flush_timeout_sec: float = consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC,
    ) -> None:
        self._committer = committer
        self._flush_timeout_sec = flush_timeout_sec

    async def on_partitions_assigned(self, _assigned: object) -> None:  # ty: ignore[invalid-method-override]
        pass

    async def on_partitions_revoked(self, revoked: object) -> None:
        await self._committer.commit_all(self._flush_timeout_sec)
        # The revoked partitions' next assignment (possibly to another consumer) starts
        # fresh, so the cancellation floor — if any was set — must not carry over.
        self._committer.clear_cancellation_watermarks(typing.cast("typing.Iterable[TopicPartition]", revoked))
