import inspect
import typing
from unittest.mock import AsyncMock

import aiokafka
import pytest
from aiokafka.structs import TopicPartition
from faststream._internal.context import ContextRepo

from faststream_concurrent_aiokafka import consts
from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler
from faststream_concurrent_aiokafka.rebalance import ConsumerRebalanceListener
from tests.mocks import MockKafkaBatchCommitter


@pytest.fixture
def committer() -> MockKafkaBatchCommitter:
    return MockKafkaBatchCommitter()


@pytest.fixture
def listener(committer: MockKafkaBatchCommitter) -> ConsumerRebalanceListener:
    return ConsumerRebalanceListener(committer)  # ty: ignore[invalid-argument-type]


async def test_rebalance_on_partitions_revoked_calls_commit_all(
    listener: ConsumerRebalanceListener, committer: MockKafkaBatchCommitter
) -> None:
    await listener.on_partitions_revoked(set())
    committer.commit_all.assert_called_once()


async def test_rebalance_on_partitions_assigned_is_noop(
    listener: ConsumerRebalanceListener, committer: MockKafkaBatchCommitter
) -> None:
    await listener.on_partitions_assigned(set())
    committer.commit_all.assert_not_called()


async def test_rebalance_commit_all_is_awaited(committer: MockKafkaBatchCommitter) -> None:
    """commit_all must be awaited (not fire-and-forget) so the rebalance blocks until flush completes."""
    flush_done: typing.Final = []

    async def track_commit(*_args: object) -> None:
        flush_done.append(True)

    committer.commit_all = AsyncMock(side_effect=track_commit)
    listener: typing.Final = ConsumerRebalanceListener(committer)  # ty: ignore[invalid-argument-type]

    await listener.on_partitions_revoked(set())
    assert flush_done, "commit_all was not awaited before returning"


async def test_rebalance_on_partitions_revoked_clears_watermarks(
    listener: ConsumerRebalanceListener, committer: MockKafkaBatchCommitter
) -> None:
    """On revoke, the cancelled-offset watermarks for the revoked partitions must be cleared.

    The next assignment of those partitions starts fresh.
    """
    revoked: typing.Final = {TopicPartition(topic="t", partition=0), TopicPartition(topic="t", partition=1)}

    await listener.on_partitions_revoked(revoked)

    committer.clear_cancellation_watermarks.assert_called_once_with(revoked)


async def test_rebalance_forwards_flush_timeout(committer: MockKafkaBatchCommitter) -> None:
    """The listener forwards its configured flush timeout to commit_all."""
    listener: typing.Final = ConsumerRebalanceListener(committer, flush_timeout_sec=2.5)  # ty: ignore[invalid-argument-type]
    await listener.on_partitions_revoked(set())
    committer.commit_all.assert_called_once_with(2.5)


async def test_rebalance_clear_runs_after_commit_all(committer: MockKafkaBatchCommitter) -> None:
    """clear_cancellation_watermarks must run after commit_all.

    Committing relies on the watermark to know which partitions to skip, so clearing first
    would let an outgoing consumer commit past a cancelled boundary.
    """
    order: typing.Final[list[str]] = []

    async def track_commit_all(*_args: object) -> None:
        order.append("commit_all")

    def track_clear(_partitions: object) -> None:
        order.append("clear")

    committer.commit_all = AsyncMock(side_effect=track_commit_all)
    committer.clear_cancellation_watermarks = track_clear  # ty: ignore[invalid-assignment]
    listener: typing.Final = ConsumerRebalanceListener(committer)  # ty: ignore[invalid-argument-type]

    await listener.on_partitions_revoked(set())

    assert order == ["commit_all", "clear"]


def test_the_rebalance_flush_default_stays_under_aiokafkas_max_poll_interval() -> None:
    """INVARIANT: the default revoke-callback flush cannot outlast aiokafka's poll interval.

    `on_partitions_revoked` blocks the rebalance while `commit_all` waits, so a flush budget at or
    above `max.poll.interval.ms` lets a slow handler get the consumer evicted from the group mid-
    revoke — the failure the listener exists to prevent, arriving through the listener itself. Two
    changes break it and neither looks like it touches rebalancing: raising our own default to buy
    slow handlers more room, or aiokafka lowering its poll interval under us. The bound is read
    from aiokafka rather than hardcoded so the second one is caught on a dependency bump.
    """
    aiokafka_default_sec: typing.Final = (
        inspect.signature(aiokafka.AIOKafkaConsumer.__init__).parameters["max_poll_interval_ms"].default / 1000
    )
    assert aiokafka_default_sec > consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC


async def test_from_context_resolves_a_handler_registered_after_the_listener(
    committer: MockKafkaBatchCommitter,
) -> None:
    """Subscribers are declared before the lifespan creates the handler, so lookup happens on revoke."""
    context: typing.Final = ContextRepo()
    listener: typing.Final = ConsumerRebalanceListener.from_context(context, flush_timeout_sec=2.5)
    handler: typing.Final = KafkaConcurrentHandler(committer=committer)  # ty: ignore[invalid-argument-type]
    await handler.start()
    context.set_global(consts.PROCESSING_CONTEXT_KEY, handler)
    revoked: typing.Final = {TopicPartition(topic="t", partition=0)}

    await listener.on_partitions_revoked(revoked)

    committer.commit_all.assert_called_once_with(2.5)
    committer.clear_cancellation_watermarks.assert_called_once_with(revoked)


async def test_from_context_is_a_noop_without_a_handler() -> None:
    listener: typing.Final = ConsumerRebalanceListener.from_context(ContextRepo())

    await listener.on_partitions_revoked({TopicPartition(topic="t", partition=0)})


async def test_from_context_skips_a_stopped_handler(committer: MockKafkaBatchCommitter) -> None:
    context: typing.Final = ContextRepo()
    listener: typing.Final = ConsumerRebalanceListener.from_context(context)
    context.set_global(
        consts.PROCESSING_CONTEXT_KEY,
        KafkaConcurrentHandler(committer=committer),  # ty: ignore[invalid-argument-type]
    )

    await listener.on_partitions_revoked({TopicPartition(topic="t", partition=0)})

    committer.commit_all.assert_not_called()
    committer.clear_cancellation_watermarks.assert_not_called()
