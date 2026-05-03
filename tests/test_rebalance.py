import typing
from unittest.mock import AsyncMock

import pytest
from faststream.kafka import TopicPartition

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

    async def track_commit() -> None:
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


async def test_rebalance_clear_runs_after_commit_all(committer: MockKafkaBatchCommitter) -> None:
    """clear_cancellation_watermarks must run after commit_all.

    Committing relies on the watermark to know which partitions to skip, so clearing first
    would let an outgoing consumer commit past a cancelled boundary.
    """
    order: typing.Final[list[str]] = []

    async def track_commit_all() -> None:
        order.append("commit_all")

    def track_clear(_partitions: object) -> None:
        order.append("clear")

    committer.commit_all = AsyncMock(side_effect=track_commit_all)
    committer.clear_cancellation_watermarks = track_clear  # ty: ignore[invalid-assignment]
    listener: typing.Final = ConsumerRebalanceListener(committer)  # ty: ignore[invalid-argument-type]

    await listener.on_partitions_revoked(set())

    assert order == ["commit_all", "clear"]
