import typing
from unittest.mock import AsyncMock

import pytest

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
