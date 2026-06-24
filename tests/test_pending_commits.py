import pytest
from faststream.kafka import TopicPartition

from faststream_concurrent_aiokafka._pending_state import PendingCommits
from tests.mocks import MockAIOKafkaConsumer, make_commit_task  # reuse existing helpers


def _tp(partition: int = 0, topic: str = "t") -> TopicPartition:
    return TopicPartition(topic=topic, partition=partition)


@pytest.fixture
def mock_consumer() -> MockAIOKafkaConsumer:
    return MockAIOKafkaConsumer()


def test_len_counts_absorbed_tasks(mock_consumer: MockAIOKafkaConsumer) -> None:
    pending = PendingCommits()
    assert len(pending) == 0
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, done=False))
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, done=False))
    assert len(pending) == 2


def test_take_ready_empty_returns_empty_list() -> None:
    assert PendingCommits().take_ready() == []


def test_take_ready_commits_contiguous_done_prefix(mock_consumer: MockAIOKafkaConsumer) -> None:
    pending = PendingCommits()
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, done=True))
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, done=True))
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=2, done=False))

    ready = pending.take_ready()

    assert len(ready) == 1
    rc = ready[0]
    assert rc.consumer is mock_consumer
    assert rc.offsets == {_tp(): 2}  # max processed (1) + 1
    assert len(rc.tasks) == 2  # only the done prefix
    assert len(pending) == 1  # offset 2 still pending


def test_take_ready_stops_at_first_not_done(mock_consumer: MockAIOKafkaConsumer) -> None:
    pending = PendingCommits()
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, done=False))
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, done=True))
    assert pending.take_ready() == []  # head not done → nothing ready
    assert len(pending) == 2


def test_cancelled_task_is_hard_boundary(mock_consumer: MockAIOKafkaConsumer) -> None:
    pending = PendingCommits()
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, done=True))
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, cancelled=True))
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=2, done=True))

    ready = pending.take_ready()
    rc = ready[0]

    assert rc.offsets == {_tp(): 1}  # advance stops at the cancelled task
    assert len(rc.tasks) == 3  # cancelled + after dropped from pending into ready
    assert len(pending) == 0


def test_watermark_blocks_advance_until_cleared(mock_consumer: MockAIOKafkaConsumer) -> None:
    pending = PendingCommits()
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=0, cancelled=True))
    pending.take_ready()  # records the (consumer, tp) watermark at 0

    # A later done task on the same partition must not advance past the floor.
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=1, done=True))
    assert pending.take_ready()[0].offsets == {}  # withheld

    pending.clear_watermarks([_tp()])
    pending.absorb(make_commit_task(mock_consumer, _tp(), offset=2, done=True))
    assert pending.take_ready()[0].offsets == {_tp(): 3}  # resumes after clear


def test_clear_watermarks_all_when_none(mock_consumer: MockAIOKafkaConsumer) -> None:
    pending = PendingCommits()
    pending.absorb(make_commit_task(mock_consumer, _tp(0), offset=0, cancelled=True))
    pending.absorb(make_commit_task(mock_consumer, _tp(1), offset=0, cancelled=True))
    pending.take_ready()
    pending.clear_watermarks()  # None → clear all
    pending.absorb(make_commit_task(mock_consumer, _tp(0), offset=1, done=True))
    assert pending.take_ready()[0].offsets == {_tp(0): 2}


def test_two_consumers_same_partition_commit_independently() -> None:
    a, b = MockAIOKafkaConsumer(), MockAIOKafkaConsumer()
    pending = PendingCommits()
    pending.absorb(make_commit_task(a, _tp(), offset=0, done=True))
    pending.absorb(make_commit_task(b, _tp(), offset=1, done=True))

    ready = {id(rc.consumer): rc for rc in pending.take_ready()}

    assert ready[id(a)].offsets == {_tp(): 1}
    assert ready[id(b)].offsets == {_tp(): 2}
