# ruff: noqa: SLF001
import asyncio
import contextlib
import logging
import typing
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from aiokafka.errors import CommitFailedError, KafkaError
from faststream.kafka import TopicPartition

from faststream_concurrent_aiokafka.batch_committer import (
    CommitterIsDeadError,
    KafkaBatchCommitter,
    KafkaCommitTask,
    _insert_sorted,
)
from tests.mocks import MockAIOKafkaConsumer, MockAsyncioTask


@pytest.fixture
def mock_consumer() -> MockAIOKafkaConsumer:
    return MockAIOKafkaConsumer()


@pytest.fixture
def sample_task(mock_consumer: MockAIOKafkaConsumer) -> KafkaCommitTask:
    mock_task: typing.Final = MockAsyncioTask(result="success")
    return KafkaCommitTask(
        asyncio_task=mock_task,  # ty: ignore[invalid-argument-type]
        offset=100,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="test-topic", partition=0),
    )


@pytest_asyncio.fixture
async def committer() -> typing.AsyncIterator[KafkaBatchCommitter]:
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.1, commit_batch_size=3)
    yield committer
    if committer._commit_task and not committer._commit_task.done():
        committer._commit_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await committer._commit_task


# ---------- _check_is_commit_task_running ----------


def test_committer_raises_when_not_spawned(committer: KafkaBatchCommitter) -> None:
    with pytest.raises(CommitterIsDeadError, match="Committer main task is not running"):
        committer._check_is_commit_task_running()


async def test_committer_raises_when_done(committer: KafkaBatchCommitter) -> None:
    committer._commit_task = asyncio.create_task(asyncio.sleep(100))
    committer._commit_task.cancel()
    await asyncio.sleep(0)

    with pytest.raises(CommitterIsDeadError):
        committer._check_is_commit_task_running()


async def test_committer_passes_when_running(committer: KafkaBatchCommitter) -> None:
    committer._commit_task = asyncio.create_task(asyncio.sleep(100))
    committer._check_is_commit_task_running()


# ---------- spawn / send_task / is_healthy / close ----------


async def test_committer_spawn_creates_task(committer: KafkaBatchCommitter) -> None:
    committer.spawn()

    assert committer._commit_task is not None
    assert not committer._commit_task.done()

    committer.spawn()
    assert committer.is_healthy


async def test_committer_send_task_adds_to_queue(committer: KafkaBatchCommitter, sample_task: KafkaCommitTask) -> None:
    committer.spawn()

    await committer.send_task(sample_task)

    assert not committer._messages_queue.empty()
    queued_task: typing.Final = await committer._messages_queue.get()
    assert queued_task == sample_task


async def test_committer_send_task_raises_when_dead(
    committer: KafkaBatchCommitter, sample_task: KafkaCommitTask
) -> None:
    with pytest.raises(CommitterIsDeadError):
        await committer.send_task(sample_task)


async def test_committer_close_graceful_shutdown() -> None:
    """close() drives the streaming loop to drain pending and exit cleanly."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=10)

    async def quick_handler() -> str:
        return "ok"

    real_task: typing.Final = asyncio.create_task(quick_handler())
    commit_task: typing.Final = KafkaCommitTask(
        asyncio_task=real_task,
        offset=100,
        consumer=consumer,
        topic_partition=TopicPartition(topic="t", partition=0),
    )

    committer.spawn()
    await committer.send_task(commit_task)
    await committer.close()

    assert committer._commit_task
    assert committer._commit_task.done()
    consumer.commit.assert_called_once()


async def test_committer_close_timeout_cancels_task(committer: KafkaBatchCommitter) -> None:
    committer.spawn()
    committer._shutdown_timeout = 0.001

    with patch.object(committer, "_run_commit_process", new_callable=AsyncMock) as mock_run:
        mock_run.side_effect = asyncio.sleep(10)

        await committer.close()
        assert committer._commit_task
        assert committer._commit_task.done()


async def test_committer_close_handles_not_running(
    committer: KafkaBatchCommitter, caplog: pytest.LogCaptureFixture
) -> None:
    await committer.close()
    assert "Committer main task is not running" in caplog.text


async def test_committer_is_healthy(committer: KafkaBatchCommitter) -> None:
    assert not committer.is_healthy

    committer.spawn()
    assert committer.is_healthy

    committer._commit_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await committer._commit_task

    assert not committer.is_healthy


async def test_committer_uses_shutdown_timeout_kwarg() -> None:
    committer: typing.Final = KafkaBatchCommitter(shutdown_timeout_sec=0.05)
    assert committer._shutdown_timeout == 0.05


async def test_committer_close_logs_when_task_already_died(caplog: pytest.LogCaptureFixture) -> None:
    """If the committer task crashed before close() is called, the exception is logged."""
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.1, commit_batch_size=10)

    async def crashing() -> typing.Never:
        msg: typing.Final = "boom"
        raise RuntimeError(msg)

    committer._commit_task = asyncio.create_task(crashing())
    with contextlib.suppress(RuntimeError):
        await committer._commit_task

    await committer.close()
    assert "Committer task had already died before close()" in caplog.text


async def test_committer_close_but_timeout_error(caplog: pytest.LogCaptureFixture) -> None:
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.1, commit_batch_size=10000)
    committer._commit_task = asyncio.create_task(asyncio.sleep(30))
    committer._shutdown_timeout = 0.1
    await committer.close()
    await asyncio.sleep(0)
    assert "Committer main task shutdown timed out, forcing cancellation" in caplog.text
    assert not committer.is_healthy


async def test_committer_close_but_unexpected_error() -> None:
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.1, commit_batch_size=10)
    mock_task: typing.Final = MagicMock()
    mock_task.done.return_value = False
    mock_task.cancelled.return_value = False
    committer._commit_task = mock_task

    original_exception: typing.Final = RuntimeError("Original error")

    with patch("asyncio.wait_for", side_effect=original_exception), pytest.raises(RuntimeError) as exc_info:
        await committer.close()

    assert exc_info.value is original_exception


# ---------- _call_committer ----------


async def test_committer_returns_true_on_empty_offsets(committer: KafkaBatchCommitter) -> None:
    result: typing.Final = await committer._call_committer([], {})
    assert result is True


async def test_committer_commits_to_kafka(committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer) -> None:
    mock_task: typing.Final = MockAsyncioTask(result="success")
    sample_task: typing.Final = KafkaCommitTask(
        asyncio_task=mock_task,  # ty: ignore[invalid-argument-type]
        offset=100,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="test-topic", partition=0),
    )
    partitions_to_offsets: typing.Final = {sample_task.topic_partition: 101}
    result: typing.Final = await committer._call_committer([sample_task], partitions_to_offsets)

    assert result is True
    mock_consumer.commit.assert_called_once()
    call_args: typing.Final = mock_consumer.commit.call_args[0][0]
    assert call_args == partitions_to_offsets


async def test_committer_retries_on_kafka_error(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    """KafkaError re-queues the batch for retry on the next cycle."""
    mock_task: typing.Final = MockAsyncioTask(result="success")
    sample_task: typing.Final = KafkaCommitTask(
        asyncio_task=mock_task,  # ty: ignore[invalid-argument-type]
        offset=100,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="test-topic", partition=0),
    )
    mock_consumer.commit.side_effect = KafkaError("transient broker error")

    partitions_to_offsets: typing.Final = {sample_task.topic_partition: 101}

    result: typing.Final = await committer._call_committer([sample_task], partitions_to_offsets)

    assert result is False
    assert not committer._messages_queue.empty()
    requeued_task: typing.Final = await committer._messages_queue.get()
    assert requeued_task == sample_task


async def test_committer_ignores_commit_failed_error(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer, sample_task: KafkaCommitTask
) -> None:
    """CommitFailedError (rebalance in progress) is silently ignored — no re-queue."""
    mock_consumer.commit.side_effect = CommitFailedError()
    partitions_to_offsets: typing.Final = {sample_task.topic_partition: 101}

    result: typing.Final = await committer._call_committer([sample_task], partitions_to_offsets)

    assert result is False
    assert committer._messages_queue.empty()


# ---------- _map_offsets_per_partition ----------


def test_committer_map_offsets_skips_cancelled_tasks(mock_consumer: MockAIOKafkaConsumer) -> None:
    """Offset must not advance past a cancelled task — that message was never processed."""
    tp: typing.Final = TopicPartition(topic="t1", partition=0)
    ok_task: typing.Final = MockAsyncioTask(result="ok")
    cancelled_task: typing.Final = MockAsyncioTask(cancelled=True)
    later_ok_task: typing.Final = MockAsyncioTask(result="ok")

    tasks: typing.Final = [
        KafkaCommitTask(
            asyncio_task=ok_task,  # ty: ignore[invalid-argument-type]
            offset=10,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
        KafkaCommitTask(
            asyncio_task=cancelled_task,  # ty: ignore[invalid-argument-type]
            offset=11,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
        KafkaCommitTask(
            asyncio_task=later_ok_task,  # ty: ignore[invalid-argument-type]
            offset=12,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
    ]

    offsets: typing.Final = KafkaBatchCommitter._map_offsets_per_partition(id(mock_consumer), tasks, {})
    # Only offset 10 is safe to commit; 11 was cancelled, 12 is beyond it.
    assert offsets[tp] == 11  # max safe offset (10) + 1


def test_committer_map_offsets_skips_partition_when_all_cancelled(mock_consumer: MockAIOKafkaConsumer) -> None:
    """If all tasks on a partition are cancelled, that partition is omitted from the commit map."""
    tp: typing.Final = TopicPartition(topic="t1", partition=0)
    cancelled_task: typing.Final = MockAsyncioTask(cancelled=True)
    task: typing.Final = KafkaCommitTask(
        asyncio_task=cancelled_task,  # ty: ignore[invalid-argument-type]
        offset=5,
        consumer=mock_consumer,
        topic_partition=tp,
    )

    offsets: typing.Final = KafkaBatchCommitter._map_offsets_per_partition(id(mock_consumer), [task], {})
    assert tp not in offsets


# ---------- cancellation watermark ----------


def test_map_offsets_records_cancellation_watermark(mock_consumer: MockAIOKafkaConsumer) -> None:
    """A cancelled task records its offset as the partition's watermark."""
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    cancelled_offset: typing.Final = 11
    task: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(cancelled=True),  # ty: ignore[invalid-argument-type]
        offset=cancelled_offset,
        consumer=mock_consumer,
        topic_partition=tp,
    )

    watermarks: dict[tuple[int, TopicPartition], int] = {}
    KafkaBatchCommitter._map_offsets_per_partition(id(mock_consumer), [task], watermarks)

    assert watermarks == {(id(mock_consumer), tp): cancelled_offset}


def test_map_offsets_blocks_partition_when_watermark_present(mock_consumer: MockAIOKafkaConsumer) -> None:
    """A successful task whose offset would advance past the watermark is dropped."""
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    new_task: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
        offset=20,
        consumer=mock_consumer,
        topic_partition=tp,
    )

    watermarks: dict[tuple[int, TopicPartition], int] = {(id(mock_consumer), tp): 11}
    offsets: typing.Final = KafkaBatchCommitter._map_offsets_per_partition(id(mock_consumer), [new_task], watermarks)

    assert tp not in offsets
    assert watermarks == {(id(mock_consumer), tp): 11}  # unchanged


def test_map_offsets_keeps_earliest_watermark(mock_consumer: MockAIOKafkaConsumer) -> None:
    """When a partition sees a second cancellation at a higher offset, the earlier wins."""
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    later_cancelled: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(cancelled=True),  # ty: ignore[invalid-argument-type]
        offset=50,
        consumer=mock_consumer,
        topic_partition=tp,
    )

    watermarks: dict[tuple[int, TopicPartition], int] = {(id(mock_consumer), tp): 11}
    KafkaBatchCommitter._map_offsets_per_partition(id(mock_consumer), [later_cancelled], watermarks)

    assert watermarks == {(id(mock_consumer), tp): 11}


def test_map_offsets_commits_max_before_cancellation_records_watermark(
    mock_consumer: MockAIOKafkaConsumer,
) -> None:
    """The pre-cancellation max is still committed in the same batch the watermark is recorded."""
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    successful_offset: typing.Final = 9
    cancelled_offset: typing.Final = 10
    tasks: typing.Final = [
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
            offset=successful_offset,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(cancelled=True),  # ty: ignore[invalid-argument-type]
            offset=cancelled_offset,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
    ]

    watermarks: dict[tuple[int, TopicPartition], int] = {}
    offsets: typing.Final = KafkaBatchCommitter._map_offsets_per_partition(id(mock_consumer), tasks, watermarks)

    assert offsets == {tp: successful_offset + 1}
    assert watermarks == {(id(mock_consumer), tp): cancelled_offset}


def test_map_offsets_watermark_isolated_per_consumer() -> None:
    """A cancelled task on one consumer must not block commits on another consumer.

    Regression: previously the watermark dict was keyed by partition only, so a single
    handler shared across consumer groups subscribing to the same (topic, partition)
    would have one group's cancellation block the other group's commit.
    """
    consumer_a: typing.Final = MockAIOKafkaConsumer(group_id="group-a")
    consumer_b: typing.Final = MockAIOKafkaConsumer(group_id="group-b")
    tp: typing.Final = TopicPartition(topic="shared", partition=0)

    cancelled_on_a: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(cancelled=True),  # ty: ignore[invalid-argument-type]
        offset=5,
        consumer=consumer_a,
        topic_partition=tp,
    )
    success_on_b: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
        offset=20,
        consumer=consumer_b,
        topic_partition=tp,
    )

    watermarks: dict[tuple[int, TopicPartition], int] = {}
    KafkaBatchCommitter._map_offsets_per_partition(id(consumer_a), [cancelled_on_a], watermarks)
    assert watermarks == {(id(consumer_a), tp): 5}

    offsets: typing.Final = KafkaBatchCommitter._map_offsets_per_partition(id(consumer_b), [success_on_b], watermarks)
    assert offsets == {tp: 21}, "Consumer B's commit must not be blocked by consumer A's watermark"
    # Consumer A's watermark stays intact for consumer A.
    assert watermarks == {(id(consumer_a), tp): 5}


def test_clear_cancellation_watermarks_specific_partitions(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    tp_a: typing.Final = TopicPartition(topic="t", partition=0)
    tp_b: typing.Final = TopicPartition(topic="t", partition=1)
    consumer_id: typing.Final = id(mock_consumer)
    # Pre-seed owner so clear can resolve which consumer's watermark to drop.
    committer._partition_owner[tp_a] = consumer_id
    committer._partition_owner[tp_b] = consumer_id
    committer._cancellation_watermarks[(consumer_id, tp_a)] = 5
    committer._cancellation_watermarks[(consumer_id, tp_b)] = 7

    committer.clear_cancellation_watermarks([tp_a])

    assert committer._cancellation_watermarks == {(consumer_id, tp_b): 7}
    assert committer._partition_owner == {tp_b: consumer_id}


def test_clear_cancellation_watermarks_all_when_none(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    consumer_id: typing.Final = id(mock_consumer)
    tp_a: typing.Final = TopicPartition(topic="t", partition=0)
    tp_b: typing.Final = TopicPartition(topic="t", partition=1)
    committer._partition_owner[tp_a] = consumer_id
    committer._partition_owner[tp_b] = consumer_id
    committer._cancellation_watermarks[(consumer_id, tp_a)] = 5
    committer._cancellation_watermarks[(consumer_id, tp_b)] = 7

    committer.clear_cancellation_watermarks()

    assert committer._cancellation_watermarks == {}
    assert committer._partition_owner == {}


def test_committer_map_offsets_advances_to_max_per_partition(mock_consumer: MockAIOKafkaConsumer) -> None:
    """Offset advances to (max processed + 1) per partition."""
    first_offset: typing.Final = 100
    second_offset: typing.Final = 999
    partition: typing.Final = 1

    tasks: typing.Final[list[KafkaCommitTask]] = []
    # partition 0: offsets 100, 110 (two tasks)
    for off in (first_offset, first_offset + 10):
        tasks.append(
            KafkaCommitTask(
                asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
                offset=off,
                consumer=mock_consumer,
                topic_partition=TopicPartition(topic="t1", partition=0),
            )
        )
    # partition 1: offsets 100, 110, 999 (three tasks; max=999)
    for off in (first_offset, first_offset + 10, second_offset):
        tasks.append(
            KafkaCommitTask(
                asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
                offset=off,
                consumer=mock_consumer,
                topic_partition=TopicPartition(topic="t1", partition=partition),
            )
        )

    offsets: typing.Final = KafkaBatchCommitter._map_offsets_per_partition(id(mock_consumer), tasks, {})
    assert offsets[TopicPartition(topic="t1", partition=0)] == first_offset + 10 + 1
    assert offsets[TopicPartition(topic="t1", partition=partition)] == second_offset + 1


# ---------- _extract_ready_prefixes ----------


def test_extract_ready_prefixes_empty_pending() -> None:
    pending: dict[TopicPartition, list[KafkaCommitTask]] = {}
    ready: typing.Final = KafkaBatchCommitter._extract_ready_prefixes(pending)
    assert ready == {}
    assert pending == {}


def test_extract_ready_prefixes_all_done(mock_consumer: MockAIOKafkaConsumer) -> None:
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    tasks: typing.Final = [
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
            offset=offset,
            consumer=mock_consumer,
            topic_partition=tp,
        )
        for offset in (10, 11, 12)
    ]
    pending: dict[TopicPartition, list[KafkaCommitTask]] = {tp: list(tasks)}

    ready: typing.Final = KafkaBatchCommitter._extract_ready_prefixes(pending)

    assert ready == {tp: tasks}
    assert pending == {}  # partition emptied


def test_extract_ready_prefixes_blocks_on_first_pending(mock_consumer: MockAIOKafkaConsumer) -> None:
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    pending_task: typing.Final = MockAsyncioTask(done=False)
    tasks: typing.Final = [
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
            offset=10,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
        KafkaCommitTask(
            asyncio_task=pending_task,  # ty: ignore[invalid-argument-type]
            offset=11,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
            offset=12,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
    ]
    pending: dict[TopicPartition, list[KafkaCommitTask]] = {tp: list(tasks)}

    ready: typing.Final = KafkaBatchCommitter._extract_ready_prefixes(pending)

    assert ready == {tp: [tasks[0]]}  # only the prefix before offset 11
    assert pending[tp] == [tasks[1], tasks[2]]


def test_extract_ready_prefixes_cancelled_drops_partition(mock_consumer: MockAIOKafkaConsumer) -> None:
    """Cancelled task drops cancelled + everything after from pending into ready.

    task_done() balances messages_queue.join() that way. _map_offsets_per_partition
    separately stops the offset advance at the cancelled task so it gets redelivered.
    """
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    tasks: typing.Final = [
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
            offset=10,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(cancelled=True),  # ty: ignore[invalid-argument-type]
            offset=11,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
            offset=12,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
    ]
    pending: dict[TopicPartition, list[KafkaCommitTask]] = {tp: list(tasks)}

    ready: typing.Final = KafkaBatchCommitter._extract_ready_prefixes(pending)

    assert ready == {tp: tasks}  # all three included in ready
    assert pending == {}  # partition emptied


def test_insert_sorted_appends_in_order(mock_consumer: MockAIOKafkaConsumer) -> None:
    """In-order arrivals (the common case) just append — no bisect cost."""
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    pending: list[KafkaCommitTask] = []
    for offset in (10, 11, 12):
        _insert_sorted(
            pending,
            KafkaCommitTask(
                asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
                offset=offset,
                consumer=mock_consumer,
                topic_partition=tp,
            ),
        )
    assert [t.offset for t in pending] == [10, 11, 12]


def test_insert_sorted_bisects_out_of_order(mock_consumer: MockAIOKafkaConsumer) -> None:
    """A re-queued task with a lower offset slides into the right position."""
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    pending: list[KafkaCommitTask] = []
    for offset in (10, 11):
        _insert_sorted(
            pending,
            KafkaCommitTask(
                asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
                offset=offset,
                consumer=mock_consumer,
                topic_partition=tp,
            ),
        )
    _insert_sorted(
        pending,
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(done=True),  # ty: ignore[invalid-argument-type]
            offset=5,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
    )
    assert [t.offset for t in pending] == [5, 10, 11]


# ---------- _commit_partitions ----------


async def test_commit_partitions_calls_commit_per_partition_max(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    """All-done tasks → one commit call with max offset per partition (next-to-fetch = max + 1)."""
    expected_offset: typing.Final = 100

    tp: typing.Final = TopicPartition(topic="t1", partition=0)
    tasks: typing.Final = [
        KafkaCommitTask(
            asyncio_task=MockAsyncioTask(result="ok", done=True),  # ty: ignore[invalid-argument-type]
            offset=offset,
            consumer=mock_consumer,
            topic_partition=tp,
        )
        for offset in (expected_offset, expected_offset + 1)
    ]
    for t in tasks:
        await committer._messages_queue.put(t)
    [await committer._messages_queue.get() for _ in tasks]

    result: typing.Final = await committer._commit_partitions({tp: tasks})

    assert result is True
    mock_consumer.commit.assert_called_once_with({tp: expected_offset + 2})


async def test_commit_partitions_partial_failure_still_commits_offset(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    """One task raising still commits the partition's max offset.

    Per-task exception logging is owned by the handler's _finish_task callback,
    not the committer.
    """
    failing: typing.Final = MockAsyncioTask(done=True)
    succeeding: typing.Final = MockAsyncioTask(result="ok", done=True)
    tp: typing.Final = TopicPartition(topic="t1", partition=0)

    tasks: typing.Final = [
        KafkaCommitTask(
            asyncio_task=failing,  # ty: ignore[invalid-argument-type]
            offset=100,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
        KafkaCommitTask(
            asyncio_task=succeeding,  # ty: ignore[invalid-argument-type]
            offset=101,
            consumer=mock_consumer,
            topic_partition=tp,
        ),
    ]
    for t in tasks:
        await committer._messages_queue.put(t)
    [await committer._messages_queue.get() for _ in tasks]

    await committer._commit_partitions({tp: tasks})

    mock_consumer.commit.assert_called_once_with({tp: 102})


async def test_commit_partitions_handles_multiple_consumers(committer: KafkaBatchCommitter) -> None:
    """Each consumer's commit is called with only its own partitions — no cross-consumer commits."""
    consumer_a: typing.Final = MockAIOKafkaConsumer()
    consumer_b: typing.Final = MockAIOKafkaConsumer()

    tp_a: typing.Final = TopicPartition(topic="topic-a", partition=0)
    tp_b: typing.Final = TopicPartition(topic="topic-b", partition=0)

    task_a: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(result="ok"),  # ty: ignore[invalid-argument-type]
        offset=10,
        consumer=consumer_a,
        topic_partition=tp_a,
    )
    task_b: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(result="ok"),  # ty: ignore[invalid-argument-type]
        offset=20,
        consumer=consumer_b,
        topic_partition=tp_b,
    )
    for t in (task_a, task_b):
        await committer._messages_queue.put(t)
    [await committer._messages_queue.get() for _ in range(2)]

    await committer._commit_partitions({tp_a: [task_a], tp_b: [task_b]})

    consumer_a.commit.assert_called_once_with({tp_a: 11})
    consumer_b.commit.assert_called_once_with({tp_b: 21})


async def test_commit_partitions_returns_false_on_commit_failure(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    """_commit_partitions returns False when _call_committer fails."""
    task: typing.Final = MockAsyncioTask(result="ok")
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    commit_task: typing.Final = KafkaCommitTask(
        asyncio_task=task,  # ty: ignore[invalid-argument-type]
        offset=1,
        consumer=mock_consumer,
        topic_partition=tp,
    )
    await committer._messages_queue.put(commit_task)
    await committer._messages_queue.get()

    with patch.object(committer, "_call_committer", new_callable=AsyncMock, return_value=False):
        result: typing.Final = await committer._commit_partitions({tp: [commit_task]})

    assert result is False


async def test_commit_partitions_returns_false_if_any_consumer_group_fails(
    committer: KafkaBatchCommitter,
) -> None:
    """If any consumer's commit slice fails, the overall return is False."""
    consumer_a: typing.Final = MockAIOKafkaConsumer()
    consumer_b: typing.Final = MockAIOKafkaConsumer()
    consumer_a.commit.side_effect = KafkaError("transient")  # consumer_a fails

    tp_a: typing.Final = TopicPartition(topic="t", partition=0)
    tp_b: typing.Final = TopicPartition(topic="t", partition=1)
    task_a: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(result="ok"),  # ty: ignore[invalid-argument-type]
        offset=1,
        consumer=consumer_a,
        topic_partition=tp_a,
    )
    task_b: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(result="ok"),  # ty: ignore[invalid-argument-type]
        offset=2,
        consumer=consumer_b,
        topic_partition=tp_b,
    )
    # Both go through the queue once so task_done() balance is maintained.
    for t in (task_a, task_b):
        await committer._messages_queue.put(t)
    [await committer._messages_queue.get() for _ in range(2)]

    result: typing.Final = await committer._commit_partitions({tp_a: [task_a], tp_b: [task_b]})

    assert result is False
    consumer_b.commit.assert_called_once()  # b still committed independently


async def test_commit_partitions_cancelled_task_not_logged_as_error(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer, caplog: pytest.LogCaptureFixture
) -> None:
    """A cancelled task is not an error — it must not produce an error log line."""
    caplog.set_level(logging.ERROR)
    cancelled: typing.Final = MockAsyncioTask(cancelled=True)
    tp: typing.Final = TopicPartition(topic="t", partition=0)
    commit_task: typing.Final = KafkaCommitTask(
        asyncio_task=cancelled,  # ty: ignore[invalid-argument-type]
        offset=5,
        consumer=mock_consumer,
        topic_partition=tp,
    )
    await committer._messages_queue.put(commit_task)
    await committer._messages_queue.get()

    await committer._commit_partitions({tp: [commit_task]})

    assert "Task has finished with an exception" not in caplog.text


# ---------- streaming loop end-to-end ----------


async def _drive_until(predicate: typing.Callable[[], bool], deadline_sec: float = 1.0, poll: float = 0.01) -> None:
    """Yield to the event loop until ``predicate()`` returns True or ``deadline_sec`` elapses."""
    deadline: typing.Final = asyncio.get_event_loop().time() + deadline_sec
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(poll)
    msg: typing.Final = "predicate did not become true in time"  # pragma: no cover
    raise AssertionError(msg)  # pragma: no cover


async def test_streaming_commits_when_batch_size_reached() -> None:
    """When total pending crosses commit_batch_size, the loop commits the contiguous-done prefix."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=3)
    committer.spawn()

    async def quick() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    tasks: typing.Final = [asyncio.create_task(quick()) for _ in range(3)]
    for index, task in enumerate(tasks):
        await committer.send_task(
            KafkaCommitTask(
                asyncio_task=task,
                offset=10 + index,
                consumer=consumer,
                topic_partition=tp,
            )
        )

    await _drive_until(lambda: consumer.commit.called)
    consumer.commit.assert_called_once_with({tp: 13})  # offsets 10,11,12 → next-to-fetch 13
    await committer.close()


async def test_streaming_commits_on_timeout() -> None:
    """When timeout fires before batch_size, the loop commits whatever's ready."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.05, commit_batch_size=100)
    committer.spawn()

    async def quick() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    real_task: typing.Final = asyncio.create_task(quick())
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=real_task,
            offset=42,
            consumer=consumer,
            topic_partition=tp,
        )
    )

    await _drive_until(lambda: consumer.commit.called, deadline_sec=2.0)
    consumer.commit.assert_called_once_with({tp: 43})
    await committer.close()


async def test_streaming_commits_on_flush_event_without_stop() -> None:
    """commit_all() must flush without shutting down the committer (for rebalance use)."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=100)
    committer.spawn()

    async def quick() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    real_task: typing.Final = asyncio.create_task(quick())
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=real_task,
            offset=1,
            consumer=consumer,
            topic_partition=tp,
        )
    )

    await committer.commit_all()

    consumer.commit.assert_called_once_with({tp: 2})
    assert committer.is_healthy  # still running after flush

    await committer.close()


async def test_streaming_commits_on_close_flush() -> None:
    """close() sets _stop_requested → flush triggers commit + shutdown."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=100)
    committer.spawn()

    async def quick() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=asyncio.create_task(quick()),
            offset=99,
            consumer=consumer,
            topic_partition=tp,
        )
    )

    await committer.close()

    consumer.commit.assert_called_once_with({tp: 100})
    assert not committer.is_healthy


async def test_streaming_clears_flush_event_after_commit_all() -> None:
    """After commit_all(), the flush event must be cleared so subsequent triggers work."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=100)
    committer.spawn()

    async def quick() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=asyncio.create_task(quick()),
            offset=1,
            consumer=consumer,
            topic_partition=tp,
        )
    )
    await committer.commit_all()
    assert not committer._flush_batch_event.is_set()
    await committer.close()


async def test_streaming_handles_cancelled_error_in_loop(committer: KafkaBatchCommitter) -> None:
    """Re-raise CancelledError from the loop without leaking pending tasks."""
    committer.spawn()
    await asyncio.sleep(0.02)  # let loop reach asyncio.wait

    assert committer._commit_task is not None
    committer._commit_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await committer._commit_task

    assert not committer.is_healthy


async def test_streaming_cancel_with_active_timeout() -> None:
    """Cancelling the loop while a timeout_task is ticking cleans up via finally."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=100)
    committer.spawn()

    async def gated() -> None:
        await asyncio.sleep(30)

    real_task: typing.Final = asyncio.create_task(gated())
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=real_task,
            offset=10,
            consumer=consumer,
            topic_partition=TopicPartition(topic="t", partition=0),
        )
    )
    await asyncio.sleep(0.02)  # let the loop absorb and start timeout_task

    assert committer._commit_task is not None
    committer._commit_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await committer._commit_task

    assert not committer.is_healthy
    real_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await real_task


async def test_streaming_skips_cancelled_head_in_wait_set() -> None:
    """A cancelled task at the head of pending is not added to the wait set (avoids busy-loop)."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.05, commit_batch_size=100)
    committer.spawn()

    cancelled_task: typing.Final = asyncio.get_event_loop().create_future()
    cancelled_task.cancel()

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=cancelled_task,  # ty: ignore[invalid-argument-type]
            offset=10,
            consumer=consumer,
            topic_partition=tp,
        )
    )

    # Timeout fires → loop's _pending_head_tasks sees the cancelled head and skips it; the
    # subsequent commit drops the cancelled task without advancing offsets.
    await committer.close()
    # No commit was issued for this partition — _map_offsets_per_partition produced an empty
    # map (cancelled task), and _call_committer returns early when the offsets dict is empty.
    consumer.commit.assert_not_called()


async def test_streaming_idle_loop_blocks_without_busy_spin(committer: KafkaBatchCommitter) -> None:
    """An idle committer with no pending must not busy-loop on already-done heads."""
    committer.spawn()
    await asyncio.sleep(0.01)

    # No task work, no flush, no shutdown — the loop should be blocked on queue.get / flush.
    assert committer.is_healthy


# ---------- pipelining (the streaming win) ----------


async def test_committer_absorbs_queue_during_slow_handler() -> None:
    """A slow task on one partition must not stall queue absorption for other partitions.

    Today this is the across-batch fix: with the old populate-then-commit loop the queue
    would grow while one batch waits on a slow handler.
    """
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=2)
    committer.spawn()

    slow_event: typing.Final = asyncio.Event()

    async def slow_handler() -> None:
        await slow_event.wait()

    async def fast_handler() -> None:
        return None

    slow_partition: typing.Final = TopicPartition(topic="t", partition=0)
    fast_partition: typing.Final = TopicPartition(topic="t", partition=1)

    # 1) Send the slow task first — it goes into pending and stays not-done.
    slow_task: typing.Final = asyncio.create_task(slow_handler())
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=slow_task,
            offset=1000,
            consumer=consumer,
            topic_partition=slow_partition,
        )
    )

    # 2) Send several fast tasks for a different partition. The streaming committer must
    #    absorb these into pending despite the slow task being in flight, then commit the
    #    fast partition's prefix once batch_size is reached.
    fast_tasks: typing.Final[list[asyncio.Task[None]]] = [asyncio.create_task(fast_handler()) for _ in range(2)]
    for index, t in enumerate(fast_tasks):
        await committer.send_task(
            KafkaCommitTask(
                asyncio_task=t,
                offset=10 + index,
                consumer=consumer,
                topic_partition=fast_partition,
            )
        )

    # The fast partition committed without waiting on the slow one. Wait for the fast
    # partition to actually reach offset 12 — depending on scheduling, the loop may
    # issue a partial commit at 11 first, which is still correct (across-batch pipelining
    # streams whatever's done) but the test asserts the final outcome.
    await _drive_until(
        lambda: any(c.args[0] == {fast_partition: 12} for c in consumer.commit.call_args_list),
        deadline_sec=1.0,
    )
    consumer.commit.assert_any_call({fast_partition: 12})

    slow_event.set()
    await committer.close()
    # After close, the slow partition also commits.
    assert consumer.commit.call_args_list[-1].args[0] == {slow_partition: 1001}


async def test_committer_streaming_drains_on_close() -> None:
    """close() must commit every already-done task before exiting.

    In-flight tasks are dropped (asyncio.wait_for inside close() bounds the wait
    via _shutdown_timeout).
    """
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=100)
    committer.spawn()

    async def quick() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    done_tasks: typing.Final = [asyncio.create_task(quick()) for _ in range(5)]
    for index, t in enumerate(done_tasks):
        await committer.send_task(
            KafkaCommitTask(
                asyncio_task=t,
                offset=100 + index,
                consumer=consumer,
                topic_partition=tp,
            )
        )

    await committer.close()
    # All five commits collapsed into one call (max offset 104 → next-to-fetch 105).
    consumer.commit.assert_called_once_with({tp: 105})


async def test_committer_streaming_handles_requeue_offset_order() -> None:
    """Lazy offset sort tolerates re-queued tasks landing after higher-offset arrivals.

    Transient KafkaError re-queues a batch; meanwhile new same-partition tasks arrive
    with higher offsets. The final commit must reflect the correct max offset.
    """
    consumer: typing.Final = MockAIOKafkaConsumer()
    # First commit attempt: transient KafkaError → re-queue. Second attempt: succeeds.
    consumer.commit.side_effect = [KafkaError("transient"), None, None]

    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=2)
    committer.spawn()

    async def quick() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)

    # Send two tasks that trigger the (failing) first commit.
    early_tasks: typing.Final[list[asyncio.Task[None]]] = [asyncio.create_task(quick()) for _ in range(2)]
    for index, t in enumerate(early_tasks):
        await committer.send_task(
            KafkaCommitTask(
                asyncio_task=t,
                offset=100 + index,
                consumer=consumer,
                topic_partition=tp,
            )
        )

    # Wait until the failing commit attempt has occurred and the batch was re-queued.
    await _drive_until(lambda: consumer.commit.call_count >= 1, deadline_sec=1.0)

    # Now send a task with a higher offset BEFORE the re-queued tasks land back in pending.
    late_task: typing.Final = asyncio.create_task(quick())
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=late_task,
            offset=200,  # much higher than the re-queued 100/101
            consumer=consumer,
            topic_partition=tp,
        )
    )

    await committer.close()

    # The final commit must reflect the max processed offset (200 → next-to-fetch 201)
    # despite the requeued 100/101 arriving after offset 200 in queue order.
    final_call: typing.Final = consumer.commit.call_args_list[-1]
    assert final_call.args[0] == {tp: 201}
