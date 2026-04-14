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

from faststream_concurrent_aiokafka import batch_committer
from faststream_concurrent_aiokafka.batch_committer import CommitterIsDeadError, KafkaBatchCommitter, KafkaCommitTask
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


def test_committer_raises_when_not_spawned(committer: KafkaBatchCommitter) -> None:
    with pytest.raises(CommitterIsDeadError, match="Committer main task is not running"):
        committer._check_is_commit_task_running()


async def test_committer_raises_when_cancelled(committer: KafkaBatchCommitter) -> None:
    committer._commit_task = asyncio.create_task(asyncio.sleep(100))
    committer._commit_task.cancel()
    await asyncio.sleep(0.5)
    with pytest.raises(CommitterIsDeadError):
        committer._check_is_commit_task_running()


async def test_committer_raises_when_done(committer: KafkaBatchCommitter) -> None:
    committer._commit_task = asyncio.create_task(asyncio.sleep(100))
    committer._commit_task.cancel()
    await asyncio.sleep(0.5)

    with pytest.raises(CommitterIsDeadError):
        committer._check_is_commit_task_running()


async def test_committer_passes_when_running(committer: KafkaBatchCommitter) -> None:
    committer._commit_task = asyncio.create_task(asyncio.sleep(100))
    committer._check_is_commit_task_running()


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


async def test_committer_commit_all_flush(committer: KafkaBatchCommitter) -> None:
    committer.spawn()

    task: typing.Final = asyncio.create_task(asyncio.sleep(10))
    commit_task: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=task,
        offset=100,
        consumer=MockAIOKafkaConsumer(),
        topic_partition=TopicPartition(topic="t1", partition=0),
    )
    await committer.send_task(commit_task)

    with patch.object(committer, "_commit_tasks_batch", new_callable=AsyncMock) as mock_commit:
        mock_commit.return_value = True

        async def mock_commit_side_effect(batch: list[str]) -> bool:
            for _ in batch:
                committer._messages_queue.task_done()
            return True

        mock_commit.side_effect = mock_commit_side_effect
        await committer.commit_all()


async def test_committer_close_graceful_shutdown(committer: KafkaBatchCommitter, sample_task: KafkaCommitTask) -> None:
    committer.spawn()
    await committer.send_task(sample_task)

    with patch.object(committer, "_commit_tasks_batch", new_callable=AsyncMock) as mock_commit:

        async def side_effect(batch: list[str]) -> bool:
            for _ in batch:
                committer._messages_queue.task_done()
            return True

        mock_commit.side_effect = side_effect
        await committer.close()
        assert committer._commit_task
        assert committer._commit_task.done()


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


async def test_committer_returns_empty_on_empty_queue(committer: KafkaBatchCommitter) -> None:
    committer._commit_batch_timeout_sec = 0.01
    tasks, should_shutdown = await committer._populate_commit_batch()
    assert tasks == []
    assert should_shutdown is False


async def test_committer_collects_batch_size(committer: KafkaBatchCommitter, sample_task: KafkaCommitTask) -> None:
    committer._commit_batch_size = 2
    committer._commit_batch_timeout_sec = 10.0

    for _ in range(3):
        await committer._messages_queue.put(sample_task)

    tasks, should_shutdown = await committer._populate_commit_batch()
    assert len(tasks) == committer._commit_batch_size
    assert should_shutdown is False


async def test_committer_returns_on_flush_event(committer: KafkaBatchCommitter, sample_task: KafkaCommitTask) -> None:
    """commit_all() flushes without stopping the loop (should_shutdown stays False)."""
    committer._commit_batch_timeout_sec = 10.0

    for _ in range(6):
        await committer._messages_queue.put(sample_task)
    committer._flush_batch_event.set()

    tasks, should_shutdown = await committer._populate_commit_batch()
    assert len(tasks) > committer._commit_batch_size
    assert should_shutdown is False


async def test_committer_returns_shutdown_on_close_flush(
    committer: KafkaBatchCommitter, sample_task: KafkaCommitTask
) -> None:
    """close() sets _stop_requested so flush triggers shutdown."""
    committer._commit_batch_timeout_sec = 10.0
    committer._stop_requested = True

    for _ in range(2):
        await committer._messages_queue.put(sample_task)
    committer._flush_batch_event.set()

    tasks, should_shutdown = await committer._populate_commit_batch()
    assert len(tasks) == 2
    assert should_shutdown is True


async def test_committer_flush_empty_list_when_queue_empty(committer: KafkaBatchCommitter) -> None:
    result: typing.Final = committer._flush_tasks_queue()
    assert result == []


async def test_committer_handles_cancelled_error(committer: KafkaBatchCommitter, sample_task: KafkaCommitTask) -> None:
    await committer._messages_queue.put(sample_task)

    async def mock_wait(*_: list[str], **__: dict[str, str]) -> typing.Never:
        raise asyncio.CancelledError

    with patch("asyncio.wait", side_effect=mock_wait):
        tasks, should_shutdown = await committer._populate_commit_batch()

    assert should_shutdown is True
    assert len(tasks) == 1


async def test_committer_check_on_timeout_working_correctly(committer: KafkaBatchCommitter) -> None:
    committer._commit_batch_timeout_sec = 0.01
    _tasks, _ = await committer._populate_commit_batch()


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
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer, sample_task: KafkaCommitTask
) -> None:
    """KafkaError re-queues the batch for retry on the next cycle."""
    mock_task: typing.Final = MockAsyncioTask(result="success")
    sample_task = KafkaCommitTask(
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


async def test_committer_waits_for_all_tasks(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    task1: typing.Final = MockAsyncioTask(result="result1", done=False)
    task2: typing.Final = MockAsyncioTask(result="result2", done=False)
    expected_offset: typing.Final = 100

    commit_task1: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=task1,  # ty: ignore[invalid-argument-type]
        offset=100,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t1", partition=0),
    )
    commit_task2: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=task2,  # ty: ignore[invalid-argument-type]
        offset=101,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t1", partition=0),
    )

    task1._done = True
    task2._done = True

    await committer._messages_queue.put(commit_task1)
    await committer._messages_queue.put(commit_task2)

    batch: typing.Final = [await committer._messages_queue.get(), await committer._messages_queue.get()]

    with patch.object(committer, "_call_committer", new_callable=AsyncMock) as mock_commit:
        mock_commit.return_value = True

        result: typing.Final = await committer._commit_tasks_batch(batch)

        assert result is True
        mock_commit.assert_called_once()
        call_args: typing.Final = mock_commit.call_args[0][1]
        assert call_args[TopicPartition(topic="t1", partition=0)] == expected_offset + 2


async def test_committer_logs_task_exceptions(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer, caplog: pytest.LogCaptureFixture
) -> None:
    task: typing.Final = MockAsyncioTask(exception=ValueError("Task failed"), done=True)

    commit_task: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=task,  # ty: ignore[invalid-argument-type]
        offset=100,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t1", partition=0),
    )
    await committer._messages_queue.put(commit_task)

    with patch.object(committer, "_call_committer", new_callable=AsyncMock, return_value=True):
        await committer._commit_tasks_batch([await committer._messages_queue.get()])

    assert "Task has finished with an exception" in caplog.text


async def test_committer_groups_by_partition(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    tasks: typing.Final = []
    first_offset: typing.Final = 100
    second_offset: typing.Final = 999
    for partition in [0, 0, 1, 1]:
        task = MockAsyncioTask(done=True)
        commit_task = batch_committer.KafkaCommitTask(
            asyncio_task=task,  # ty: ignore[invalid-argument-type]
            offset=first_offset + partition * 10,
            consumer=mock_consumer,
            topic_partition=TopicPartition(topic="t1", partition=partition),
        )
        await committer._messages_queue.put(commit_task)
        tasks.append(commit_task)

    task = MockAsyncioTask(done=True)
    commit_task = batch_committer.KafkaCommitTask(
        asyncio_task=task,  # ty: ignore[invalid-argument-type]
        offset=second_offset,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t1", partition=0),
    )
    await committer._messages_queue.put(commit_task)
    while not committer._messages_queue.empty():
        await committer._messages_queue.get()
    tasks.append(commit_task)

    with patch.object(committer, "_call_committer", new_callable=AsyncMock) as mock_commit:
        mock_commit.return_value = True
        await committer._commit_tasks_batch(tasks)

        partitions: typing.Final = mock_commit.call_args[0][1]
        assert partitions[TopicPartition(topic="t1", partition=0)] == second_offset + 1
        assert partitions[TopicPartition(topic="t1", partition=1)] == first_offset + partition * 10 + 1


async def test_committer_runs_until_shutdown(committer: KafkaBatchCommitter, sample_task: KafkaCommitTask) -> None:
    await committer._messages_queue.put(sample_task)
    committer._commit_batch_timeout_sec = 0.01

    with patch.object(committer, "_populate_commit_batch") as mock_populate:
        mock_populate.side_effect = [
            ([sample_task], True),
        ]
        with patch.object(committer, "_commit_tasks_batch", new_callable=AsyncMock) as mock_commit:
            mock_commit.return_value = True

            await committer._run_commit_process()

            mock_populate.assert_called_once()
            mock_commit.assert_called_once_with([sample_task])


async def test_committer_skips_empty_batches(committer: KafkaBatchCommitter) -> None:
    committer._commit_batch_timeout_sec = 0.01

    with patch.object(committer, "_populate_commit_batch") as mock_populate:
        mock_populate.side_effect = [
            ([], False),
            ([], True),
        ]
        with patch.object(committer, "_commit_tasks_batch", new_callable=AsyncMock) as mock_commit:
            await committer._run_commit_process()
            mock_commit.assert_not_called()


async def test_committer_full_flow_single_task() -> None:
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.1, commit_batch_size=10)
    consumer: typing.Final = MockAIOKafkaConsumer()

    async def handler() -> str:
        await asyncio.sleep(0.01)
        return "processed"

    real_task: typing.Final = asyncio.create_task(handler())

    commit_task: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=real_task,
        offset=100,
        consumer=consumer,
        topic_partition=TopicPartition(topic="test", partition=0),
    )

    committer.spawn()
    await committer.send_task(commit_task)

    await asyncio.sleep(0.2)
    await committer.close()
    assert consumer.commit.called
    real_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await real_task


async def test_committer_multiple_topics_and_partitions(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    expected_amount_topic_partitions: typing.Final = 4
    tasks: typing.Final = []
    for topic in ["topic-a", "topic-b"]:
        for partition in [0, 1]:
            for offset in [100, 200]:
                task = MockAsyncioTask(done=True)
                commit_task = batch_committer.KafkaCommitTask(
                    asyncio_task=task,  # ty: ignore[invalid-argument-type]
                    offset=offset,
                    consumer=mock_consumer,
                    topic_partition=TopicPartition(topic=topic, partition=partition),
                )
                await committer._messages_queue.put(commit_task)
                tasks.append(commit_task)

    with patch.object(committer, "_call_committer", new_callable=AsyncMock) as mock_commit:
        mock_commit.return_value = True
        await committer._commit_tasks_batch(tasks)
        call_args: typing.Final = mock_commit.call_args[0][1]
        assert len(call_args) == expected_amount_topic_partitions


async def test_committer_task_with_none_result(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    task: typing.Final = MockAsyncioTask(result=None, done=True)
    commit_task: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=task,  # ty: ignore[invalid-argument-type]
        offset=100,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t", partition=0),
    )
    await committer._messages_queue.put(commit_task)

    with patch.object(committer, "_call_committer", new_callable=AsyncMock, return_value=True):
        result: typing.Final = await committer._commit_tasks_batch([commit_task])
        assert result is True


async def test_committer_very_large_batch_size(mock_consumer: MockAIOKafkaConsumer) -> None:
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.1, commit_batch_size=10000)

    task: typing.Final = MockAsyncioTask(done=True)
    commit_task: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=task,  # ty: ignore[invalid-argument-type]
        offset=1,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t", partition=0),
    )

    await committer._messages_queue.put(commit_task)

    tasks, _ = await committer._populate_commit_batch()
    assert len(tasks) == 1


async def test_committer_close_but_timeout_error(caplog: pytest.LogCaptureFixture) -> None:
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=0.1, commit_batch_size=10000)
    committer._commit_task = asyncio.create_task(asyncio.sleep(30))
    committer._shutdown_timeout = 0.1
    await committer.close()
    await asyncio.sleep(0.5)
    assert "Committer main task shutdown timed out, forcing cancellation" in caplog.text
    assert not committer.is_healthy


async def test_committer_partial_batch_failure_still_commits(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer, caplog: pytest.LogCaptureFixture
) -> None:
    """When one task raises, the batch still commits offsets and the error is logged."""
    caplog.set_level(logging.ERROR)

    failing_task: typing.Final = MockAsyncioTask(exception=ValueError("handler failed"), done=True)
    succeeding_task: typing.Final = MockAsyncioTask(result="ok", done=True)

    commit_task1: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=failing_task,  # ty: ignore[invalid-argument-type]
        offset=100,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t1", partition=0),
    )
    commit_task2: typing.Final = batch_committer.KafkaCommitTask(
        asyncio_task=succeeding_task,  # ty: ignore[invalid-argument-type]
        offset=101,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t1", partition=0),
    )

    # Must put items in queue before calling _commit_tasks_batch so task_done() is balanced
    await committer._messages_queue.put(commit_task1)
    await committer._messages_queue.put(commit_task2)
    batch: typing.Final = [
        await committer._messages_queue.get(),
        await committer._messages_queue.get(),
    ]

    with patch.object(committer, "_call_committer", new_callable=AsyncMock, return_value=True) as mock_commit:
        await committer._commit_tasks_batch(batch)

    mock_commit.assert_called_once()
    call_args: typing.Final = mock_commit.call_args[0][1]
    # Max offset is 101; Kafka commits next-to-fetch offset = max + 1
    assert call_args[TopicPartition(topic="t1", partition=0)] == 102
    assert "Task has finished with an exception" in caplog.text


async def test_committer_handles_multiple_consumers(committer: KafkaBatchCommitter) -> None:
    """Each consumer's commit is called with only its own partitions — no cross-consumer commits."""
    consumer_a: typing.Final = MockAIOKafkaConsumer()
    consumer_b: typing.Final = MockAIOKafkaConsumer()

    task_a: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(result="ok"),  # ty: ignore[invalid-argument-type]
        offset=10,
        consumer=consumer_a,
        topic_partition=TopicPartition(topic="topic-a", partition=0),
    )
    task_b: typing.Final = KafkaCommitTask(
        asyncio_task=MockAsyncioTask(result="ok"),  # ty: ignore[invalid-argument-type]
        offset=20,
        consumer=consumer_b,
        topic_partition=TopicPartition(topic="topic-b", partition=0),
    )

    for t in (task_a, task_b):
        await committer._messages_queue.put(t)
    batch: typing.Final = [await committer._messages_queue.get(), await committer._messages_queue.get()]

    await committer._commit_tasks_batch(batch)

    consumer_a.commit.assert_called_once_with({TopicPartition(topic="topic-a", partition=0): 11})
    consumer_b.commit.assert_called_once_with({TopicPartition(topic="topic-b", partition=0): 21})


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


async def test_committer_cancelled_task_stops_offset_advance(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    """Offset must not advance past a cancelled task — that message was never processed."""
    ok_task: typing.Final = MockAsyncioTask(result="ok")
    cancelled_task: typing.Final = MockAsyncioTask(cancelled=True)
    later_ok_task: typing.Final = MockAsyncioTask(result="ok")

    tp: typing.Final = TopicPartition(topic="t1", partition=0)
    commit_task_ok: typing.Final = KafkaCommitTask(
        asyncio_task=ok_task,  # ty: ignore[invalid-argument-type]
        offset=10,
        consumer=mock_consumer,
        topic_partition=tp,
    )
    commit_task_cancelled: typing.Final = KafkaCommitTask(
        asyncio_task=cancelled_task,  # ty: ignore[invalid-argument-type]
        offset=11,
        consumer=mock_consumer,
        topic_partition=tp,
    )
    commit_task_later: typing.Final = KafkaCommitTask(
        asyncio_task=later_ok_task,  # ty: ignore[invalid-argument-type]
        offset=12,
        consumer=mock_consumer,
        topic_partition=tp,
    )

    for t in (commit_task_ok, commit_task_cancelled, commit_task_later):
        await committer._messages_queue.put(t)
    batch: typing.Final = [await committer._messages_queue.get() for _ in range(3)]

    with patch.object(committer, "_call_committer", new_callable=AsyncMock, return_value=True) as mock_commit:
        await committer._commit_tasks_batch(batch)

    offsets: typing.Final = mock_commit.call_args[0][1]
    # Only offset 10 is safe to commit; 11 was cancelled, 12 is beyond it
    assert offsets[tp] == 11  # max safe offset (10) + 1


async def test_committer_all_cancelled_tasks_skips_commit(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    """If all tasks on a partition are cancelled, nothing is committed for that partition."""
    tp: typing.Final = TopicPartition(topic="t1", partition=0)
    cancelled_task: typing.Final = MockAsyncioTask(cancelled=True)
    commit_task: typing.Final = KafkaCommitTask(
        asyncio_task=cancelled_task,  # ty: ignore[invalid-argument-type]
        offset=5,
        consumer=mock_consumer,
        topic_partition=tp,
    )

    await committer._messages_queue.put(commit_task)
    batch: typing.Final = [await committer._messages_queue.get()]

    with patch.object(committer, "_call_committer", new_callable=AsyncMock, return_value=True) as mock_commit:
        await committer._commit_tasks_batch(batch)

    offsets: typing.Final = mock_commit.call_args[0][1]
    assert tp not in offsets


async def test_committer_cancelled_task_not_logged_as_error(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer, caplog: pytest.LogCaptureFixture
) -> None:
    """CancelledError from a task is expected during shutdown and must not be logged as an error."""
    caplog.set_level(logging.ERROR)
    cancelled_task: typing.Final = MockAsyncioTask(cancelled=True)
    commit_task: typing.Final = KafkaCommitTask(
        asyncio_task=cancelled_task,  # ty: ignore[invalid-argument-type]
        offset=5,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t1", partition=0),
    )

    await committer._messages_queue.put(commit_task)
    batch: typing.Final = [await committer._messages_queue.get()]

    with patch.object(committer, "_call_committer", new_callable=AsyncMock, return_value=True):
        await committer._commit_tasks_batch(batch)

    assert "Task has finished with an exception" not in caplog.text


async def test_committer_commit_tasks_batch_returns_false_on_commit_failure(
    committer: KafkaBatchCommitter, mock_consumer: MockAIOKafkaConsumer
) -> None:
    """_commit_tasks_batch returns False when _call_committer fails."""
    task: typing.Final = MockAsyncioTask(result="ok")
    commit_task: typing.Final = KafkaCommitTask(
        asyncio_task=task,  # ty: ignore[invalid-argument-type]
        offset=1,
        consumer=mock_consumer,
        topic_partition=TopicPartition(topic="t", partition=0),
    )
    await committer._messages_queue.put(commit_task)
    batch: typing.Final = [await committer._messages_queue.get()]

    with patch.object(committer, "_call_committer", new_callable=AsyncMock, return_value=False):
        result: typing.Final = await committer._commit_tasks_batch(batch)

    assert result is False


async def test_committer_commit_all_does_not_stop_loop(committer: KafkaBatchCommitter) -> None:
    """commit_all() must flush without shutting down the committer (for rebalance use)."""
    committer.spawn()

    async def noop() -> None:
        pass

    real_task: typing.Final = asyncio.create_task(noop())
    commit_task: typing.Final = KafkaCommitTask(
        asyncio_task=real_task,
        offset=1,
        consumer=MockAIOKafkaConsumer(),
        topic_partition=TopicPartition(topic="t", partition=0),
    )

    with patch.object(committer, "_commit_tasks_batch", new_callable=AsyncMock) as mock_commit:

        async def side_effect(batch: list[KafkaCommitTask]) -> bool:
            for _ in batch:
                committer._messages_queue.task_done()
            return True

        mock_commit.side_effect = side_effect
        await committer.send_task(commit_task)
        await committer.commit_all()

    assert committer.is_healthy  # still running after flush
