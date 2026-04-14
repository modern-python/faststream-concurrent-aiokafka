# ruff: noqa: SLF001
import asyncio
import contextlib
import logging
import signal
import typing
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from faststream_concurrent_aiokafka.batch_committer import CommitterIsDeadError
from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler
from faststream_concurrent_aiokafka.rebalance import ConsumerRebalanceListener
from tests.mocks import MockConsumerRecord, MockKafkaBatchCommitter, MockKafkaMessage


@pytest_asyncio.fixture
async def handler() -> typing.AsyncIterator[KafkaConcurrentHandler]:
    handler: typing.Final = KafkaConcurrentHandler(committer=MockKafkaBatchCommitter())  # ty: ignore[invalid-argument-type]
    yield handler
    if handler._is_running:
        await handler.stop()


@pytest_asyncio.fixture
async def handler_with_committer() -> typing.AsyncIterator[KafkaConcurrentHandler]:
    h: typing.Final = KafkaConcurrentHandler(committer=MockKafkaBatchCommitter())  # ty: ignore[invalid-argument-type]
    yield h
    if h._is_running:
        await h.stop()


@pytest_asyncio.fixture
async def handler_with_limit() -> typing.AsyncIterator[KafkaConcurrentHandler]:
    h: typing.Final = KafkaConcurrentHandler(committer=MockKafkaBatchCommitter(), concurrency_limit=2)  # ty: ignore[invalid-argument-type]
    yield h


@pytest.fixture
def sample_message() -> MockKafkaMessage:
    return MockKafkaMessage()


@pytest.fixture
def sample_record() -> MockConsumerRecord:
    return MockConsumerRecord()


def test_concurrent_init_zero_concurrency_limit_raises() -> None:
    with pytest.raises(ValueError, match="concurrency_limit must be >= 1"):
        KafkaConcurrentHandler(committer=MockKafkaBatchCommitter(), concurrency_limit=0)  # ty: ignore[invalid-argument-type]


def test_concurrent_init_negative_concurrency_limit_raises() -> None:
    with pytest.raises(ValueError, match="concurrency_limit must be >= 1"):
        KafkaConcurrentHandler(committer=MockKafkaBatchCommitter(), concurrency_limit=-1)  # ty: ignore[invalid-argument-type]


async def test_concurrent_releases_limiter_on_completion(handler_with_limit: KafkaConcurrentHandler) -> None:
    expected_value: typing.Final = 2

    assert handler_with_limit._limiter
    await handler_with_limit._limiter.acquire()
    assert handler_with_limit._limiter._value == 1
    mock_task: typing.Final = MagicMock()
    mock_task.cancelled.return_value = False
    mock_task.exception.return_value = None
    handler_with_limit._finish_task(mock_task)
    assert handler_with_limit._limiter._value == expected_value


async def test_concurrent_failed_task_exception(
    handler_with_limit: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    mock_task: typing.Final = MagicMock()
    mock_task.cancelled.return_value = False
    mock_task.exception.return_value = ValueError("Task failed")
    handler_with_limit._finish_task(mock_task)
    assert "Task has failed with the exception" in caplog.text


async def test_concurrent_removes_task_from_set(handler: KafkaConcurrentHandler) -> None:
    mock_task: typing.Final = MagicMock()
    mock_task.cancelled.return_value = False
    mock_task.exception.return_value = None
    handler._current_tasks.add(mock_task)
    handler._finish_task(mock_task)
    assert mock_task not in handler._current_tasks


async def test_concurrent_creates_task(
    handler: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    async def coro() -> str:
        return "result"

    await handler.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]
    assert len(handler._current_tasks) == 1


async def test_concurrent_task_added_to_set(
    handler: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    async def coro() -> str:
        return "result"

    await handler.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]

    task: typing.Final = next(iter(handler._current_tasks))
    assert isinstance(task, asyncio.Task)


async def test_concurrent_done_callback_added(
    handler: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    async def coro() -> str:
        return "result"

    await handler.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]

    task: typing.Final = next(iter(handler._current_tasks))
    assert len(task._callbacks) > 0


async def test_concurrent_acquires_limiter_when_limited(
    handler_with_limit: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    async def coro() -> str:
        return "result"

    await handler_with_limit.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]
    assert handler_with_limit._limiter
    assert handler_with_limit._limiter._value == 1


async def test_concurrent_sends_to_committer_when_enabled(
    handler_with_committer: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    await handler_with_committer.start()

    async def coro() -> str:
        return "result"

    await handler_with_committer.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]
    assert handler_with_committer._committer
    handler_with_committer._committer.send_task.assert_called_once()  # ty: ignore[unresolved-attribute]


async def test_concurrent_handles_committer_dead_error(
    handler_with_committer: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    handler: typing.Final = handler_with_committer
    await handler.start()
    assert handler._committer
    handler._committer.send_task.side_effect = CommitterIsDeadError("Dead")  # ty: ignore[unresolved-attribute]

    async def coro() -> str:
        return "result"

    with pytest.raises(CommitterIsDeadError):
        await handler.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]

    assert not handler._is_running


async def test_concurrent_signal_handler_triggers_stop(handler: KafkaConcurrentHandler) -> None:
    await handler.start()

    with patch.object(handler, "stop", new_callable=AsyncMock) as mock_stop:
        handler._signal_handler(signal.SIGTERM)
        await asyncio.sleep(0)
        mock_stop.assert_called_once()


async def test_concurrent_signal_handler_logs_signal(
    handler: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    handler._signal_handler(signal.SIGINT)
    assert "Received signal" in caplog.text
    assert "SIGINT" in caplog.text


async def test_concurrent_start_sets_running(handler: KafkaConcurrentHandler) -> None:
    await handler.start()
    assert handler.is_running
    assert handler.is_healthy


async def test_concurrent_start_creates_committer_when_enabled(handler_with_committer: KafkaConcurrentHandler) -> None:
    handler: typing.Final = handler_with_committer
    await handler.start()
    assert handler._committer
    handler._committer.spawn.assert_called_once()  # ty: ignore[unresolved-attribute]
    assert handler.is_healthy


async def test_concurrent_start_skips_when_already_running(
    handler: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    await handler.start()
    await handler.start()
    assert caplog.text.count("Start middleware handler") == 1


async def test_concurrent_stop_base(handler: KafkaConcurrentHandler) -> None:
    await handler.start()
    await handler.stop()

    assert not handler.is_running


async def test_concurrent_stop_closes_committer(handler_with_committer: KafkaConcurrentHandler) -> None:
    handler: typing.Final = handler_with_committer
    await handler.start()
    await handler.stop()
    assert handler._committer
    handler._committer.close.assert_called_once()  # ty: ignore[unresolved-attribute]


async def test_concurrent_stop_waits_for_subtasks(handler: KafkaConcurrentHandler) -> None:
    await handler.start()
    with patch.object(handler, "wait_for_subtasks", new_callable=AsyncMock) as mock_wait:
        await handler.stop()
        mock_wait.assert_called_once()


async def test_concurrent_stop_handles_handler_removal_error(
    handler: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    await handler.start()
    with patch("asyncio.get_running_loop", side_effect=Exception("Loop error")):
        await handler.stop()

    assert "Exception raised" in caplog.text


async def test_concurrent_stop_when_not_running(
    handler: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    await handler.stop()
    assert "Shutting down" not in caplog.text


async def test_concurrent_waits_for_all_subtasks(handler: KafkaConcurrentHandler) -> None:
    results: typing.Final = []
    expected_tasks_len: typing.Final = 2

    async def task1() -> str:
        await asyncio.sleep(0.01)
        results.append(1)
        return "task1"

    async def task2() -> str:
        await asyncio.sleep(0.02)
        results.append(2)
        return "task2"

    handler._current_tasks.add(asyncio.create_task(task1()))
    handler._current_tasks.add(asyncio.create_task(task2()))
    await handler.wait_for_subtasks()
    assert len(results) == expected_tasks_len


async def test_concurrent_handles_task_exceptions(
    handler: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    async def failing_task() -> typing.Never:
        msg: typing.Final = "Task failed"
        raise ValueError(msg)

    handler._current_tasks.add(asyncio.create_task(failing_task()))
    await handler.wait_for_subtasks()
    assert handler._current_tasks.pop().done()


async def test_concurrent_logs_timeout(handler: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR)

    async def slow_task() -> None:
        await asyncio.sleep(100)

    with patch(
        "faststream_concurrent_aiokafka.processing.GRACEFUL_TIMEOUT_SEC",
        1,
    ):
        handler._current_tasks.add(asyncio.create_task(slow_task()))
        await handler.wait_for_subtasks()
        assert "haven't finished in graceful time" in caplog.text


async def test_concurrent_finish_task_does_not_crash_on_cancelled_task(
    handler_with_limit: KafkaConcurrentHandler,
) -> None:
    task: typing.Final = asyncio.create_task(asyncio.sleep(10))
    handler_with_limit._current_tasks.add(task)
    task.add_done_callback(handler_with_limit._finish_task)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert task not in handler_with_limit._current_tasks


async def test_concurrent_cancels_all_tasks(handler: KafkaConcurrentHandler) -> None:
    task1: typing.Final = asyncio.create_task(asyncio.sleep(10))
    task2: typing.Final = asyncio.create_task(asyncio.sleep(10))
    handler._current_tasks.add(task1)
    handler._current_tasks.add(task2)

    await handler.force_cancel_all()

    assert task1.cancelled()
    assert task2.cancelled()


async def test_concurrent_cancels_all_tasks_force(
    handler: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)

    await handler.start()

    task: typing.Final = asyncio.create_task(asyncio.sleep(10))
    handler._current_tasks.add(task)
    await handler.force_cancel_all()

    assert not handler._is_running
    assert "Force cancelling all tasks" in caplog.text
    assert len(handler._current_tasks) == 0


async def test_concurrent_full_lifecycle() -> None:
    handler: typing.Final = KafkaConcurrentHandler(committer=MockKafkaBatchCommitter(), concurrency_limit=2)  # ty: ignore[invalid-argument-type]

    await handler.start()
    assert handler.is_healthy

    processed: typing.Final = []

    async def process_msg(msg_id: int) -> None:
        await asyncio.sleep(0.01)
        processed.append(msg_id)

    msg: typing.Final = MockKafkaMessage()
    record: typing.Final = MockConsumerRecord()

    for i in range(5):
        await handler.handle_task(process_msg(i), record, msg)  # ty: ignore[invalid-argument-type]

    await handler.wait_for_subtasks()
    await handler.stop()

    assert not handler.is_running
    assert len(processed) > 0


async def test_concurrent_message_processing() -> None:
    target_value: typing.Final = 5
    handler: typing.Final = KafkaConcurrentHandler(committer=MockKafkaBatchCommitter(), concurrency_limit=target_value)  # ty: ignore[invalid-argument-type]
    await handler.start()

    start_times: typing.Final = []
    end_times: typing.Final = []

    async def tracked_task(idx: int) -> None:
        start_times.append((idx, asyncio.get_event_loop().time()))
        await asyncio.sleep(0.05)
        end_times.append((idx, asyncio.get_event_loop().time()))

    msg: typing.Final = MockKafkaMessage()
    record: typing.Final = MockConsumerRecord()

    for i in range(target_value):
        await handler.handle_task(tracked_task(i), record, msg)  # ty: ignore[invalid-argument-type]

    await handler.wait_for_subtasks()
    await handler.stop()

    if len(start_times) == target_value and len(end_times) == target_value:
        max_start: typing.Final = max(t for _, t in start_times)
        min_end: typing.Final = min(t for _, t in end_times)
        assert max_start < min_end


async def test_concurrent_signal_handling_integration() -> None:
    handler: typing.Final = KafkaConcurrentHandler(committer=MockKafkaBatchCommitter())  # ty: ignore[invalid-argument-type]
    await handler.start()

    handler._signal_handler(signal.SIGTERM)
    assert handler._stop_task is not None
    await handler._stop_task
    assert not handler.is_running


def test_concurrent_create_rebalance_listener(handler: KafkaConcurrentHandler) -> None:
    listener: typing.Final = handler.create_rebalance_listener()
    assert isinstance(listener, ConsumerRebalanceListener)
