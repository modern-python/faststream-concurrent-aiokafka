# ruff: noqa: SLF001
import asyncio
import contextlib
import logging
import typing
from unittest.mock import MagicMock

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


async def test_concurrent_finish_task_discards_from_tracked_set(handler: KafkaConcurrentHandler) -> None:
    real_task: typing.Final = asyncio.create_task(asyncio.sleep(0))
    await real_task
    handler._tracked_tasks.add(real_task)

    handler._finish_task(real_task)

    assert real_task not in handler._tracked_tasks


async def test_concurrent_handle_task_dispatches(
    handler: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    async def coro() -> str:
        return "result"

    await handler.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]

    assert len(handler._tracked_tasks) == 1
    sent_commit_task: typing.Final = handler._committer.send_task.call_args[0][0]  # ty: ignore[unresolved-attribute]
    assert isinstance(sent_commit_task.asyncio_task, asyncio.Task)
    assert len(sent_commit_task.asyncio_task._callbacks) > 0


async def test_concurrent_acquires_limiter_when_limited(
    handler_with_limit: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    async def coro() -> str:
        return "result"

    await handler_with_limit.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]
    assert handler_with_limit._limiter
    assert handler_with_limit._limiter._value == 1


async def test_concurrent_sends_to_committer_when_enabled(
    handler: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    await handler.start()

    async def coro() -> str:
        return "result"

    await handler.handle_task(coro(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]
    handler._committer.send_task.assert_called_once()  # ty: ignore[unresolved-attribute]


async def test_concurrent_handles_committer_dead_error(
    handler: KafkaConcurrentHandler, sample_message: MockKafkaMessage, sample_record: MockConsumerRecord
) -> None:
    await handler.start()
    handler._committer.send_task.side_effect = CommitterIsDeadError("Dead")  # ty: ignore[unresolved-attribute]

    with pytest.raises(CommitterIsDeadError):
        await handler.handle_task(asyncio.sleep(0), sample_record, sample_message)  # ty: ignore[invalid-argument-type]

    assert not handler._is_running


async def test_concurrent_start_sets_running(handler: KafkaConcurrentHandler) -> None:
    await handler.start()
    assert handler.is_running
    assert handler.is_healthy


async def test_concurrent_start_creates_committer_when_enabled(handler: KafkaConcurrentHandler) -> None:
    await handler.start()
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


async def test_concurrent_stop_closes_committer(handler: KafkaConcurrentHandler) -> None:
    await handler.start()
    await handler.stop()
    handler._committer.close.assert_called_once()  # ty: ignore[unresolved-attribute]


async def test_concurrent_stop_when_not_running(
    handler: KafkaConcurrentHandler, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    await handler.stop()
    assert "Shutting down" not in caplog.text


async def test_concurrent_stop_cancels_in_flight_tasks(handler: KafkaConcurrentHandler) -> None:
    await handler.start()
    sample_record: typing.Final = MockConsumerRecord()
    sample_message: typing.Final = MockKafkaMessage()

    started: typing.Final = asyncio.Event()

    async def slow() -> None:
        started.set()
        await asyncio.sleep(60)

    await handler.handle_task(slow(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]
    await started.wait()

    assert len(handler._tracked_tasks) == 1
    in_flight: typing.Final = next(iter(handler._tracked_tasks))

    await handler.stop()

    # MockKafkaBatchCommitter.close() is an AsyncMock and returns instantly without
    # giving the event loop a chance to deliver the cancellation. Await the task so
    # the CancelledError propagates and observable state settles.
    with contextlib.suppress(asyncio.CancelledError):
        await in_flight
    assert in_flight.cancelled()
    assert handler._tracked_tasks == set()


async def test_concurrent_stop_returns_quickly_with_slow_handlers(handler: KafkaConcurrentHandler) -> None:
    """Cancelling in-flight tasks lets stop() return well under any per-task latency."""
    await handler.start()
    sample_record: typing.Final = MockConsumerRecord()
    sample_message: typing.Final = MockKafkaMessage()

    async def slow() -> None:
        await asyncio.sleep(60)

    for _ in range(5):
        await handler.handle_task(slow(), sample_record, sample_message)  # ty: ignore[invalid-argument-type]

    # Yield so scheduled slow() coroutines reach their sleep before stop() cancels them.
    await asyncio.sleep(0)

    loop: typing.Final = asyncio.get_running_loop()
    started: typing.Final = loop.time()
    await handler.stop()
    elapsed: typing.Final = loop.time() - started

    assert elapsed < 1.0, f"stop() took {elapsed:.3f}s with slow handlers"


async def test_concurrent_finish_task_does_not_crash_on_cancelled_task(
    handler_with_limit: KafkaConcurrentHandler,
) -> None:
    task: typing.Final = asyncio.create_task(asyncio.sleep(10))
    handler_with_limit._tracked_tasks.add(task)
    task.add_done_callback(handler_with_limit._finish_task)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert task not in handler_with_limit._tracked_tasks


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

    # Let tasks complete naturally before stop, then assert lifecycle is clean.
    if handler._tracked_tasks:
        await asyncio.gather(*list(handler._tracked_tasks), return_exceptions=True)

    await handler.stop()

    assert not handler.is_running
    assert len(processed) == 5


def test_concurrent_create_rebalance_listener(handler: KafkaConcurrentHandler) -> None:
    listener: typing.Final = handler.create_rebalance_listener()
    assert isinstance(listener, ConsumerRebalanceListener)
