# ruff: noqa: SLF001, ANN401
import asyncio
import logging
import typing
from unittest.mock import patch

import pytest
import pytest_asyncio
from faststream.kafka import KafkaBroker, TestKafkaBroker

from faststream_concurrent_aiokafka.middleware import (
    KafkaConcurrentProcessingMiddleware,
    initialize_concurrent_processing,
    stop_concurrent_processing,
)
from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler  # used via patch.object


@pytest_asyncio.fixture
async def setup_broker() -> KafkaBroker:
    broker: typing.Final = KafkaBroker("localhost:9092")
    broker.add_middleware(KafkaConcurrentProcessingMiddleware)
    return broker


async def test_middleware_simple_message_processing(setup_broker: KafkaBroker) -> None:
    processed_messages: typing.Final = []

    @setup_broker.subscriber("test-topic", group_id="test-group")
    async def handler(msg: typing.Any) -> None:
        processed_messages.append(msg)

    async with TestKafkaBroker(setup_broker) as test_broker:
        hdl: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
            concurrency_limit=5,
        )

        try:
            await test_broker.publish({"id": 1, "data": "test"}, topic="test-topic")
            await hdl.wait_for_subtasks()
        finally:
            await stop_concurrent_processing(test_broker.context)

    assert len(processed_messages) == 1
    assert processed_messages[0]["id"] == 1


async def test_middleware_multiple_messages_parallel(setup_broker: KafkaBroker) -> None:
    processed: typing.Final = []
    timestamps: typing.Final = []
    expected_size: typing.Final = 3

    @setup_broker.subscriber("parallel-topic", group_id="parallel-group")
    async def handler(msg: typing.Any) -> None:
        timestamps.append(("start", msg["id"], asyncio.get_event_loop().time()))
        await asyncio.sleep(0.05)
        timestamps.append(("end", msg["id"], asyncio.get_event_loop().time()))
        processed.append(msg)

    hdl: typing.Final = await initialize_concurrent_processing(
        context=setup_broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=3
    )

    async def test(inner_broker: KafkaBroker) -> None:
        for i in range(expected_size):
            await inner_broker.publish({"id": i}, topic="parallel-topic")
        await hdl.wait_for_subtasks()

    async with TestKafkaBroker(setup_broker) as test_broker:
        await test(test_broker)

    assert len(processed) == expected_size
    starts: typing.Final = [t for t in timestamps if t[0] == "start"]
    ends: typing.Final = [t for t in timestamps if t[0] == "end"]
    last_start: typing.Final = max(s[2] for s in starts)
    first_end: typing.Final = min(e[2] for e in ends)
    assert last_start < first_end, "Messages were not processed in parallel"
    await stop_concurrent_processing(setup_broker.context)


async def test_middleware_concurrency_limit_enforced(setup_broker: KafkaBroker) -> None:
    concurrent: typing.Final = [0]
    max_concurrent: typing.Final = [0]
    concurrent_size: typing.Final = 2

    @setup_broker.subscriber("limited-topic", group_id="limited-group")
    async def handler(msg: typing.Any) -> None:
        concurrent[0] += 1
        max_concurrent[0] = max(max_concurrent[0], concurrent[0])
        await asyncio.sleep(0.1)
        concurrent[0] -= 1
        assert msg

    hdl: typing.Final = await initialize_concurrent_processing(
        context=setup_broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=2
    )

    async def test(inner_broker: KafkaBroker) -> None:
        for i in range(5):
            await inner_broker.publish({"id": i}, topic="limited-topic")
        await hdl.wait_for_subtasks()

    async with TestKafkaBroker(setup_broker) as test_broker:
        await test(test_broker)
    assert max_concurrent[0] <= concurrent_size, f"Concurrency limit exceeded: {max_concurrent[0]}"


async def test_middleware_handler_context_instance_stable(setup_broker: KafkaBroker) -> None:
    """The handler returned by initialize_concurrent_processing is the same object stored in context."""
    processed: typing.Final = []

    @setup_broker.subscriber("stable-topic", group_id="stable-group")
    async def handler(msg: typing.Any) -> None:
        processed.append(msg)

    async with TestKafkaBroker(setup_broker) as test_broker:
        hdl: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
            concurrency_limit=5,
        )

        try:
            for i in range(3):
                await test_broker.publish({"id": i}, topic="stable-topic")
            await hdl.wait_for_subtasks()
        finally:
            await stop_concurrent_processing(test_broker.context)

    assert len(processed) == 3


async def test_middleware_message_filtering_by_group_header(setup_broker: KafkaBroker) -> None:
    processed: typing.Final = []
    first_id: typing.Final = 1
    second_id: typing.Final = 2
    third_id: typing.Final = 3
    expected_size: typing.Final = 2

    @setup_broker.subscriber("filtered-topic", group_id="target-group")
    async def handler(msg: typing.Any) -> None:
        processed.append(msg)

    async with TestKafkaBroker(setup_broker) as test_broker:
        hdl: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
            concurrency_limit=5,
        )

        try:
            await test_broker.publish(
                {"id": first_id},
                topic="filtered-topic",
                headers={"my_stuff": "some stuff"},
            )
            await test_broker.publish(
                {"id": second_id},
                topic="filtered-topic",
                headers={"topic_group": "target-group"},
            )
            await test_broker.publish({"id": third_id}, topic="filtered-topic")
            await hdl.wait_for_subtasks()
        finally:
            await stop_concurrent_processing(test_broker.context)

    assert len(processed) == expected_size
    assert processed[0]["id"] == first_id
    assert processed[1]["id"] == third_id


async def test_middleware_initialize_start_failure_raises(setup_broker: KafkaBroker) -> None:
    with patch.object(KafkaConcurrentHandler, "start", side_effect=Exception("Start failed")):
        async with TestKafkaBroker(setup_broker) as test_broker:
            with pytest.raises(Exception, match="Start failed"):
                await initialize_concurrent_processing(
                    context=test_broker.context,
                    commit_batch_size=10,
                    commit_batch_timeout_sec=5,
                )


async def test_middleware_initialize_skips_when_already_running(
    setup_broker: KafkaBroker, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)

    async with TestKafkaBroker(setup_broker) as test_broker:
        await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
        )

        await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
        )

        assert "Processing is already active" in caplog.text

        await stop_concurrent_processing(test_broker.context)


async def test_middleware_unhealthy_handler_raises(setup_broker: KafkaBroker, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR)

    @setup_broker.subscriber("unhealthy-topic", group_id="unhealthy-group")
    async def handler(msg: typing.Any) -> None:
        pass  # pragma: no cover

    async with TestKafkaBroker(setup_broker) as test_broker:
        handler_instance: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
        )

        handler_instance._is_running = False

        with pytest.raises(RuntimeError, match="Concurrent processing is not running"):
            await test_broker.publish({"id": 1}, topic="unhealthy-topic")

        await asyncio.sleep(0)
        await stop_concurrent_processing(test_broker.context)


async def test_middleware_no_kafka_message_with_batch_processing_raises(
    setup_broker: KafkaBroker, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    @setup_broker.subscriber("no-kafka-msg-topic", group_id="no-kafka-msg-group")
    async def handler(msg: typing.Any) -> None:
        pass  # pragma: no cover

    async with TestKafkaBroker(setup_broker) as test_broker:
        await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
        )

        original_get: typing.Final = test_broker.context.get

        def mock_get(key: str, default: typing.Any = None) -> typing.Any:
            if key == "message":
                return None
            return original_get(key, default)

        test_broker.context.get = mock_get  # ty: ignore[invalid-assignment]

        with pytest.raises(RuntimeError, match="No kafka message in the middleware"):
            await test_broker.publish({"id": 1}, topic="no-kafka-msg-topic")

        await asyncio.sleep(0)
        await stop_concurrent_processing(test_broker.context)


async def test_middleware_no_handler_in_context_raises(
    setup_broker: KafkaBroker, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    @setup_broker.subscriber("no-handler-topic", group_id="no-handler-group")
    async def handler(msg: typing.Any) -> None:
        pass  # pragma: no cover

    async with TestKafkaBroker(setup_broker) as test_broker:
        with pytest.raises(RuntimeError, match="No concurrent processing instance in the context"):
            await test_broker.publish({"id": 1}, topic="no-handler-topic")


async def test_middleware_batch_processing_has_committer(setup_broker: KafkaBroker) -> None:
    processed: typing.Final = []
    expected_size: typing.Final = 3

    @setup_broker.subscriber("batch-topic", group_id="batch-group")
    async def handler(msg: typing.Any) -> None:
        processed.append(msg)

    async with TestKafkaBroker(setup_broker) as test_broker:
        handler_instance: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
        )

        try:
            for i in range(expected_size):
                await test_broker.publish({"id": i}, topic="batch-topic")
            await handler_instance.wait_for_subtasks()
        finally:
            await stop_concurrent_processing(test_broker.context)

    assert handler_instance._committer is not None
    assert len(processed) == expected_size


async def test_middleware_stop_without_start_is_noop(
    setup_broker: KafkaBroker, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)

    async with TestKafkaBroker(setup_broker) as test_broker:
        await stop_concurrent_processing(test_broker.context)
        assert "Concurrent processing is not running" in caplog.text


async def test_middleware_handler_exception_logged_not_crashed(
    setup_broker: KafkaBroker, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    @setup_broker.subscriber("handler-error-topic", group_id="handler-error-group")
    async def handler(msg: typing.Any) -> None:
        msg = "Handler failed"
        raise ValueError(msg)

    async with TestKafkaBroker(setup_broker) as test_broker:
        hdl: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
        )

        try:
            await test_broker.publish({"id": 1}, topic="handler-error-topic")
            await hdl.wait_for_subtasks()
        finally:
            await stop_concurrent_processing(test_broker.context)

    assert "Task has failed" in caplog.text or "exception" in caplog.text.lower()


async def test_middleware_concurrency_limiter_release_on_error(setup_broker: KafkaBroker) -> None:
    failed: typing.Final = []
    expected_fails: typing.Final = 2

    @setup_broker.subscriber("limiter-error-topic", group_id="limiter-error-group")
    async def handler(msg: typing.Any) -> None:
        failed.append(msg)
        msg = "Failed"
        raise ValueError(msg)

    async with TestKafkaBroker(setup_broker) as test_broker:
        hdl: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
            concurrency_limit=1,
        )

        try:
            await test_broker.publish({"id": 1}, topic="limiter-error-topic")
            await hdl.wait_for_subtasks()

            await test_broker.publish({"id": 2}, topic="limiter-error-topic")
            await hdl.wait_for_subtasks()
        finally:
            await stop_concurrent_processing(test_broker.context)

    assert len(failed) == expected_fails


async def test_middleware_start_stop_reinitialize(setup_broker: KafkaBroker) -> None:
    """Handler can be stopped and re-initialized; the second instance is fresh and healthy."""
    async with TestKafkaBroker(setup_broker) as test_broker:
        first_handler: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context, concurrency_limit=5
        )
        assert first_handler.is_healthy

        await stop_concurrent_processing(test_broker.context)
        assert not first_handler.is_running

        second_handler: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context, concurrency_limit=3
        )
        assert second_handler.is_healthy
        assert second_handler is not first_handler

        await stop_concurrent_processing(test_broker.context)


async def test_middleware_general_exception_wrapped(
    setup_broker: KafkaBroker, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR)

    @setup_broker.subscriber("general-error-topic", group_id="general-error-group")
    async def handler(msg: typing.Any) -> None:
        pass  # pragma: no cover

    async with TestKafkaBroker(setup_broker) as test_broker:
        await initialize_concurrent_processing(
            context=test_broker.context,
            commit_batch_size=10,
            commit_batch_timeout_sec=5,
        )

        with patch.object(
            KafkaConcurrentHandler, "handle_task", side_effect=MemoryError("Out of memory in handle_task")
        ):
            try:
                with pytest.raises(RuntimeError, match=r"Kafka middleware. An error while sending task"):
                    await test_broker.publish({"id": 1}, topic="general-error-topic")
            finally:
                await stop_concurrent_processing(test_broker.context)
