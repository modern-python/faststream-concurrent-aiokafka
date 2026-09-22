# ruff: noqa: SLF001
import asyncio
import contextlib
import typing
from unittest.mock import MagicMock

from faststream.kafka import KafkaBroker, TestKafkaBroker

from faststream_concurrent_aiokafka.healthcheck import is_kafka_handler_healthy
from faststream_concurrent_aiokafka.middleware import (
    initialize_concurrent_processing,
    stop_concurrent_processing,
)


async def test_healthy_when_handler_is_running() -> None:
    broker: typing.Final = KafkaBroker("localhost:9092")
    async with TestKafkaBroker(broker) as test_broker:
        await initialize_concurrent_processing(context=test_broker.context)
        try:
            assert is_kafka_handler_healthy(test_broker.context) is True
        finally:
            await stop_concurrent_processing(test_broker.context)


async def test_unhealthy_when_no_handler_in_context() -> None:
    broker: typing.Final = KafkaBroker("localhost:9092")
    async with TestKafkaBroker(broker) as test_broker:
        assert is_kafka_handler_healthy(test_broker.context) is False


async def test_unhealthy_when_handler_stopped() -> None:
    broker: typing.Final = KafkaBroker("localhost:9092")
    async with TestKafkaBroker(broker) as test_broker:
        await initialize_concurrent_processing(context=test_broker.context)
        await stop_concurrent_processing(test_broker.context)
        assert is_kafka_handler_healthy(test_broker.context) is False


async def test_unhealthy_when_is_healthy_returns_false() -> None:
    broker: typing.Final = KafkaBroker("localhost:9092")
    async with TestKafkaBroker(broker) as test_broker:
        mock_handler: typing.Final = MagicMock()
        mock_handler.is_healthy = False
        test_broker.context.set_global("concurrent_processing", mock_handler)
        assert is_kafka_handler_healthy(test_broker.context) is False


async def test_unhealthy_when_the_committer_died_under_a_running_handler() -> None:
    """A dead committer must fail the probe on its own, before a message meets the send_task guard.

    The handler is still running and still accepting dispatches, so `_is_running` says nothing
    here; only the committer's liveness does. This is the chain an operator's liveness probe
    depends on to restart a consumer that can no longer commit offsets.
    """
    broker: typing.Final = KafkaBroker("localhost:9092")
    async with TestKafkaBroker(broker) as test_broker:
        handler: typing.Final = await initialize_concurrent_processing(context=test_broker.context)
        try:
            assert is_kafka_handler_healthy(test_broker.context) is True

            commit_task: typing.Final = handler._committer._commit_task
            assert commit_task is not None
            commit_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await commit_task

            assert handler.is_running is True
            assert is_kafka_handler_healthy(test_broker.context) is False
        finally:
            await stop_concurrent_processing(test_broker.context)
