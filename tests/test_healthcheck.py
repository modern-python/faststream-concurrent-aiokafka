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
