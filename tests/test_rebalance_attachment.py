# ruff: noqa: SLF001
import inspect
import logging
import typing
from unittest.mock import AsyncMock, Mock

import pytest
from aiokafka import ConsumerRebalanceListener as BaseConsumerRebalanceListener
from aiokafka.structs import TopicPartition
from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.kafka.subscriber.usecase import ConcurrentBetweenPartitionsSubscriber, LogicSubscriber
from faststream.middlewares import AckPolicy

from faststream_concurrent_aiokafka import (
    ConsumerRebalanceListener,
    initialize_concurrent_processing,
    stop_concurrent_processing,
)


async def _handle(_msg: str) -> None: ...


def _subscribe(registrator: KafkaBroker | KafkaRouter, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:  # noqa: ANN401
    subscriber: typing.Final = registrator.subscriber(*args, **kwargs)
    subscriber(_handle)
    return subscriber


def _broker_in_app() -> KafkaBroker:
    broker: typing.Final = KafkaBroker()
    FastStream(broker)
    return broker


async def _initialize(broker: KafkaBroker) -> None:
    await initialize_concurrent_processing(context=broker.context, rebalance_flush_timeout_sec=2.5)


async def test_listener_is_attached_to_manual_subscribers_on_the_broker_and_its_routers() -> None:
    broker: typing.Final = _broker_in_app()
    on_broker: typing.Final = _subscribe(broker, "a", group_id="g", ack_policy=AckPolicy.MANUAL)
    router: typing.Final = KafkaRouter()
    on_router: typing.Final = _subscribe(router, "b", group_id="g", ack_policy=AckPolicy.MANUAL)
    broker.include_router(router)

    await _initialize(broker)
    try:
        for subscriber in (on_broker, on_router):
            listener = subscriber._listener
            assert isinstance(listener, ConsumerRebalanceListener)
            assert listener._flush_timeout_sec == 2.5
    finally:
        await stop_concurrent_processing(broker.context)


async def test_attached_listener_flushes_the_running_handler() -> None:
    broker: typing.Final = _broker_in_app()
    subscriber: typing.Final = _subscribe(broker, "a", group_id="g", ack_policy=AckPolicy.MANUAL)
    handler: typing.Final = await initialize_concurrent_processing(context=broker.context)
    try:
        handler.committer.commit_all = AsyncMock()
        revoked: typing.Final = {TopicPartition(topic="a", partition=0)}

        await subscriber._listener.on_partitions_revoked(revoked)

        handler.committer.commit_all.assert_awaited_once()
    finally:
        await stop_concurrent_processing(broker.context)


@pytest.mark.parametrize(
    "subscriber_kwargs",
    [
        pytest.param({"ack_policy": AckPolicy.ACK_FIRST}, id="not-manual"),
        pytest.param({"ack_policy": AckPolicy.MANUAL, "batch": True}, id="batch"),
    ],
)
async def test_listener_is_not_attached_to_subscribers_this_library_does_not_process(
    subscriber_kwargs: dict[str, typing.Any],
) -> None:
    broker: typing.Final = _broker_in_app()
    subscriber: typing.Final = _subscribe(broker, "a", group_id="g", **subscriber_kwargs)

    await _initialize(broker)
    try:
        assert subscriber._listener is None
    finally:
        await stop_concurrent_processing(broker.context)


async def test_listener_is_not_attached_to_manually_assigned_partitions() -> None:
    """Partitions assigned with `partitions=` never rebalance, so there is nothing to flush."""
    broker: typing.Final = _broker_in_app()
    subscriber: typing.Final = _subscribe(
        broker, partitions=[TopicPartition(topic="a", partition=0)], ack_policy=AckPolicy.MANUAL
    )

    await _initialize(broker)
    try:
        assert subscriber._listener is None
    finally:
        await stop_concurrent_processing(broker.context)


@pytest.mark.parametrize("is_async", [True, False])
async def test_a_user_listener_is_kept_and_still_called(*, is_async: bool) -> None:
    user_listener: typing.Final = Mock(spec=BaseConsumerRebalanceListener)
    if is_async:
        user_listener.on_partitions_revoked = AsyncMock()
        user_listener.on_partitions_assigned = AsyncMock()
    broker: typing.Final = _broker_in_app()
    subscriber: typing.Final = _subscribe(
        broker, "a", group_id="g", ack_policy=AckPolicy.MANUAL, listener=user_listener
    )
    partitions: typing.Final = {TopicPartition(topic="a", partition=0)}

    handler: typing.Final = await initialize_concurrent_processing(context=broker.context)
    try:
        handler.committer.commit_all = AsyncMock()
        await subscriber._listener.on_partitions_revoked(partitions)
        await subscriber._listener.on_partitions_assigned(partitions)
    finally:
        await stop_concurrent_processing(broker.context)

    handler.committer.commit_all.assert_awaited_once()
    user_listener.on_partitions_revoked.assert_called_once_with(partitions)
    user_listener.on_partitions_assigned.assert_called_once_with(partitions)


async def test_an_explicit_concurrent_listener_is_left_alone() -> None:
    broker: typing.Final = _broker_in_app()
    explicit: typing.Final = ConsumerRebalanceListener.from_context(broker.context)
    subscriber: typing.Final = _subscribe(broker, "a", group_id="g", ack_policy=AckPolicy.MANUAL, listener=explicit)

    await _initialize(broker)
    try:
        assert subscriber._listener is explicit
    finally:
        await stop_concurrent_processing(broker.context)


async def test_restarting_processing_does_not_wrap_the_listener_twice() -> None:
    broker: typing.Final = _broker_in_app()
    subscriber: typing.Final = _subscribe(broker, "a", group_id="g", ack_policy=AckPolicy.MANUAL)
    await _initialize(broker)
    await stop_concurrent_processing(broker.context)
    first: typing.Final = subscriber._listener

    await _initialize(broker)
    try:
        assert subscriber._listener is first
    finally:
        await stop_concurrent_processing(broker.context)


async def test_missing_application_is_logged_as_error(caplog: pytest.LogCaptureFixture) -> None:
    broker: typing.Final = KafkaBroker()
    subscriber: typing.Final = _subscribe(broker, "a", group_id="g", ack_policy=AckPolicy.MANUAL)

    await _initialize(broker)
    try:
        assert subscriber._listener is None
        assert [r.levelno for r in caplog.records if "rebalance listener" in r.getMessage()] == [logging.ERROR]
    finally:
        await stop_concurrent_processing(broker.context)


async def test_already_started_broker_is_logged_as_error(caplog: pytest.LogCaptureFixture) -> None:
    """The listener is fixed when the consumer subscribes, so attaching after start would do nothing."""
    broker: typing.Final = _broker_in_app()
    subscriber: typing.Final = _subscribe(broker, "late-topic", group_id="g", ack_policy=AckPolicy.MANUAL)
    broker.running = True

    await _initialize(broker)
    try:
        assert subscriber._listener is None
        errors: typing.Final = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "late-topic" in errors[0].getMessage()
    finally:
        await stop_concurrent_processing(broker.context)


@pytest.mark.parametrize("subscriber_class", [LogicSubscriber, ConcurrentBetweenPartitionsSubscriber])
def test_faststream_still_reads_the_listener_when_the_consumer_subscribes(
    subscriber_class: type[LogicSubscriber[typing.Any]],
) -> None:
    """INVARIANT: attaching relies on FastStream reading the private `_listener` in `start()`.

    FastStream offers no public way to add a rebalance listener after a subscriber is declared.
    If a FastStream release renames the attribute or stops reading it at start, attaching would
    silently do nothing; this fails first.
    """
    assert "listener=self._listener" in inspect.getsource(subscriber_class.start)
