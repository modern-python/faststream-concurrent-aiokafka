import asyncio
import typing
import uuid

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from faststream.kafka import KafkaBroker

from faststream_concurrent_aiokafka import (
    KafkaConcurrentProcessingMiddleware,
    initialize_concurrent_processing,
    stop_concurrent_processing,
)


CONSUMER_READY_SLEEP = 1.5  # let consumer join the group and get partition assignment
POLL_SLEEP = 3.0  # wait for messages to be consumed after publish


def _topic(prefix: str = "t") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _broker(bootstrap_servers: str) -> KafkaBroker:
    broker: typing.Final = KafkaBroker(bootstrap_servers)
    broker.add_middleware(KafkaConcurrentProcessingMiddleware)
    return broker


async def _create_topic(bootstrap_servers: str, topic: str) -> None:
    """Pre-create a topic so leader election completes before we publish."""
    admin: typing.Final = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    finally:
        await admin.close()


async def test_faststream_real_kafka_no_middleware(kafka_bootstrap_servers: str) -> None:
    processed: typing.Final[list[dict[str, int]]] = []
    topic: typing.Final = _topic("smoke")
    broker: typing.Final = KafkaBroker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="smoke-group", auto_offset_reset="earliest")
    async def handler(msg: dict[str, int]) -> None:
        processed.append(msg)

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        await broker.publish({"id": 99}, topic=topic)
        await asyncio.sleep(POLL_SLEEP)

    assert len(processed) == 1
    assert processed[0]["id"] == 99  # noqa: PLR2004


async def test_real_kafka_basic_processing(kafka_bootstrap_servers: str) -> None:
    processed: typing.Final[list[dict[str, int]]] = []
    topic: typing.Final = _topic("basic")
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="basic-group", auto_offset_reset="earliest")
    async def handler(msg: dict[str, int]) -> None:
        processed.append(msg)

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            await broker.publish({"id": 1}, topic=topic)
            await asyncio.sleep(POLL_SLEEP)
        finally:
            await stop_concurrent_processing(broker.context)

    assert len(processed) == 1
    assert processed[0]["id"] == 1


async def test_real_kafka_concurrent_processing(kafka_bootstrap_servers: str) -> None:
    """Messages are dispatched as concurrent tasks — handler invocations overlap."""
    timestamps: typing.Final[list[tuple[str, int, float]]] = []
    topic: typing.Final = _topic("concurrent")
    n_messages: typing.Final = 5
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="concurrent-group", auto_offset_reset="earliest")
    async def handler(msg: dict[str, int]) -> None:
        loop: typing.Final = asyncio.get_event_loop()
        timestamps.append(("start", msg["id"], loop.time()))
        await asyncio.sleep(0.5)
        timestamps.append(("end", msg["id"], loop.time()))

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=0
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            for i in range(n_messages):
                await broker.publish({"id": i}, topic=topic)
            await asyncio.sleep(POLL_SLEEP + 0.5 * n_messages)
        finally:
            await stop_concurrent_processing(broker.context)

    starts: typing.Final = [t for t in timestamps if t[0] == "start"]
    ends: typing.Final = [t for t in timestamps if t[0] == "end"]
    assert len(starts) == n_messages
    last_start: typing.Final = max(s[2] for s in starts)
    first_end: typing.Final = min(e[2] for e in ends)
    assert last_start < first_end, "Messages were not processed concurrently"


async def test_real_kafka_concurrency_limit(kafka_bootstrap_servers: str) -> None:
    concurrent: typing.Final = [0]
    max_concurrent: typing.Final = [0]
    limit: typing.Final = 2
    topic: typing.Final = _topic("limited")
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="limited-group", auto_offset_reset="earliest")
    async def handler(_msg: dict[str, int]) -> None:
        concurrent[0] += 1
        max_concurrent[0] = max(max_concurrent[0], concurrent[0])
        await asyncio.sleep(0.3)
        concurrent[0] -= 1

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=limit
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            for i in range(8):
                await broker.publish({"id": i}, topic=topic)
            await asyncio.sleep(POLL_SLEEP + 1.5)
        finally:
            await stop_concurrent_processing(broker.context)

    assert max_concurrent[0] <= limit, f"Concurrency limit exceeded: {max_concurrent[0]} > {limit}"
    assert max_concurrent[0] > 1, "Expected concurrent execution but got sequential"


async def test_real_kafka_topic_group_filtering(kafka_bootstrap_servers: str) -> None:
    processed: typing.Final[list[dict[str, int]]] = []
    topic: typing.Final = _topic("filter")
    group_id: typing.Final = "filter-group"
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id=group_id, auto_offset_reset="earliest")
    async def handler(msg: dict[str, int]) -> None:
        processed.append(msg)

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            # No header → processed
            await broker.publish({"id": 1}, topic=topic)
            # Matching header → processed
            await broker.publish({"id": 2}, topic=topic, headers={"topic_group": group_id})
            # Non-matching header → skipped
            await broker.publish({"id": 3}, topic=topic, headers={"topic_group": "other-group"})
            await asyncio.sleep(POLL_SLEEP)
        finally:
            await stop_concurrent_processing(broker.context)

    assert len(processed) == 2  # noqa: PLR2004
    ids: typing.Final = {msg["id"] for msg in processed}
    assert ids == {1, 2}


async def test_real_kafka_handler_exception_consumer_continues(kafka_bootstrap_servers: str) -> None:
    """A handler exception must not crash the consumer — subsequent messages are processed."""
    processed: typing.Final[list[dict[str, int]]] = []
    topic: typing.Final = _topic("excepts")
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="excepts-group", auto_offset_reset="earliest")
    async def handler(msg: dict[str, int]) -> None:
        if msg["id"] == 1:
            msg_0 = "intentional failure"
            raise ValueError(msg_0)
        processed.append(msg)

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            await broker.publish({"id": 1}, topic=topic)
            await broker.publish({"id": 2}, topic=topic)
            await asyncio.sleep(POLL_SLEEP)
        finally:
            await stop_concurrent_processing(broker.context)

    assert len(processed) == 1
    assert processed[0]["id"] == 2  # noqa: PLR2004


async def test_real_kafka_graceful_shutdown_waits_for_tasks(kafka_bootstrap_servers: str) -> None:
    """stop_concurrent_processing waits for in-flight tasks to complete before returning."""
    completed: typing.Final[list[int]] = []
    topic: typing.Final = _topic("shutdown")
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="shutdown-group", auto_offset_reset="earliest")
    async def handler(msg: dict[str, int]) -> None:
        await asyncio.sleep(0.5)
        completed.append(msg["id"])

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        for i in range(3):
            await broker.publish({"id": i}, topic=topic)
        await asyncio.sleep(POLL_SLEEP)  # let messages be received and dispatched
        await stop_concurrent_processing(broker.context)

    assert len(completed) == 3  # noqa: PLR2004
