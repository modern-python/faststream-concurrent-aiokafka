import asyncio
import contextlib
import typing
import uuid

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from faststream import ContextRepo
from faststream.asgi import AsgiFastStream
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.middlewares import AckPolicy

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
    assert processed[0]["id"] == 99


async def test_real_kafka_basic_processing(kafka_bootstrap_servers: str) -> None:
    processed: typing.Final[list[dict[str, int]]] = []
    topic: typing.Final = _topic("basic")
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="basic-group", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
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

    @broker.subscriber(topic, group_id="concurrent-group", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler(msg: dict[str, int]) -> None:
        loop: typing.Final = asyncio.get_event_loop()
        timestamps.append(("start", msg["id"], loop.time()))
        await asyncio.sleep(0.5)
        timestamps.append(("end", msg["id"], loop.time()))

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=100
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

    @broker.subscriber(topic, group_id="limited-group", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
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


async def test_real_kafka_handler_exception_consumer_continues(kafka_bootstrap_servers: str) -> None:
    """A handler exception must not crash the consumer — subsequent messages are processed."""
    processed: typing.Final[list[dict[str, int]]] = []
    topic: typing.Final = _topic("excepts")
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="excepts-group", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
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
    assert processed[0]["id"] == 2


async def test_real_kafka_concurrency_limit_reached(kafka_bootstrap_servers: str) -> None:
    """When all semaphore slots are saturated, subsequent messages queue and are still processed."""
    limit: typing.Final = 2
    n_messages: typing.Final = limit + 2  # force two messages to wait for a free slot
    topic: typing.Final = _topic("saturated")
    broker: typing.Final = _broker(kafka_bootstrap_servers)
    start_times: typing.Final[list[float]] = []
    completed: typing.Final[list[int]] = []

    @broker.subscriber(topic, group_id="saturated-group", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler(msg: dict[str, int]) -> None:
        start_times.append(asyncio.get_event_loop().time())
        await asyncio.sleep(0.5)
        completed.append(msg["id"])

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=limit
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            for i in range(n_messages):
                await broker.publish({"id": i}, topic=topic)
            # enough time for all messages to be processed despite queuing
            await asyncio.sleep(POLL_SLEEP + 0.5 * n_messages)
        finally:
            await stop_concurrent_processing(broker.context)

    assert len(completed) == n_messages, f"Expected {n_messages} completed, got {len(completed)}"
    # The 3rd start must happen after at least one of the first two finished (~0.5 s later)
    start_times_sorted = sorted(start_times)
    first_wave_end = start_times_sorted[0] + 0.5
    assert start_times_sorted[limit] >= first_wave_end - 0.1, (
        "3rd task started before limit freed — semaphore not enforced at saturation"
    )


async def test_real_kafka_multiple_subscribers(kafka_bootstrap_servers: str) -> None:
    """Multiple subscribers on different topics share one broker and process messages concurrently."""
    topic_a: typing.Final = _topic("multi-a")
    topic_b: typing.Final = _topic("multi-b")
    broker: typing.Final = _broker(kafka_bootstrap_servers)
    processed_a: typing.Final[list[int]] = []
    processed_b: typing.Final[list[int]] = []
    n_each: typing.Final = 3

    @broker.subscriber(topic_a, group_id="multi-group-a", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler_a(msg: dict[str, int]) -> None:
        processed_a.append(msg["id"])

    @broker.subscriber(topic_b, group_id="multi-group-b", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler_b(msg: dict[str, int]) -> None:
        processed_b.append(msg["id"])

    await _create_topic(kafka_bootstrap_servers, topic_a)
    await _create_topic(kafka_bootstrap_servers, topic_b)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=100
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            for i in range(n_each):
                await broker.publish({"id": i}, topic=topic_a)
                await broker.publish({"id": i}, topic=topic_b)
            await asyncio.sleep(POLL_SLEEP)
        finally:
            await stop_concurrent_processing(broker.context)

    assert len(processed_a) == n_each, f"topic_a: expected {n_each}, got {len(processed_a)}"
    assert len(processed_b) == n_each, f"topic_b: expected {n_each}, got {len(processed_b)}"
    assert sorted(processed_a) == list(range(n_each))
    assert sorted(processed_b) == list(range(n_each))


async def test_real_kafka_graceful_shutdown_waits_for_tasks(kafka_bootstrap_servers: str) -> None:
    """stop_concurrent_processing waits for in-flight tasks to complete before returning."""
    completed: typing.Final[list[int]] = []
    topic: typing.Final = _topic("shutdown")
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="shutdown-group", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
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

    assert len(completed) == 3


async def test_real_kafka_multi_subscriber_commits_all_offsets(kafka_bootstrap_servers: str) -> None:
    """Offsets for all subscribers are committed — regression for single-consumer assumption in batch committer.

    With AckPolicy.MANUAL, enable_auto_commit=False; the batch committer is the sole
    commit mechanism. After a clean stop, restarting with the same group IDs must replay
    zero messages — proving both consumers' offsets were committed.
    """
    topic_a: typing.Final = _topic("commit-a")
    topic_b: typing.Final = _topic("commit-b")
    group_a: typing.Final = f"commit-group-a-{uuid.uuid4().hex[:6]}"
    group_b: typing.Final = f"commit-group-b-{uuid.uuid4().hex[:6]}"
    n_each: typing.Final = 3

    await _create_topic(kafka_bootstrap_servers, topic_a)
    await _create_topic(kafka_bootstrap_servers, topic_b)

    # Phase 1: consume messages; batch committer commits offsets for both consumers
    processed_a: typing.Final[list[int]] = []
    processed_b: typing.Final[list[int]] = []

    broker1: typing.Final = _broker(kafka_bootstrap_servers)

    @broker1.subscriber(topic_a, group_id=group_a, auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler_a(msg: dict[str, int]) -> None:
        processed_a.append(msg["id"])

    @broker1.subscriber(topic_b, group_id=group_b, auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler_b(msg: dict[str, int]) -> None:
        processed_b.append(msg["id"])

    async with broker1:
        await broker1.start()
        await initialize_concurrent_processing(
            context=broker1.context, commit_batch_size=10, commit_batch_timeout_sec=2, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            for i in range(n_each):
                await broker1.publish({"id": i}, topic=topic_a)
                await broker1.publish({"id": i}, topic=topic_b)
            await asyncio.sleep(POLL_SLEEP)
        finally:
            await stop_concurrent_processing(broker1.context)

    assert len(processed_a) == n_each, f"phase 1: topic_a expected {n_each}, got {len(processed_a)}"
    assert len(processed_b) == n_each, f"phase 1: topic_b expected {n_each}, got {len(processed_b)}"

    # Phase 2: restart with same group IDs; if offsets were committed, nothing replays
    replayed_a: typing.Final[list[int]] = []
    replayed_b: typing.Final[list[int]] = []

    broker2: typing.Final = _broker(kafka_bootstrap_servers)

    @broker2.subscriber(topic_a, group_id=group_a, auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler_a2(msg: dict[str, int]) -> None:  # pragma: no cover
        replayed_a.append(msg["id"])

    @broker2.subscriber(topic_b, group_id=group_b, auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler_b2(msg: dict[str, int]) -> None:  # pragma: no cover
        replayed_b.append(msg["id"])

    async with broker2:
        await broker2.start()
        await initialize_concurrent_processing(
            context=broker2.context, commit_batch_size=10, commit_batch_timeout_sec=2, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            await asyncio.sleep(POLL_SLEEP)
        finally:
            await stop_concurrent_processing(broker2.context)

    assert replayed_a == [], f"topic_a messages replayed after clean stop: {replayed_a}"
    assert replayed_b == [], f"topic_b messages replayed after clean stop: {replayed_b}"


async def test_middleware_on_router(kafka_bootstrap_servers: str) -> None:
    """Middleware set on a KafkaRouter (not broker-level) routes messages through concurrent handler."""
    processed: typing.Final[list[dict[str, int]]] = []
    topic: typing.Final = _topic("router-mw")
    # Plain broker — no broker-level middleware; middleware is scoped to the router
    broker: typing.Final = KafkaBroker(kafka_bootstrap_servers)
    router: typing.Final = KafkaRouter(middlewares=[KafkaConcurrentProcessingMiddleware])

    @router.subscriber(topic, group_id="router-mw-group", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler(msg: dict[str, int]) -> None:
        processed.append(msg)

    broker.include_router(router)

    await _create_topic(kafka_bootstrap_servers, topic)
    async with broker:
        await broker.start()
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            await broker.publish({"id": 7}, topic=topic)
            await asyncio.sleep(POLL_SLEEP)
        finally:
            await stop_concurrent_processing(broker.context)

    assert len(processed) == 1
    assert processed[0]["id"] == 7


async def test_asgi_faststream_basic_processing(kafka_bootstrap_servers: str) -> None:
    """AsgiFastStream lifespan initialises and stops concurrent processing correctly."""
    processed: typing.Final[list[dict[str, int]]] = []
    topic: typing.Final = _topic("asgi")
    broker: typing.Final = _broker(kafka_bootstrap_servers)

    @broker.subscriber(topic, group_id="asgi-group", auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler(msg: dict[str, int]) -> None:
        processed.append(msg)

    @contextlib.asynccontextmanager
    async def lifespan(_context: ContextRepo) -> typing.AsyncIterator[None]:
        # AsgiFastStream injects its own app-level context, which is separate from
        # broker.context. Use broker.context explicitly so the middleware can find
        # the handler via self.context.
        await initialize_concurrent_processing(
            context=broker.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=5
        )
        try:
            yield
        finally:
            await stop_concurrent_processing(broker.context)

    app: typing.Final = AsgiFastStream(broker, lifespan=lifespan)

    await _create_topic(kafka_bootstrap_servers, topic)
    async with app.start_lifespan_context():
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        await broker.publish({"id": 42}, topic=topic)
        await asyncio.sleep(POLL_SLEEP)

    assert len(processed) == 1
    assert processed[0]["id"] == 42
