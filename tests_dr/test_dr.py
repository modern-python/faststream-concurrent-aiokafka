import asyncio
import collections
import dataclasses
import json
import logging
import pathlib
import random
import re
import time
import typing
import uuid

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import TopicPartition
from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.middlewares import AckPolicy

from faststream_concurrent_aiokafka import (
    KafkaConcurrentProcessingMiddleware,
    consts,
    initialize_concurrent_processing,
    is_kafka_handler_healthy,
    stop_concurrent_processing,
)
from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler


BOOTSTRAP: typing.Final = ["localhost:19092", "localhost:29092", "localhost:39092"]
NODES: typing.Final = (0, 1, 2)
PARTITIONS: typing.Final = 6
RESULTS_DIR: typing.Final = pathlib.Path(__file__).parent / "results"
CONVERGE_DEADLINE_SEC: typing.Final = 150.0
BEFORE_FAULT: typing.Final = 2000
DURING_FAULT: typing.Final = 1000
AFTER_RESTORE: typing.Final = 1000
HEALTHY_RE: typing.Final = re.compile(r"Healthy:\s+true")


def _container(node: int) -> str:
    return f"fca-dr-{node}"


async def _docker(*args: str, check: bool = True) -> str:
    proc: typing.Final = await asyncio.create_subprocess_exec(
        "docker", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    if check and proc.returncode:
        msg = f"docker {' '.join(args)}: {err.decode()}"
        raise RuntimeError(msg)
    return out.decode()


async def _cluster_healthy() -> bool:
    for node in NODES:
        out = await _docker(
            "exec", _container(node), "rpk", "cluster", "health", "-X", f"brokers=redpanda-{node}:9092", check=False
        )
        if "Healthy:" in out:
            return HEALTHY_RE.search(out) is not None
    return False


async def _heal_cluster() -> None:
    for node in NODES:
        await _docker("unpause", _container(node), check=False)
        await _docker("start", _container(node), check=False)
    deadline: typing.Final = time.monotonic() + 120
    while time.monotonic() < deadline:
        if await _cluster_healthy():
            return
        await asyncio.sleep(1)
    msg = "cluster did not become healthy"
    raise RuntimeError(msg)


@pytest.fixture(autouse=True)
async def healthy_cluster() -> typing.AsyncIterator[None]:
    await _heal_cluster()
    yield
    await _heal_cluster()


class _LogCollector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.records: list[tuple[float, str, str, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append((time.monotonic(), record.name, record.levelname, record.getMessage()[:140]))


@pytest.fixture
def logs() -> typing.Iterator[_LogCollector]:
    collector: typing.Final = _LogCollector()
    loggers: typing.Final = [logging.getLogger(n) for n in ("faststream_concurrent_aiokafka", "aiokafka")]
    for lg in loggers:
        lg.addHandler(collector)
        lg.setLevel(logging.WARNING)
    yield collector
    for lg in loggers:
        lg.removeHandler(collector)


@dataclasses.dataclass(kw_only=True)
class _Run:
    topic: str
    group: str
    processed: collections.Counter[int] = dataclasses.field(default_factory=collections.Counter)
    acked: set[int] = dataclasses.field(default_factory=set)
    failed_produce: int = 0
    health_samples: list[tuple[float, bool, float]] = dataclasses.field(default_factory=list)
    marks: dict[str, float] = dataclasses.field(default_factory=dict)

    def mark(self, name: str) -> None:
        self.marks[name] = time.monotonic()


async def _create_topic(topic: str) -> None:
    admin: typing.Final = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=PARTITIONS,
                    replication_factor=3,
                    topic_configs={"min.insync.replicas": "2"},
                )
            ]
        )
    finally:
        await admin.close()


async def _coordinator(group: str) -> int:
    admin: typing.Final = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        return await admin.find_coordinator(group)
    finally:
        await admin.close()


async def _commit_lag(topic: str, group: str) -> int | None:
    admin: typing.Final = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=5000)
    consumer: typing.Final = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, request_timeout_ms=5000)
    try:
        await asyncio.wait_for(admin.start(), 10)
        await asyncio.wait_for(consumer.start(), 10)
        tps: typing.Final = [TopicPartition(topic, p) for p in range(PARTITIONS)]
        ends: typing.Final = await asyncio.wait_for(consumer.end_offsets(tps), 10)
        committed: typing.Final = await asyncio.wait_for(admin.list_consumer_group_offsets(group, partitions=tps), 10)
        return sum(ends[tp] - max(committed[tp].offset, 0) if tp in committed else ends[tp] for tp in tps)
    except Exception:  # noqa: BLE001
        return None
    finally:
        await admin.close()
        await consumer.stop()


async def _produce(producer: AIOKafkaProducer, run: _Run, ids: range) -> None:
    async def one(i: int) -> None:
        try:
            await producer.send_and_wait(run.topic, json.dumps({"id": i}).encode())
            run.acked.add(i)
        except Exception:  # noqa: BLE001
            run.failed_produce += 1

    await asyncio.gather(*(one(i) for i in ids))


def _app(run: _Run) -> KafkaBroker:
    broker: typing.Final = KafkaBroker(BOOTSTRAP, log_level=logging.DEBUG)
    broker.add_middleware(KafkaConcurrentProcessingMiddleware)

    @broker.subscriber(run.topic, group_id=run.group, auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler(msg: dict[str, int]) -> None:
        await asyncio.sleep(random.uniform(0.01, 0.05))  # noqa: S311
        run.processed[msg["id"]] += 1

    FastStream(broker)
    return broker


async def _start_app(broker: KafkaBroker) -> None:
    await initialize_concurrent_processing(
        context=broker.context,
        concurrency_limit=20,
        commit_batch_size=50,
        commit_batch_timeout_sec=2,
    )
    await broker.start()


def _handler(broker: KafkaBroker) -> KafkaConcurrentHandler:
    return broker.context.get(consts.PROCESSING_CONTEXT_KEY)


async def _sample_health(broker: KafkaBroker, run: _Run) -> None:
    while True:
        run.health_samples.append(
            (time.monotonic(), is_kafka_handler_healthy(broker.context), _handler(broker).commit_stall_sec)
        )
        await asyncio.sleep(0.25)


def _max_stall(run: _Run, start: str, end: str) -> float:
    return max(
        (stall for t, _, stall in run.health_samples if run.marks[start] <= t <= run.marks[end]),
        default=0.0,
    )


async def _wait_for(predicate: typing.Callable[[], bool], within_sec: float) -> bool:
    deadline: typing.Final = time.monotonic() + within_sec
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.2)
    return False


async def _wait_converged(run: _Run, within_sec: float) -> int | None:
    deadline: typing.Final = time.monotonic() + within_sec
    lag: int | None = None
    while time.monotonic() < deadline:
        if run.acked <= run.processed.keys():
            run.marks.setdefault("all_processed", time.monotonic())
            lag = await _commit_lag(run.topic, run.group)
            if lag == 0:
                run.mark("fully_committed")
                return 0
        await asyncio.sleep(1)
    return lag


def _report(name: str, run: _Run, logs: _LogCollector, extra: dict[str, typing.Any]) -> dict[str, typing.Any]:
    t0: typing.Final = run.marks.get("fault", 0.0)
    unhealthy: typing.Final = [round(t - t0, 1) for t, ok, _ in run.health_samples if not ok]
    log_counts: typing.Final = collections.Counter(f"{lvl} {lg}: {msg[:90]}" for _, lg, lvl, msg in logs.records)
    report: typing.Final = {
        "scenario": name,
        "acked": len(run.acked),
        "failed_produce": run.failed_produce,
        "processed_unique": len(run.processed),
        "missing": len(run.acked - run.processed.keys()),
        "duplicates": sum(c - 1 for c in run.processed.values()),
        "marks_rel_to_fault_sec": {k: round(v - t0, 1) for k, v in run.marks.items()},
        "unhealthy_samples": len(unhealthy),
        "first_unhealthy_rel_sec": unhealthy[:1],
        "max_commit_stall_sec": round(max((stall for _, _, stall in run.health_samples), default=0.0), 1),
        "logs": dict(log_counts.most_common(15)),
        **extra,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / f"{name}.json").write_text(json.dumps(report, indent=2))
    return report


@dataclasses.dataclass(frozen=True, kw_only=True)
class Fault:
    name: str
    target: typing.Literal["coordinator", "non_coordinator", "all"]
    action: typing.Literal["kill", "pause"]
    restore_after_sec: float
    produce_during: bool


FAULTS: typing.Final = [
    Fault(
        name="kill_non_coordinator", target="non_coordinator", action="kill", restore_after_sec=20, produce_during=True
    ),
    Fault(name="kill_coordinator", target="coordinator", action="kill", restore_after_sec=20, produce_during=True),
    Fault(name="pause_coordinator", target="coordinator", action="pause", restore_after_sec=30, produce_during=True),
    Fault(name="kill_all_brokers", target="all", action="kill", restore_after_sec=30, produce_during=False),
]


async def _inject(fault: Fault, coordinator: int) -> list[int]:
    if fault.target == "all":
        nodes = list(NODES)
    elif fault.target == "coordinator":
        nodes = [coordinator]
    else:
        nodes = [next(n for n in NODES if n != coordinator)]
    for node in nodes:
        await _docker(fault.action, _container(node))
    return nodes


async def _restore(fault: Fault, nodes: list[int]) -> None:
    for node in nodes:
        await _docker("unpause" if fault.action == "pause" else "start", _container(node))


@pytest.mark.parametrize("fault", FAULTS, ids=[f.name for f in FAULTS])
async def test_broker_fault_keeps_at_least_once(fault: Fault, logs: _LogCollector) -> None:
    run: typing.Final = _Run(topic=f"dr-{uuid.uuid4().hex[:8]}", group=f"dr-g-{uuid.uuid4().hex[:8]}")
    await _create_topic(run.topic)
    producer: typing.Final = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", request_timeout_ms=10000)
    await producer.start()
    broker: typing.Final = _app(run)
    await _start_app(broker)
    sampler: typing.Final = asyncio.create_task(_sample_health(broker, run))
    stop_sec: float | None = None
    try:
        await _produce(producer, run, range(BEFORE_FAULT))
        assert await _wait_for(lambda: len(run.processed) >= 300, 30), "consumer never started processing"
        coordinator: typing.Final = await _coordinator(run.group)

        run.mark("fault")
        nodes: typing.Final = await _inject(fault, coordinator)
        if fault.produce_during:
            await _produce(producer, run, range(BEFORE_FAULT, BEFORE_FAULT + DURING_FAULT))
        await asyncio.sleep(max(0.0, fault.restore_after_sec - (time.monotonic() - run.marks["fault"])))
        run.mark("restore")
        await _restore(fault, nodes)

        await _produce(producer, run, range(BEFORE_FAULT + DURING_FAULT, BEFORE_FAULT + DURING_FAULT + AFTER_RESTORE))
        run.mark("after_restore_produced")
        lag: typing.Final = await _wait_converged(run, CONVERGE_DEADLINE_SEC)
        final_stall_sec: typing.Final = _handler(broker).commit_stall_sec
    finally:
        sampler.cancel()
        t_stop = time.monotonic()
        await stop_concurrent_processing(broker.context)
        await broker.stop()
        stop_sec = round(time.monotonic() - t_stop, 1)
        await producer.stop()

    outage_stall_sec: typing.Final = _max_stall(run, "fault", "restore")
    report: typing.Final = _report(
        fault.name,
        run,
        logs,
        {
            "coordinator": coordinator,
            "faulted_nodes": nodes,
            "final_lag": lag,
            "stop_sec": stop_sec,
            "outage_max_commit_stall_sec": round(outage_stall_sec, 1),
            "final_commit_stall_sec": final_stall_sec,
        },
    )
    assert report["missing"] == 0, report
    assert lag == 0, report
    assert report["unhealthy_samples"] == 0, report
    assert final_stall_sec == 0, report
    if fault.target == "all":
        assert outage_stall_sec > 0, report


async def test_shutdown_during_full_outage_redelivers_on_restart(logs: _LogCollector) -> None:
    run: typing.Final = _Run(topic=f"dr-{uuid.uuid4().hex[:8]}", group=f"dr-g-{uuid.uuid4().hex[:8]}")
    await _create_topic(run.topic)
    producer: typing.Final = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all", request_timeout_ms=10000)
    await producer.start()
    await _produce(producer, run, range(BEFORE_FAULT))
    await producer.stop()

    broker: typing.Final = _app(run)
    await _start_app(broker)
    assert await _wait_for(lambda: len(run.processed) >= 300, 30), "consumer never started processing"
    processed_before_outage: typing.Final = len(run.processed)

    run.mark("fault")
    for node in NODES:
        await _docker("kill", _container(node))
    await asyncio.sleep(5)

    t_stop: typing.Final = time.monotonic()
    stop_error: str | None = None
    try:
        await asyncio.wait_for(stop_concurrent_processing(broker.context), 60)
    except Exception as exc:  # noqa: BLE001
        stop_error = repr(exc)
    stop_processing_sec: typing.Final = round(time.monotonic() - t_stop, 1)
    t_broker: typing.Final = time.monotonic()
    broker_stop_error: str | None = None
    try:
        await asyncio.wait_for(broker.stop(), 60)
    # aiokafka 0.14.0 Fetcher.close() can raise CancelledError when no broker is reachable.
    except (Exception, asyncio.CancelledError) as exc:  # noqa: BLE001
        broker_stop_error = repr(exc)
    broker_stop_sec: typing.Final = round(time.monotonic() - t_broker, 1)
    processed_at_shutdown: typing.Final = len(run.processed)

    run.mark("restore")
    await _heal_cluster()

    broker2: typing.Final = _app(run)
    await _start_app(broker2)
    try:
        lag: typing.Final = await _wait_converged(run, CONVERGE_DEADLINE_SEC)
    finally:
        await stop_concurrent_processing(broker2.context)
        await broker2.stop()

    report: typing.Final = _report(
        "shutdown_during_full_outage",
        run,
        logs,
        {
            "processed_before_outage": processed_before_outage,
            "processed_at_shutdown": processed_at_shutdown,
            "stop_concurrent_processing_sec": stop_processing_sec,
            "stop_error": stop_error,
            "broker_stop_sec": broker_stop_sec,
            "broker_stop_error": broker_stop_error,
            "final_lag": lag,
        },
    )
    assert report["missing"] == 0, report
    assert lag == 0, report
