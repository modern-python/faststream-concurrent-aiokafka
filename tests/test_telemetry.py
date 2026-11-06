import asyncio
import time
from collections.abc import Sequence
from uuid import uuid4

import pytest
from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.kafka.opentelemetry import KafkaTelemetryMiddleware
from faststream.middlewares import AckPolicy
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from faststream_concurrent_aiokafka import (
    KafkaConcurrentProcessingMiddleware,
    initialize_concurrent_processing,
    stop_concurrent_processing,
)


class _ProcessSpanExporter(InMemorySpanExporter):
    def __init__(self, topic: str) -> None:
        super().__init__()
        self.process_name = f"{topic} process"
        self.process_exported = asyncio.Event()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        result = super().export(spans)
        if any(span.name == self.process_name for span in spans):
            self.process_exported.set()
        return result


def _broker_with_telemetry(
    bootstrap_servers: str,
    provider: TracerProvider,
    *,
    router: bool,
) -> tuple[KafkaBroker, KafkaBroker | KafkaRouter]:
    broker = KafkaBroker(bootstrap_servers, middlewares=(KafkaConcurrentProcessingMiddleware,))
    registrator = KafkaRouter() if router else broker
    registrator.add_middleware(KafkaTelemetryMiddleware(tracer_provider=provider))
    if isinstance(registrator, KafkaRouter):
        broker.include_router(registrator)
    FastStream(broker)
    return broker, registrator


@pytest.mark.parametrize("outcome", ["success", "error", "cancel", "inline"])
async def test_process_span_follows_concurrent_task(kafka_bootstrap_servers: str, outcome: str) -> None:
    topic = f"telemetry-{uuid4().hex}"
    exporter = _ProcessSpanExporter(topic)
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    broker, registrator = _broker_with_telemetry(kafka_bootstrap_servers, provider, router=outcome == "error")
    started = asyncio.Event()
    release = asyncio.Event()
    completed: list[int] = []
    handler_task: asyncio.Task | None = None

    @registrator.subscriber(
        topic,
        group_id=topic,
        auto_offset_reset="earliest",
        ack_policy=AckPolicy.ACK if outcome == "inline" else AckPolicy.MANUAL,
    )
    async def handler(body: str) -> None:
        nonlocal handler_task
        handler_task = asyncio.current_task()
        started.set()
        try:
            await release.wait()
            if outcome == "error":
                raise ValueError(body)
        finally:
            completed.append(time.time_ns())

    try:
        async with broker:
            processing = await initialize_concurrent_processing(broker.context)
            try:
                assert await initialize_concurrent_processing(broker.context) is processing
                await broker.start()
                await broker.publish("test exception", topic)
                await asyncio.wait_for(started.wait(), 15)
                assert handler_task is not None
                assert not exporter.process_exported.is_set()
                if outcome == "cancel":
                    handler_task.cancel()
                else:
                    release.set()
                await asyncio.wait_for(exporter.process_exported.wait(), 5)
            finally:
                release.set()
                await stop_concurrent_processing(broker.context)

        (span,) = [span for span in exporter.get_finished_spans() if span.name == exporter.process_name]
        assert span.kind is trace.SpanKind.CONSUMER
        assert (span.end_time or 0) >= completed[0]
        if outcome == "error":
            assert span.status.status_code is trace.StatusCode.ERROR
            assert any(event.name == "exception" for event in span.events)
        if outcome == "cancel":
            assert handler_task.cancelled()
    finally:
        provider.shutdown()
