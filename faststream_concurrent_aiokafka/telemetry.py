from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any

from faststream import ContextRepo
from faststream.kafka.opentelemetry import KafkaTelemetryMiddleware
from faststream.kafka.response import KafkaPublishCommand
from faststream.message import StreamMessage
from faststream.opentelemetry.middleware import BaseTelemetryMiddleware


class _ConcurrentTelemetryScope(BaseTelemetryMiddleware[KafkaPublishCommand]):
    _consume_finished = False
    _cleanup_requested = False

    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:  # noqa: ANN401
        try:
            return await super().consume_scope(call_next, msg)
        finally:
            self._consume_finished = True
            if self._cleanup_requested:
                await super().after_processed()

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool | None:
        self._cleanup_requested = True
        if self._consume_finished:
            return await super().after_processed(exc_type, exc_val, exc_tb)
        return False


class KafkaConcurrentTelemetryMiddleware(KafkaTelemetryMiddleware):
    """Kafka telemetry that keeps the process span open through concurrent processing."""

    def __call__(self, msg: object, /, *, context: ContextRepo) -> BaseTelemetryMiddleware[KafkaPublishCommand]:
        return _ConcurrentTelemetryScope(
            msg,
            tracer=self._tracer,
            metrics_container=self._metrics,
            settings_provider_factory=self._settings_provider_factory,
            context=context,
        )
