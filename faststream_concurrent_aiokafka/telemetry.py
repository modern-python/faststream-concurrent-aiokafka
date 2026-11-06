import sys
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any

from faststream import BaseMiddleware, ContextRepo
from faststream._internal.broker.registrator import Registrator
from faststream._internal.configs.broker import BrokerConfig
from faststream._internal.types import BrokerMiddleware
from faststream.kafka.response import KafkaPublishCommand
from faststream.message import StreamMessage


class _ConcurrentTelemetryScope(BaseMiddleware[KafkaPublishCommand]):
    def __init__(self, scope: BaseMiddleware[KafkaPublishCommand]) -> None:
        super().__init__(scope.msg, context=scope.context)
        self._scope = scope
        self._consume_finished = False
        self._cleanup_requested = False

    async def on_receive(self) -> None:
        await self._scope.on_receive()

    async def publish_scope(
        self,
        call_next: Callable[[KafkaPublishCommand], Awaitable[Any]],
        cmd: KafkaPublishCommand,
    ) -> Any:  # noqa: ANN401
        return await self._scope.publish_scope(call_next, cmd)

    async def consume_scope(
        self,
        call_next: Callable[[StreamMessage[Any]], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> Any:  # noqa: ANN401
        try:
            return await self._scope.consume_scope(call_next, msg)
        finally:
            self._consume_finished = True
            if self._cleanup_requested:
                await self._scope.after_processed()

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> bool | None:
        self._cleanup_requested = True
        if self._consume_finished:
            return await self._scope.after_processed(exc_type, exc_val, exc_tb)
        return False


class _ConcurrentTelemetryFactory:
    def __init__(self, factory: BrokerMiddleware[Any, KafkaPublishCommand]) -> None:
        self._factory = factory

    def __call__(self, msg: object, /, *, context: ContextRepo) -> _ConcurrentTelemetryScope:
        return _ConcurrentTelemetryScope(self._factory(msg, context=context))


def adapt_kafka_telemetry(registrator: Registrator) -> None:
    # A registered FastStream telemetry factory already loaded this module.
    module = sys.modules.get("faststream.kafka.opentelemetry.middleware")
    stock_factory = getattr(module, "KafkaTelemetryMiddleware", None)
    for config in registrator.config.configs:
        if isinstance(config, BrokerConfig) and any(
            type(factory) is stock_factory for factory in config.broker_middlewares
        ):
            config.broker_middlewares = tuple(
                _ConcurrentTelemetryFactory(factory) if type(factory) is stock_factory else factory
                for factory in config.broker_middlewares
            )
    for router in registrator.routers:
        adapt_kafka_telemetry(router)
