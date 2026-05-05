import typing

from faststream import ContextRepo

from faststream_concurrent_aiokafka import consts


if typing.TYPE_CHECKING:
    from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler


def is_kafka_handler_healthy(context: ContextRepo) -> bool:
    handler: KafkaConcurrentHandler | None = context.get(consts.PROCESSING_CONTEXT_KEY)
    return handler is not None and handler.is_healthy
