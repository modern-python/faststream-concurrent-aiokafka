import typing

from faststream import ContextRepo

from faststream_concurrent_aiokafka.middleware import _PROCESSING_CONTEXT_KEY


if typing.TYPE_CHECKING:
    from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler


def is_kafka_handler_healthy(context: ContextRepo) -> bool:
    """Return True if the KafkaConcurrentHandler stored in *context* is healthy."""
    handler: typing.Final[KafkaConcurrentHandler | None] = context.get(_PROCESSING_CONTEXT_KEY)
    return handler is not None and handler.is_healthy
