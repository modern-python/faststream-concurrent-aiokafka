"""Shared mock classes used across multiple test modules."""

import contextlib
import typing
from unittest.mock import AsyncMock, Mock


class MockAIOKafkaConsumer:
    def __init__(self, group_id: str = "test-group") -> None:
        self._group_id = group_id
        self.commit = AsyncMock()


class MockAsyncioTask:
    def __init__(
        self,
        result: str | None = None,
        done: bool = True,
        cancelled: bool = False,
    ) -> None:
        self._result: str | None = result
        self._done: bool = done
        self._cancelled: bool = cancelled

    def cancelled(self) -> bool:
        return self._cancelled

    def done(self) -> bool:
        return self._done or self._cancelled


class MockKafkaMessage:
    def __init__(
        self,
        topic: str = "test-topic",
        partition: int = 0,
        offset: int = 100,
        headers: dict[str, str] | None = None,
        group_id: str = "test-group",
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.headers = headers or {}
        self.consumer = MockAIOKafkaConsumer(group_id)
        self.committed: None = None  # mirrors KafkaAckableMessage (AckPolicy.MANUAL)


class MockConsumerRecord:
    def __init__(self, topic: str = "test-topic", partition: int = 0, offset: int = 100) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset


class MockKafkaBatchCommitter:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.send_task = AsyncMock()
        self.close = AsyncMock()
        self.spawn = Mock()
        self.commit_all = AsyncMock()
        self.clear_cancellation_watermarks = Mock()
        self._healthy = True

    @property
    def is_healthy(self) -> bool:
        return self._healthy


_DEFAULT_MESSAGE: typing.Final = object()


@contextlib.contextmanager
def patched_message(broker: typing.Any, message: typing.Any = _DEFAULT_MESSAGE) -> typing.Generator[None, None, None]:  # noqa: ANN401
    """Replace ``broker.context.get('message')`` with ``message`` (default: a fresh MockKafkaMessage).

    Pass ``message=None`` explicitly to inject a missing-message scenario.
    Other context keys fall through to the original ``get``. Restored on exit.
    """
    msg: typing.Final = MockKafkaMessage() if message is _DEFAULT_MESSAGE else message
    original_get: typing.Final = broker.context.get

    def mock_get(key: str, default: typing.Any = None) -> typing.Any:  # noqa: ANN401
        if key == "message":
            return msg
        return original_get(key, default)

    broker.context.get = mock_get
    try:
        yield
    finally:
        broker.context.get = original_get
