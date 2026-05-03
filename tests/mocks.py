"""Shared mock classes used across multiple test modules."""

from unittest.mock import AsyncMock, Mock


class MockAIOKafkaConsumer:
    def __init__(self, group_id: str = "test-group") -> None:
        self._group_id = group_id
        self.commit = AsyncMock()


class MockAsyncioTask:
    def __init__(
        self,
        result: str | None = None,
        exception: Exception | None = None,
        done: bool = True,
        cancelled: bool = False,
    ) -> None:
        self._result: str | None = result
        self._exception: Exception | None = exception
        self._done: bool = done
        self._cancelled: bool = cancelled

    def cancelled(self) -> bool:
        return self._cancelled

    def done(self) -> bool:
        return self._done or self._cancelled

    def exception(self) -> Exception | None:
        return self._exception


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
        self._healthy = True

    @property
    def is_healthy(self) -> bool:
        return self._healthy
