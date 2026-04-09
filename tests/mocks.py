"""Shared mock classes used across multiple test modules."""

import typing
from unittest.mock import AsyncMock, Mock


class MockAIOKafkaConsumer:
    def __init__(self, group_id: str = "test-group") -> None:
        self._group_id = group_id
        self.commit = AsyncMock()


class MockAsyncioTask:
    def __init__(self, result: str | None = None, exception: Exception | None = None, done: bool = True) -> None:
        self._result: str | None = result
        self._exception: Exception | None = exception
        self._done: bool = done
        self._cancelled: bool = False

    def __await__(self) -> typing.Generator[typing.Any, None, str | None]:
        if self._exception:
            raise self._exception
        if False:  # pragma: no cover
            yield  # makes this a generator so it behaves as a proper awaitable
        return self._result


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
