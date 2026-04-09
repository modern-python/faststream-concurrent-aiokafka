import os
import typing

import pytest

from faststream_concurrent_aiokafka.processing import KafkaConcurrentHandler


@pytest.fixture(scope="session")
def kafka_bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture(autouse=True)
def reset_singleton() -> typing.Iterator[None]:
    KafkaConcurrentHandler.reset()
    yield
    KafkaConcurrentHandler.reset()
