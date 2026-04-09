import os

import pytest


@pytest.fixture(scope="session")
def kafka_bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
