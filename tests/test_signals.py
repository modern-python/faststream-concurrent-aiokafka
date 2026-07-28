import logging
import typing

import pytest
from faststream.exceptions import (
    AckMessage,
    IgnoredException,
    NackMessage,
    RejectMessage,
    SkipMessage,
    StopApplication,
    StopConsume,
)

from faststream_concurrent_aiokafka._signals import classify_signal


class _FutureSignal(IgnoredException):
    """Stands in for a control signal a later FastStream release might add."""


class _CustomAck(AckMessage):
    """A user subclassing a known signal must inherit its policy."""


@pytest.mark.parametrize(
    ("exc", "expected_level", "expected_honored"),
    [
        (AckMessage(), logging.DEBUG, True),
        (RejectMessage(), logging.DEBUG, True),
        (SkipMessage(), logging.DEBUG, True),
        (NackMessage(), logging.ERROR, False),
        (StopConsume(), logging.ERROR, False),
        (StopApplication(), logging.ERROR, False),
        (_FutureSignal(), logging.ERROR, False),
    ],
)
def test_classify_signal_policy(exc: IgnoredException, expected_level: int, expected_honored: bool) -> None:
    policy: typing.Final = classify_signal(exc)

    assert policy.level == expected_level
    assert policy.honored is expected_honored
    assert policy.reason


@pytest.mark.parametrize(
    "exc",
    [AckMessage(), RejectMessage(), SkipMessage(), NackMessage(), StopConsume(), StopApplication()],
)
def test_classify_signal_reason_names_the_signal(exc: IgnoredException) -> None:
    """The log line must identify which signal fired, since all six share one code path."""
    assert type(exc).__name__ in classify_signal(exc).reason


def test_classify_signal_matches_subclasses_of_known_signals() -> None:
    assert classify_signal(_CustomAck()).level == logging.DEBUG
    assert classify_signal(_CustomAck()).honored is True
