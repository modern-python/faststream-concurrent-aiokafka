import dataclasses
import logging
import typing

from faststream.exceptions import (
    AckMessage,
    IgnoredException,
    NackMessage,
    RejectMessage,
    SkipMessage,
    StopApplication,
    StopConsume,
)


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class SignalPolicy:
    """How the dispatch shield treats one FastStream control-flow signal.

    ``honored`` records whether absorbing the signal actually satisfies what the caller
    asked for. It drives nothing at runtime; it is the reason the level differs, and it
    keeps that judgement testable next to the level rather than buried in prose.
    """

    level: int
    reason: str
    honored: bool


_UNKNOWN_SIGNAL: typing.Final = SignalPolicy(
    level=logging.ERROR,
    reason="Task raised an unrecognised FastStream control signal; it was absorbed and the offset commits",
    honored=False,
)

# isinstance-matched in order, so a user subclassing a known signal inherits its policy.
# These six types are siblings under IgnoredException - none subclasses another - so the
# order between them carries no meaning beyond readability.
_POLICIES: typing.Final[tuple[tuple[type[IgnoredException], SignalPolicy], ...]] = (
    (
        AckMessage,
        SignalPolicy(
            level=logging.DEBUG,
            reason="Task signalled AckMessage; the offset commits as usual",
            honored=True,
        ),
    ),
    (
        RejectMessage,
        SignalPolicy(
            level=logging.DEBUG,
            reason="Task signalled RejectMessage; for Kafka a reject is an ack, so the offset commits",
            honored=True,
        ),
    ),
    (
        SkipMessage,
        SignalPolicy(
            level=logging.DEBUG,
            reason="Task signalled SkipMessage; the offset commits and processing moves on",
            honored=True,
        ),
    ),
    (
        NackMessage,
        SignalPolicy(
            level=logging.ERROR,
            reason=(
                "Task signalled NackMessage, which concurrent processing cannot honour: "
                "the offset commits instead of being redelivered"
            ),
            honored=False,
        ),
    ),
    (
        StopConsume,
        SignalPolicy(
            level=logging.ERROR,
            reason=(
                "Task signalled StopConsume, which cannot stop a subscriber from a concurrently "
                "dispatched task: the subscriber keeps consuming and the offset commits"
            ),
            honored=False,
        ),
    ),
    (
        StopApplication,
        SignalPolicy(
            level=logging.ERROR,
            reason=(
                "Task signalled StopApplication, which cannot stop the application from a "
                "concurrently dispatched task: the application keeps running and the offset commits"
            ),
            honored=False,
        ),
    ),
)


def classify_signal(exc: IgnoredException) -> SignalPolicy:
    """Return the policy for ``exc``. Total over the family: unknown signals are absorbed too.

    Absorbing an unrecognised signal is deliberate. Letting one escape into the asyncio task
    is never the better default - StopApplication proves the point, since it subclasses
    SystemExit and asyncio re-raises that into the event loop - and the ERROR names it.
    """
    for signal_type, policy in _POLICIES:
        if isinstance(exc, signal_type):
            return policy
    return _UNKNOWN_SIGNAL
