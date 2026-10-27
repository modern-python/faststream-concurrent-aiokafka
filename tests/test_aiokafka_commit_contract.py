"""Pins aiokafka behaviour the committer's commit timeout is built around.

Cancelling `consumer.commit()` releases aiokafka's commit lock while the shielded request keeps
running, so a retry can land before the request it replaced. If this test starts failing, aiokafka
has changed that behaviour and the outstanding-commit bookkeeping in `KafkaBatchCommitter` may no
longer be needed.
"""

import asyncio
import typing

from aiokafka.consumer.group_coordinator import GroupCoordinator


class _Coordinator:
    def __init__(self) -> None:
        self._commit_lock = asyncio.Lock()
        self._retry_backoff_ms = 100
        self.release_first = asyncio.Event()
        self.events: list[str] = []

    async def ensure_coordinator_known(self) -> None:
        return None

    async def _do_commit_offsets(self, _assignment: object, offsets: dict[str, int]) -> None:
        self.events.append(f"start {offsets['p']}")
        if offsets["p"] == 101:
            await self.release_first.wait()
        self.events.append(f"done {offsets['p']}")


async def test_a_cancelled_commit_can_land_after_its_retry() -> None:
    coordinator: typing.Final = _Coordinator()
    first: typing.Final = asyncio.ensure_future(GroupCoordinator.commit_offsets(coordinator, None, {"p": 101}))  # ty: ignore[invalid-argument-type]
    await asyncio.sleep(0.01)
    first.cancel()
    await asyncio.sleep(0)

    await GroupCoordinator.commit_offsets(coordinator, None, {"p": 102})  # ty: ignore[invalid-argument-type]
    coordinator.release_first.set()
    await asyncio.sleep(0.01)

    assert coordinator.events == ["start 101", "start 102", "done 102", "done 101"]
