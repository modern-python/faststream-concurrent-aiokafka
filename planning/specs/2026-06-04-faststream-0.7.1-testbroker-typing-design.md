# FastStream 0.7.1 TestBroker typing alignment — design

**Status:** Draft
**Date:** 2026-06-04
**Slug:** `faststream-0.7.1-testbroker-typing`

## Goal

Adopt the upstream `TestBroker` typing fix shipped in FastStream 0.7.1
(ag2ai/faststream#2903) by bumping the FastStream pin to `>=0.7.1,<0.8`
and deleting `tests/mocks.fake_test_broker`, the helper whose sole
purpose was working around 0.7.0's `Broker | list[Broker]` `__aenter__`
return annotation.

## Background

In FastStream 0.7.0, `TestBroker.__aenter__` was annotated
`Broker | list[Broker]`. That union made the natural usage shape fail
mypy/ty:

```python
async with TestKafkaBroker(KafkaBroker()) as br:
    await br.publish(None, "test")
    # error: Item "list[KafkaBroker]" of "KafkaBroker | list[KafkaBroker]"
    #        has no attribute "publish"  [union-attr]
```

We worked around this in [`tests/mocks.py`][mocks-py] with
`fake_test_broker`, an async context manager that enters
`TestKafkaBroker(broker)` and re-yields the value after an
`assert isinstance(test_broker, KafkaBroker)`. The helper also threads a
`connect_only` kwarg through, but no caller ever sets it (all 24
callsites take the default `False`).

FastStream 0.7.1 (PR ag2ai/faststream#2903) fixes the root cause:

1. `TestBroker` becomes `Generic[Broker, EnterType]`. `EnterType` uses
   `typing_extensions.TypeVar` with `default=Any` for backward
   compatibility.
2. `__aenter__` returns `EnterType` instead of `Broker | list[Broker]`.
3. Each concrete subclass adds two `@overload`s on `__init__` that bind
   `EnterType` to either `SomeBroker` (single) or
   `tuple[SomeBroker, ...]` (multi). **Note the multi case now returns
   a `tuple`, not a `list`.**
4. The AST-inspection helper in `_internal/testing/ast.py` learns to
   walk past *any* number of `__init__` frames, so subclasses that add
   their own `__init__` continue to work.

Because every callsite in this repo passes a single broker,
`TestKafkaBroker(broker).__aenter__()` is now statically `KafkaBroker`
with zero narrowing required. `fake_test_broker` has no remaining
purpose.

[mocks-py]: ../../tests/mocks.py

## Scope

### In scope
- Bump the FastStream pin in `pyproject.toml`:
  `faststream[kafka]>=0.7,<0.8` → `faststream[kafka]>=0.7.1,<0.8`.
- Delete `fake_test_broker` from `tests/mocks.py` and drop the
  now-unused imports (`contextlib`, `TestKafkaBroker`).
- Replace all 24 `async with fake_test_broker(...) as test_broker:`
  callsites with `async with TestKafkaBroker(...) as test_broker:`:
  - `tests/test_middleware.py` — 20 sites
  - `tests/test_healthcheck.py` — 4 sites
- Update the imports in those two test modules: drop `fake_test_broker`
  from `from tests.mocks import ...`, add
  `from faststream.kafka import TestKafkaBroker`.

### Out of scope
- No behavioural change to the library (`faststream_concurrent_aiokafka/`)
  — this is a test-infrastructure cleanup riding the floor bump.
- No `TestBroker` generic rebinding or ASGI registry annotation update:
  this repo is middleware-only, defines no `TestBroker` subclass, and
  ships no ASGI registry hook. (The redis-timers package has both;
  faststream-concurrent-aiokafka has neither.)
- No new regression test. The 24 inlined callsites already exercise the
  new `__aenter__` return shape on every `test_broker.publish(...)` /
  `test_broker.start()` / direct attribute access — an explicit guard
  test would only re-verify upstream's API.

## Detailed changes

### `pyproject.toml`

Current (line 30): `"faststream[kafka]>=0.7,<0.8",`
After:             `"faststream[kafka]>=0.7.1,<0.8",`

### `tests/mocks.py`

Before (lines 1–7, 97–107):

```python
"""Shared mock classes used across multiple test modules."""

import contextlib
import typing
from unittest.mock import AsyncMock, Mock

from faststream.kafka import KafkaBroker, TestKafkaBroker

# ... (unchanged middle of file) ...

@contextlib.asynccontextmanager
async def fake_test_broker(broker: KafkaBroker, *, connect_only: bool = False) -> typing.AsyncIterator[KafkaBroker]:
    """TestKafkaBroker for a single broker, narrowed to KafkaBroker.

    faststream 0.7's TestKafkaBroker.__aenter__ returns Broker | list[Broker]
    (variadic constructor). Every call site here passes one broker; this helper
    keeps that invariant in one place so test bodies don't repeat the narrowing.
    """
    async with TestKafkaBroker(broker, connect_only=connect_only) as test_broker:
        assert isinstance(test_broker, KafkaBroker)
        yield test_broker
```

After:

```python
"""Shared mock classes used across multiple test modules."""

import typing
from unittest.mock import AsyncMock, Mock
```

`contextlib`, `KafkaBroker`, and `TestKafkaBroker` are all only
referenced by the deleted helper, so the entire
`from faststream.kafka import KafkaBroker, TestKafkaBroker` line and
`import contextlib` are removed. `patched_message` and all `Mock*`
classes are unaffected.

### `tests/test_middleware.py`

- Line 10: `from faststream.kafka import KafkaBroker`
  → `from faststream.kafka import KafkaBroker, TestKafkaBroker`.
- Line 19: `from tests.mocks import fake_test_broker, patched_message`
  → `from tests.mocks import patched_message`.

- All 20 occurrences of
  `async with fake_test_broker(setup_broker) as test_broker:`
  → `async with TestKafkaBroker(setup_broker) as test_broker:`

  These are exhaustively at lines 36, 73, 102, 115, 134, 148, 179,
  214, 252, 278, 299, 321, 342, 361, 383, 390, 402, 422, 450 (per
  current `grep` output — the executing-plans pass should re-grep
  to confirm before mass-replacing).

### `tests/test_healthcheck.py`

- Line 4: `from faststream.kafka import KafkaBroker`
  → `from faststream.kafka import KafkaBroker, TestKafkaBroker`.
- Line 11: `from tests.mocks import fake_test_broker` → delete the
  entire line (it's the only `tests.mocks` import in this file).
- All 4 occurrences of `async with fake_test_broker(broker) as test_broker:`
  at lines 16, 26, 32, 40 → `async with TestKafkaBroker(broker) as test_broker:`.

## Validation

Run in order:

1. `just install` — pulls `faststream==0.7.1`.
2. `just lint` — confirms ruff/format clean after the deletions and
   import shuffles. `ty check` (run by `just lint`) verifies the
   `__aenter__` return narrows to `KafkaBroker` at every callsite
   without the helper assert.
3. `just test` — full suite (runs the Redpanda container too). Every
   test that called `fake_test_broker` now calls `TestKafkaBroker`
   directly, so any regression in the 0.7.1 `EnterType` overload would
   show up as a type error or an AttributeError on `test_broker`.

## Risks

- **None significant.** Pin floor goes from 0.7 to 0.7.1 — a trivial
  increment matching what redis-timers shipped in its PR #27. The
  library code (`faststream_concurrent_aiokafka/`) is unchanged, so
  existing users see no API or behaviour difference.
- **`_get_broker_registry` annotation drift** (the redis-timers
  package had to update `TestBroker[Any]` → `TestBroker[Any, Any]`):
  not applicable here — this repo does not patch
  `faststream.asgi.factories.asyncapi.try_it_out._get_broker_registry`
  and has no `TestBroker` subclass.

## Rollout

- Single PR on branch `chore/faststream-0.7.1-testbroker-typing`.
- Bundled commit (the pin bump and the helper deletion are tightly
  coupled — the deletion is only safe once we require 0.7.1+).
- Follows the project workflow in `CLAUDE.md`:
  brainstorming → spec → writing-plans → plan →
  executing-plans / subagent-driven-development →
  requesting-code-review → finishing-a-development-branch.
