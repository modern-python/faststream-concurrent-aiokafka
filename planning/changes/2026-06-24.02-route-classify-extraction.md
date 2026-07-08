---
summary: Extract consume_scope's 8-branch message classification into a pure _classify(...) -> _Route; consume_scope becomes a precondition + match.
---

# Change: Lift the route decision out of `consume_scope`

**Lane:** lightweight — single source file + its test file, no new module, no
public-API change, internal-only types. (Architecture review candidate #3,
"Worth exploring".)

## Goal

`KafkaConcurrentProcessingMiddleware.consume_scope` interleaves an 8-branch
message classification with the dispatch action, so the routing decision can
only be exercised by driving the whole middleware (broker + context + mock
consumer with the right magic attributes). Extract the classification into a
pure synchronous `_classify(...) -> _Route` so the decision becomes a testable
surface and `consume_scope` becomes a `match` plus the dispatch action.
Strictly behaviour-preserving — the branch *order* is load-bearing (it decides
which error a multiply-misconfigured subscriber gets).

## Approach

A 4-variant internal union carries the action distinction; `_classify` is pure
(no logging, no I/O) and returns one of them:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class _PassThrough: ...      # fake consumer, or non-MANUAL ack → await call_next(msg)
@dataclasses.dataclass(frozen=True, slots=True)
class _Dispatch: ...         # the concurrent path → handle_task(...)
@dataclasses.dataclass(frozen=True, slots=True)
class _Skip: ...             # handler shutting down → log + return None
@dataclasses.dataclass(frozen=True, slots=True)
class _Refuse:
    reason: str              # misconfiguration → raise RuntimeError(reason)

_Route = _PassThrough | _Dispatch | _Skip | _Refuse


def _classify(
    *,
    committed: object | None,
    attrs: _ConsumerAttrs,
    handler: KafkaConcurrentHandler | None,
    is_batch: bool,
) -> _Route:
    if attrs.is_fake:
        return _PassThrough()
    if committed is not None:           # non-MANUAL ack: FastStream manages offsets
        return _PassThrough()
    if is_batch:
        return _Refuse("KafkaConcurrentProcessingMiddleware does not support batch subscribers (batch=True). …")
    if not handler:
        return _Refuse("Concurrent processing is not running. Call `initialize_concurrent_processing` on app startup.")
    if not handler.is_running:
        return _Skip()
    if attrs.auto_commit:
        return _Refuse("KafkaConcurrentProcessingMiddleware requires ack_policy=AckPolicy.MANUAL …")
    return _Dispatch()
```

Boundary decisions (from grilling):
- **`_classify` is pure** — no logging. `_Skip`'s warning and `_Refuse`'s raise
  live in `consume_scope`. `_Refuse` carries the (user-facing) reason string so
  the message is co-located with the branch that produces it and is assertable.
- **`_classify` takes plain values**, not the message/consumer objects, so a
  test writes `_classify(committed=None, attrs=_ConsumerAttrs(is_fake=True, auto_commit=False), handler=…, is_batch=False)` with no broker/context/consumer-mock plumbing — the exact friction this change targets.
- **The no-message precondition stays in `consume_scope`** (it's structurally
  prior to `attrs = _consumer_attrs(msg.consumer)` — you can't read attrs off a
  `None` message — and is a context-plumbing precondition, not a per-message
  routing decision). Its `RuntimeError` still fires first, preserving order.
- **Lives in `middleware.py`**, internal/underscore-prefixed (matches the
  existing `_ConsumerAttrs` / `_consumer_attrs` convention); not in `__all__`.
  A separate `_routing.py` would force moving `_ConsumerAttrs` to avoid a
  circular import — disproportionate for this lane.

`consume_scope` after the change:

```python
kafka_message = self.context.get("message")
if not kafka_message:
    raise RuntimeError("No Kafka message found in context. …")
attrs = _consumer_attrs(kafka_message.consumer)
route = _classify(
    committed=kafka_message.committed,
    attrs=attrs,
    handler=self.context.get(consts.PROCESSING_CONTEXT_KEY),
    is_batch=isinstance(self.msg, (list, tuple)),
)
match route:
    case _PassThrough():
        return await call_next(msg)
    case _Refuse(reason):
        raise RuntimeError(reason)
    case _Skip():
        logger.warning("Kafka middleware. Handler is shutting down, skipping message")
        return None
    case _Dispatch():
        try:
            await concurrent_processing.handle_task(call_next(msg), typing.cast("ConsumerRecord", self.msg), kafka_message)
        except CommitterIsDeadError:
            logger.warning("Kafka middleware. Handler is shutting down, skipping message")
        except asyncio.CancelledError:
            logger.warning("Kafka middleware. Task cancelled during shutdown")
            raise
        return None
```

Behaviour-preservation notes:
- Branch order in `_classify` is identical to today's `consume_scope`
  (fake → committed → batch → no-handler → not-running → auto-commit → dispatch).
- The `# noqa: TRY004` on the batch `RuntimeError` and the `# ty: ignore` on the
  `consume_scope` override are preserved.
- `_Dispatch` needs the handler instance for `handle_task`; `consume_scope`
  reads it from context for both the `_classify` `handler=` argument and the
  dispatch call (the `not handler` / `not handler.is_running` cases never reach
  `_Dispatch`, so the cast/use is safe).

## Files

- `faststream_concurrent_aiokafka/middleware.py` — add `_Route` variants +
  `_classify`; rewrite `consume_scope` to precondition + `match`.
- `tests/test_middleware.py` — add companion pure-`_classify` unit tests (one
  per branch: fake/non-MANUAL → `_PassThrough`; batch/no-handler/auto-commit →
  `_Refuse`; not-running → `_Skip`; happy path → `_Dispatch`). Existing
  through-the-middleware tests stay (they cover the action side).

## Verification

- [ ] Failing test first: add a `_classify` test before the symbol exists —
      `uv run --no-sync pytest tests/test_middleware.py -k classify` → ImportError.
- [ ] Apply the change (add types + `_classify`, rewrite `consume_scope`).
- [ ] `_classify` tests pass — `uv run --no-sync pytest tests/test_middleware.py -k classify -v`.
- [ ] Existing middleware tests pass UNTOUCHED — `uv run --no-sync pytest tests/test_middleware.py -v`.
- [ ] `just test` — full suite green (behaviour preserved).
- [ ] `just lint` — clean (ruff + ty; `match` exhaustiveness over `_Route`).
