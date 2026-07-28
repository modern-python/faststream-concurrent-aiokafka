# Middleware & lifecycle

`middleware.py` wires the handler into FastStream's message flow and provides the
init/stop lifecycle functions. `healthcheck.py` exposes a single probe function.

## Middleware

`KafkaConcurrentProcessingMiddleware` is a FastStream `BaseMiddleware` subclass.
Its `consume_scope` retrieves the handler from `self.context` (the key
`"concurrent_processing"`) and decides whether to route the message through the
concurrent handler.

The routing decision is a pure synchronous `_classify(*, committed, attrs,
handler, is_batch, ack_policy) -> _Route` (in `middleware.py`), where `_Route` is
one of `_PassThrough` / `_Dispatch` / `_Skip` / `_Refuse(reason)`.
`consume_scope` validates the message exists, computes the inputs, then `match`es
the route to its action (pass through / dispatch / log-and-skip / raise). The
decision is unit-tested as a pure function; the branch *order* below is
load-bearing — a multiply-misconfigured subscriber gets the first matching
branch's error.

Pass-through cases (the message is processed normally, not concurrently):

- a FakeConsumer, i.e. running under `TestKafkaBroker`;
- `AckPolicy.ACK_FIRST`, detected via `kafka_message.committed is not None`. It
  is the *only* policy that gets a plain `KafkaMessage` (which starts
  `committed=AckStatus.ACKED`); its offsets belong to aiokafka's
  `enable_auto_commit`, so the middleware leaves it alone.
- any *other* non-`MANUAL` policy, detected from the subscriber's declared
  `ack_policy`. `ACK`, `REJECT_ON_ERROR` and `NACK_ON_ERROR` get
  `KafkaAckableMessage` with `committed=None` exactly like `MANUAL`, so they are
  indistinguishable from it by message shape, but FastStream builds its own
  `AcknowledgementMiddleware` for them (`auto_ack_disabled` covers only
  `{MANUAL, ACK_FIRST}`). Dispatched, they would be acked the instant
  `consume_scope` returns — a bare `consumer.commit()` ahead of every in-flight
  task — so they must not be dispatched.

  They pass through rather than being refused because the middleware is designed
  to be registered **once at broker level** across a mix of subscribers: a
  non-`MANUAL` subscriber must behave exactly as if the middleware were absent.
  Pass-through is safe for the same reason the dispatch was not: each FastStream
  subscriber builds its own `AIOKafkaConsumer`
  (`faststream/kafka/subscriber/usecase.py:94`), so a passed-through
  subscriber's ack touches only its own consumer's partitions and cannot commit
  past another subscriber's in-flight work — and with no background task,
  "consumed" and "processed" stay the same moment, as in stock FastStream.

  `consume_scope` obtains the policy from `self.context.get("handler_")`
  (FastStream enters that scope before any middleware) via
  `getattr(..., "ack_policy", None)`; a policy that cannot be determined is
  `None` and is never assumed to be non-`MANUAL`, so it does not pass through on
  that basis.

This branch is placed **before every refusal** — deliberately, and the ordering
is load-bearing. A non-`MANUAL` subscriber is none of this library's business,
so it must not be rejected for being a batch subscriber, for missing
`initialize_concurrent_processing`, or for anything else. Only after it does
`is_batch` (`isinstance(self.msg, (list, tuple))`) raise a clear `RuntimeError`:
batch subscribers are unsupported *on the concurrent path*, so `batch=True` plus
`MANUAL` is refused while `batch=True` on another policy passes through cleanly.

The middleware refuses to operate if `_enable_auto_commit=True` on the consumer
— auto-commit would defeat the at-least-once offset control the handler exists
to provide.

If the handler has already been stopped, the middleware logs a warning and skips
the message. The offset stays uncommitted, so the message is redelivered on
restart.

## Direct-ack guard

On the `_Dispatch` route only, `consume_scope` calls `_install_ack_guards`, which
shadows `ack`, `nack` and `reject` on that one `KafkaAckableMessage` with
synchronous functions that raise `RuntimeError`. The three guards are
`functools.partial` objects built once at import (`_ACK_GUARDS`), not per
message. `StreamMessage` carries a
`__dict__`, so an instance attribute shadows the class method for this message
alone; because a handler resolves `KafkaMessage` via `Context("message")` — the
same object `consume_scope` holds — one mutation covers handlers and inner
middleware alike.

All three are shadowed, not just `ack`: `reject()` delegates to `self.ack()`, so
guarding only `ack` would report the wrong method to a `reject()` caller.

The guards are synchronous so they fire while `msg.ack()` is *evaluated*, before
any `await` — an async guard would degrade an unawaited call to a
`RuntimeWarning`.

**Route scoping is load-bearing.** Pass-through routes must be left alone:
`TestKafkaBroker`'s `FakeConsumer` path acks normally, under `AckPolicy.ACK_FIRST`
the offsets are aiokafka's, and under `ACK` / `REJECT_ON_ERROR` /
`NACK_ON_ERROR` FastStream's own `AcknowledgementMiddleware` acks. Guarding any
of them would break code this library does not own. Because the guards install
only on `_Dispatch`, that scoping is automatic — a passed-through subscriber
never receives them.

Reaching through to `msg.consumer` stays unguarded — one shared consumer serves
every message and partition, so per-message shadowing does not apply.

## Lifecycle functions

- `initialize_concurrent_processing(context, ...)` creates and starts a handler,
  then stores it in `context` under `"concurrent_processing"`.
- `stop_concurrent_processing(context)` gates on `is_running`, calls
  `handler.stop()`, and clears the context entry so a fresh handler can be
  initialised later. It is safe to call even when the committer's background task
  has already died — `KafkaBatchCommitter.close()` early-returns on a `done()`
  task and logs any exception.

## Healthcheck

`is_kafka_handler_healthy(context)` (`healthcheck.py`) returns `True` iff the
handler is present in the context and reports `is_healthy` — that is, the handler
is running (`_is_running`) **and** the committer's background task is still
alive. It is intended for readiness/liveness probes.
