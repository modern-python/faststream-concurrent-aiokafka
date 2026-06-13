# Middleware & lifecycle

`middleware.py` wires the handler into FastStream's message flow and provides the
init/stop lifecycle functions. `healthcheck.py` exposes a single probe function.

## Middleware

`KafkaConcurrentProcessingMiddleware` is a FastStream `BaseMiddleware` subclass.
Its `consume_scope` retrieves the handler from `self.context` (the key
`"concurrent_processing"`) and decides whether to route the message through the
concurrent handler.

Pass-through cases (the message is processed normally, not concurrently):

- a FakeConsumer, i.e. running under `TestKafkaBroker`;
- any subscriber whose ack policy is not MANUAL (detected via
  `kafka_message.committed is not None`) — concurrent commit control only makes
  sense with manual acks.

After the pass-throughs, batch raw messages
(`isinstance(self.msg, (list, tuple))`) raise a clear `RuntimeError`: batch
subscribers are unsupported.

The middleware refuses to operate if `_enable_auto_commit=True` on the consumer
— auto-commit would defeat the at-least-once offset control the handler exists
to provide.

If the handler has already been stopped, the middleware logs a warning and skips
the message. The offset stays uncommitted, so the message is redelivered on
restart.

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
