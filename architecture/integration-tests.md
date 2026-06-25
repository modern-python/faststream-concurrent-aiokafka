# Integration tests

`tests/test_integration.py` exercises the concurrent handler against a **real**
Kafka-compatible broker rather than mocks, so the offset-commit and rebalance
behaviour is verified end to end.

## Broker

Tests run against a Redpanda container (Kafka-compatible, lightweight) via
`testcontainers[kafka]`. The container is **session-scoped** — one instance for
the whole test run — and therefore requires Docker. It runs automatically as
part of `just test`.

## Harness invariants

These are load-bearing facts about driving FastStream/aiokafka in tests; getting
any of them wrong produces flaky or silently-empty test runs:

- `async with KafkaBroker():` only calls `connect()`, which sets up the
  producer. It does **not** start subscribers — you must also call
  `await broker.start()` explicitly to launch the consumer poll tasks.
- Always use `auto_offset_reset="earliest"` on test subscribers. The default
  `"latest"` makes the consumer miss messages published before it gets its
  partition assignment.
- Pre-create topics with `AIOKafkaAdminClient` before starting the broker.
  Auto-creation on first publish triggers a `NotLeaderForPartitionError` retry
  loop that can outlast short sleeps.
- After `await broker.start()`, sleep ~1.5 s before publishing to let the
  consumer join the group and receive partition assignments.
- `AsgiFastStream` lifespan tests must use
  `async with app.start_lifespan_context()` — calling `app.start()` /
  `app.stop()` bypasses the `lifespan` context manager entirely.
- `AsgiFastStream` injects its own app-level `ContextRepo` into the lifespan,
  separate from `broker.context`. Pass `broker.context` explicitly to
  `initialize_concurrent_processing` and `stop_concurrent_processing`.
- Subscriber-level `middlewares` on `@broker.subscriber(...)` takes a
  `SubscriberMiddleware` (a plain `(call_next, msg)` callable), not a
  `BaseMiddleware` subclass. To scope `KafkaConcurrentProcessingMiddleware` to a
  subset of subscribers, use `KafkaRouter(middlewares=[...])` plus
  `broker.include_router(router)`.
