# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
just install      # lock + sync all deps (run after pulling or changing pyproject.toml)
just lint         # eof-fixer, ruff format, ruff check --fix, ty check
just lint-ci      # same but no auto-fix (used in CI)
just build        # build the application Docker image
just test         # run all tests in Docker (starts Redpanda, runs pytest, tears down)
just test-branch  # same with branch coverage
just down         # tear down all containers
just publish      # bump version to $GITHUB_REF_NAME, build, publish to PyPI
```

Run a single test file or test by name:
```bash
uv run --no-sync pytest tests/test_kafka_committer.py
uv run --no-sync pytest -k test_committer_logs_task_exceptions
```

## Architecture

The library provides concurrent Kafka message processing for FastStream. Modules:

**`processing.py` — `KafkaConcurrentHandler`**
The core engine. One handler is created per `initialize_concurrent_processing` call and stored in FastStream's `ContextRepo` under the key `"concurrent_processing"`. It is *not* a singleton — calling `stop_concurrent_processing` clears the context entry so a fresh handler can be initialised. The handler manages:
- An `asyncio.Semaphore` for concurrency limiting (minimum: 1)
- A set of in-flight `asyncio.Task`s, with a done-callback (`_finish_task`) that releases the semaphore and discards the task
- Signal handlers (SIGTERM/SIGINT/SIGQUIT) that trigger graceful shutdown
- A `KafkaBatchCommitter` for offset commits

Key design: `handle_task()` fires-and-forgets the user coroutine as an asyncio task and enqueues a `KafkaCommitTask` on the committer. Offsets are not committed until the user task finishes (at-least-once semantics).

**`middleware.py` — FastStream middleware + lifecycle functions**
- `KafkaConcurrentProcessingMiddleware`: FastStream `BaseMiddleware` subclass. Its `consume_scope` retrieves the handler from `self.context`. It passes through (a) FakeConsumer (TestKafkaBroker) and (b) any subscriber whose ack policy is not MANUAL (`kafka_message.committed is not None`). It refuses if `_enable_auto_commit=True` on the consumer. If the handler has been stopped, it logs a warning and skips the message (the offset stays uncommitted, so the message is redelivered on restart).
- `initialize_concurrent_processing(context, ...)`: create and start a handler, store it in context.
- `stop_concurrent_processing(context)`: gates on `is_running`; calls `handler.stop()` and clears the context entry. Safe to call when the committer task has already died — `KafkaBatchCommitter.close()` early-returns on a `done()` task and logs any exception.

**`healthcheck.py` — `is_kafka_handler_healthy`**
A single function that accepts a `ContextRepo` and returns `True` if the handler is present and `is_healthy` (i.e. `_is_running` AND committer task alive). Intended for readiness/liveness probes.

**`batch_committer.py` — `KafkaBatchCommitter`**
Runs as a background asyncio task (`spawn()`). Pulls `KafkaCommitTask`s off a queue, batches by `(timeout OR batch_size)`, awaits each task's asyncio future, groups by `(consumer_id, partition)`, takes the max offset per partition (stopping at the first cancelled task), and commits via `consumer.commit({TopicPartition: offset+1})`. Transient `KafkaError` re-queues the batch; `CommitFailedError`/`IllegalStateError` (rebalance/revocation) discards it. `CommitterIsDeadError` is raised to callers when the committer's main task has died, which triggers `handler.stop()`.

**`rebalance.py` — `ConsumerRebalanceListener`**
Returned by `handler.create_rebalance_listener()`. On `on_partitions_revoked`, calls `committer.commit_all()` so offsets are flushed before the partition is reassigned, preventing duplicate processing after rebalance.

## Key patterns

- **Type suppression**: use `# ty: ignore[rule-name]` (not `# type: ignore`).
- **No `from __future__ import annotations`**: annotations are evaluated eagerly; `typing.Self`/`typing.Never` are used directly (requires Python ≥ 3.11).
- **Imports at module level**: no local imports inside functions.

## Integration tests

`tests/test_integration.py` runs against a real Redpanda container (Kafka-compatible, lightweight) via `testcontainers[kafka]`. The container is session-scoped — one instance for the whole test run.

**Running integration tests** requires Docker — they run automatically as part of `just test`.

**Key findings from building these tests:**

- `async with KafkaBroker():` only calls `connect()`, which sets up the producer. It does **not** start subscribers. You must also call `await broker.start()` explicitly to launch the consumer poll tasks.
- Always use `auto_offset_reset="earliest"` on test subscribers. The default `"latest"` causes the consumer to miss messages published before it gets its partition assignment.
- Pre-create topics with `AIOKafkaAdminClient` before starting the broker. Auto-creation on first publish triggers a `NotLeaderForPartitionError` retry loop that can outlast short sleeps.
- After `await broker.start()`, sleep ~1.5 s before publishing to let the consumer join the group and receive partition assignments.
- `AsgiFastStream` lifespan tests must use `async with app.start_lifespan_context()` — calling `app.start()` / `app.stop()` bypasses the `lifespan` context manager entirely.
- `AsgiFastStream` injects its own app-level `ContextRepo` into the lifespan, separate from `broker.context`. Pass `broker.context` explicitly to `initialize_concurrent_processing` and `stop_concurrent_processing`.
- Subscriber-level `middlewares` on `@broker.subscriber(...)` takes `SubscriberMiddleware` (a plain `(call_next, msg)` callable), not `BaseMiddleware` subclasses. To scope `KafkaConcurrentProcessingMiddleware` to a subset of subscribers, use `KafkaRouter(middlewares=[KafkaConcurrentProcessingMiddleware])` and `broker.include_router(router)`.
