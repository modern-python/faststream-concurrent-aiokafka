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

The library provides concurrent Kafka message processing for FastStream. Three modules are exposed:

**`processing.py` — `KafkaConcurrentHandler` (singleton)**
The core engine. Implements the singleton pattern (one instance per process) using `__new__` + `threading.Lock`. Manages:
- An `asyncio.Semaphore` for concurrency limiting (`concurrency_limit=0` disables it)
- A set of in-flight `asyncio.Task`s
- A background observer task that periodically calls `_check_tasks_health()` to discard stale completed tasks
- Signal handlers (SIGTERM/SIGINT/SIGQUIT) that trigger graceful shutdown
- Optional integration with `KafkaBatchCommitter` when `enable_batch_commit=True`

Key design: `handle_task()` fires-and-forgets coroutines as asyncio tasks. Message filtering by consumer group is done via the `topic_group` header — messages whose header doesn't match the consumer's `_group_id` are skipped.

**`middleware.py` — FastStream middleware + lifecycle functions**
- `KafkaConcurrentProcessingMiddleware`: FastStream `BaseMiddleware` subclass. Its `consume_scope` wraps each incoming message in a task submitted to `KafkaConcurrentHandler` (retrieved from FastStream's context).
- `initialize_concurrent_processing(context, ...)`: call on app startup to create and start the handler, storing it in FastStream's global context.
- `stop_concurrent_processing(context)`: call on app shutdown; resets the singleton so it can be re-initialized (important for tests).

**`healthcheck.py` — `is_kafka_handler_healthy`**
A single function that accepts a `ContextRepo` and returns `True` if the `KafkaConcurrentHandler` is present and healthy. Intended for readiness/liveness probes.

**`batch_committer.py` — `KafkaBatchCommitter`**
Runs as a background asyncio task (spawned via `spawn()`). Collects `KafkaCommitTask` objects from a queue, batches them by topic-partition, waits for each task's asyncio future to complete, then commits the max offset per partition to Kafka. Batching is triggered by timeout or batch size, whichever comes first. `CommitterIsDeadError` is raised to callers if the committer's main task has died.

## Key patterns

- **Singleton reset in tests**: `KafkaConcurrentHandler._initialized = False` and `._instance = None` must be reset between tests. The shared `autouse` `reset_singleton` fixture lives in `tests/conftest.py` — do not re-define it in individual test files.
- **Type suppression**: use `# ty: ignore[rule-name]` (not `# type: ignore`) for ty type checker suppressions.
- **No `from __future__ import annotations`**: annotations are evaluated eagerly; `typing.Self`/`typing.Never` are used directly (requires Python ≥ 3.11).

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
