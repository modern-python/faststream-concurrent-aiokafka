# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
just install       # lock + sync all deps (run after pulling or changing pyproject.toml)
just lint          # eof-fixer, ruff format, ruff check --fix, ty check
just lint-ci       # same but no auto-fix (used in CI)
just test          # pytest with coverage
just test-branch   # pytest with branch coverage
just publish       # bump version to $GITHUB_REF_NAME, build, publish to PyPI
```

Run a single test file or test by name:
```bash
uv run --no-sync pytest tests/committer/test_kafka_committer.py
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

**`batch_committer.py` — `KafkaBatchCommitter`**
Runs as a background asyncio task (spawned via `spawn()`). Collects `KafkaCommitTask` objects from a queue, batches them by topic-partition, waits for each task's asyncio future to complete, then commits the max offset per partition to Kafka. Batching is triggered by timeout or batch size, whichever comes first. `CommitterIsDeadError` is raised to callers if the committer's main task has died.

## Key patterns

- **Singleton reset in tests**: `KafkaConcurrentHandler._initialized = False` and `._instance = None` must be reset between tests (done in conftest fixtures via `stop_concurrent_processing`).
- **Type suppression**: use `# ty: ignore[rule-name]` (not `# type: ignore`) for ty type checker suppressions.
- **No `from __future__ import annotations`**: annotations are evaluated eagerly; `typing.Self`/`typing.Never` are used directly (requires Python ≥ 3.11).
