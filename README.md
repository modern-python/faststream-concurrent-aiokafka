# faststream-concurrent-aiokafka

[![Supported versions](https://img.shields.io/pypi/pyversions/faststream-concurrent-aiokafka.svg)](https://pypi.python.org/pypi/faststream-concurrent-aiokafka)
[![downloads](https://img.shields.io/pypi/dm/faststream-concurrent-aiokafka.svg)](https://pypistats.org/packages/faststream-concurrent-aiokafka)
[![GitHub stars](https://img.shields.io/github/stars/modern-python/faststream-concurrent-aiokafka)](https://github.com/modern-python/faststream-concurrent-aiokafka/stargazers)

Concurrent message processing middleware for [FastStream](https://faststream.airt.ai/) with aiokafka.

By default FastStream processes Kafka messages sequentially — one message at a time per subscriber. This library turns each incoming message into an asyncio task so multiple messages are handled concurrently, while keeping offset commits correct and shutdown graceful.

## Features

- Concurrent message processing via asyncio tasks
- Configurable concurrency limit (semaphore-based)
- Batch offset committing per partition after each task completes
- Rebalance-safe: pending offsets are flushed on partition revocation via `ConsumerRebalanceListener`
- Fast shutdown: cancels in-flight tasks; uncommitted offsets are redelivered on restart (at-least-once)
- Signal handling owned by your lifespan / process manager — this lib does not register SIGTERM/SIGINT handlers
- Handler exceptions are logged but do not crash the consumer
- Health check helper to probe handler status from a `ContextRepo`

## 📦 [PyPi](https://pypi.org/project/faststream-concurrent-aiokafka)

## 📝 [License](LICENSE)


## Installation

```bash
pip install faststream-concurrent-aiokafka
```

## Quick Start

`ack_policy=AckPolicy.MANUAL` is **required** on every concurrent subscriber — the middleware enforces this at runtime.
Without it, FastStream would commit offsets before processing tasks complete, causing silent message loss on crash.
Subscribers that use other ack policies are automatically passed through without concurrent processing.

> **`AsgiFastStream` note**: its lifespan receives an app-level `ContextRepo` separate from `broker.context`. Pass `broker.context` explicitly instead of the injected argument.

```python
from contextlib import asynccontextmanager
from faststream import ContextRepo
from faststream.asgi import AsgiFastStream
from faststream.kafka import KafkaBroker
from faststream.middlewares import AckPolicy
from faststream_concurrent_aiokafka import (
    KafkaConcurrentProcessingMiddleware,
    initialize_concurrent_processing,
    stop_concurrent_processing,
)

broker = KafkaBroker(...)
# Register KCM on the broker before any other middleware (see DI note below)
broker.add_middleware(KafkaConcurrentProcessingMiddleware)

@asynccontextmanager
async def lifespan(_context: ContextRepo):
    await initialize_concurrent_processing(
        context=broker.context,
        concurrency_limit=20,         # max concurrent tasks (minimum: 1)
        commit_batch_size=100,        # commit after this many completed tasks
        commit_batch_timeout_sec=5.0, # or after this many seconds
    )
    try:
        yield
    finally:
        await stop_concurrent_processing(broker.context)

app = AsgiFastStream(broker, lifespan=lifespan)

@broker.subscriber("my-topic", group_id="my-group", ack_policy=AckPolicy.MANUAL)
async def handle(msg: str) -> None:
    ...

# Subscribers without AckPolicy.MANUAL are passed through unchanged
@broker.subscriber("other-topic", group_id="other-group")
async def handle_other(msg: str) -> None:
    ...
```

## Core Concepts

### KafkaConcurrentProcessingMiddleware

A FastStream `BaseMiddleware` subclass. Add it to your broker to enable concurrent processing. It wraps each incoming message in an asyncio task submitted to `KafkaConcurrentHandler`.

### KafkaConcurrentHandler

The processing engine. Manages:
- An `asyncio.Semaphore` to enforce `concurrency_limit`
- In-flight task tracking via a counter + `asyncio.Event` (each task's done-callback releases the semaphore, decrements the counter, and sets the event when it reaches zero)
- A `KafkaBatchCommitter` for offset commits
- Signal handlers for graceful shutdown
- An optional `ConsumerRebalanceListener` (via `handler.create_rebalance_listener()`) that flushes pending commits when partitions are revoked

### KafkaBatchCommitter

Runs as a background asyncio task. A streaming loop absorbs `KafkaCommitTask` objects into per-partition pending state and commits each partition's contiguous-done prefix when total pending crosses `commit_batch_size`, when `commit_batch_timeout_sec` fires, or when `commit_all`/`close` sets the flush event. Cancelled tasks are treated as a hard boundary — the offset advance stops at the cancelled task so it gets redelivered on restart (at-least-once). If the committer's task dies, `CommitterIsDeadError` is raised to callers.

## API Reference

### `initialize_concurrent_processing(context, ...)`

Create and start the concurrent processing handler; store it in FastStream's context.

| Parameter | Default | Description |
|---|---|---|
| `context` | required | FastStream `ContextRepo` instance |
| `concurrency_limit` | `10` | Max concurrent asyncio tasks (minimum: 1) |
| `commit_batch_size` | `10` | Max messages per commit batch |
| `commit_batch_timeout_sec` | `10.0` | Max seconds before flushing a batch |
| `shutdown_timeout_sec` | `20.0` | Max seconds the batch committer waits for its background task to drain before forcing cancellation |

Returns the `KafkaConcurrentHandler` instance.

### `stop_concurrent_processing(context)`

Cancel all in-flight handler tasks, flush completed offsets via the committer, then stop the handler. Uncommitted offsets (from cancelled tasks or anything queued past a cancelled offset) are redelivered on restart — at-least-once.

### `is_kafka_handler_healthy(context)`

Returns `True` if the `KafkaConcurrentHandler` stored in `context` is running and healthy, `False` otherwise (not initialized, stopped, or observer task dead). Useful for readiness/liveness probes.

### `KafkaConcurrentProcessingMiddleware`

FastStream middleware class. Register it via `broker.add_middleware(...)`. See Quick Start for usage examples.

> **Must be outermost.** `consume_scope` fires the handler as a background task and returns `None` immediately. Any middleware that wraps it on the outside will see that premature return and misfire — wrong timing, early cleanup, or missed exceptions. Middlewares added after it (i.e. inner in the chain) run correctly inside the background task.

#### DI framework compatibility (`modern-di-faststream` and similar)

DI frameworks like `modern-di-faststream` register a broker-level middleware that creates a REQUEST-scoped dependency container around each message. If that middleware is **outer** to `KafkaConcurrentProcessingMiddleware`, its scope closes as soon as `consume_scope` returns — before the background task runs — so any dependencies resolved inside the task (database sessions, repositories, …) are created from an already-closed container. Their finalizers never run, leaving connections unreturned to the pool.

**Fix**: call `broker.add_middleware(KafkaConcurrentProcessingMiddleware)` **before** `setup_di(...)` (or any equivalent DI bootstrap call). FastStream stacks broker middlewares so the **first** registered is outermost; adding KCM first makes it wrap the DI middleware, so the DI middleware runs *inside* KCM's background task and can manage the scope lifetime correctly.

```python
broker = KafkaBroker(...)
broker.add_middleware(KafkaConcurrentProcessingMiddleware)  # registered first → outermost
modern_di_faststream.setup_di(app, container=container)    # registered after → inner to KCM
```

## How It Works

1. **Message dispatch**: On each incoming message, `consume_scope` calls `handle_task()`, which acquires a semaphore slot then fires the handler coroutine as a background `asyncio.Task`.

2. **Concurrency control**: The semaphore blocks new tasks when `concurrency_limit` is reached. The slot is released via a done-callback when the task finishes or fails.

3. **Offset committing**: Each dispatched task is paired with its Kafka offset and consumer reference and enqueued in `KafkaBatchCommitter`. Once the task completes, the committer groups offsets by partition and calls `consumer.commit(partitions_to_offsets)` with `offset + 1` (Kafka's "next offset to fetch" convention).

4. **Rebalance handling**: When Kafka revokes a partition, the `ConsumerRebalanceListener` (returned by `handler.create_rebalance_listener()`) calls `committer.commit_all()` to flush pending offsets before the partition is reassigned. This prevents in-flight messages from being redelivered to the new owner.

5. **Shutdown**: `stop_concurrent_processing` cancels every in-flight asyncio task, then awaits `committer.close()`. The committer treats cancelled tasks as a hard offset boundary — cancelled-and-after offsets stay uncommitted and get redelivered on restart. Total wall-clock is sub-second in normal conditions and bounded by `shutdown_timeout_sec` only as a safety net for stuck network commits.

## Migration from < 0.x

Previously, `stop_concurrent_processing` waited up to `2 × shutdown_timeout_sec` for in-flight handlers to drain to completion. The new behavior cancels them immediately. The at-least-once contract is unchanged — uncommitted offsets are redelivered on restart, the same way they always were when the handler crashed mid-task.

| What changed | Old | New |
|---|---|---|
| In-flight handler tasks on stop | drained to completion | **cancelled** |
| `KafkaConcurrentHandler.wait_for_subtasks()` | public method | removed |
| `shutdown_timeout_sec` | applied separately to handler and committer | applied to committer only |
| Signal handler installation | installed automatically | removed — own them via your lifespan / process manager |

If your handlers do non-idempotent work that's expensive to repeat, ensure your handlers are wrapped in `try/finally` so cleanup runs on `CancelledError`, or pin to the previous version of this library. To trigger shutdown on SIGTERM/SIGINT, your lifespan or main entry point must catch the signal and call `stop_concurrent_processing(broker.context)` — under uvicorn / AsgiFastStream this happens automatically through the lifespan `finally` block.

## Requirements

- Python >= 3.11
- `faststream[kafka]`
