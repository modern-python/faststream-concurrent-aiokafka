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
- Graceful shutdown: waits up to 10 s for in-flight tasks before exiting
- Signal handling (SIGTERM / SIGINT / SIGQUIT) triggers graceful shutdown
- Background observer task to detect and discard stale completed tasks
- Handler exceptions are logged but do not crash the consumer
- Health check helper to probe handler status from a `ContextRepo`

## 📦 [PyPi](https://pypi.org/project/faststream-concurrent-aiokafka)

## 📝 [License](LICENSE)


## Installation

```bash
pip install faststream-concurrent-aiokafka
```

## Quick Start

`ack_policy=AckPolicy.MANUAL` is **required** on every subscriber — the middleware enforces this at runtime.
Without it, aiokafka's auto-commit timer would commit offsets before processing tasks complete, causing silent message loss on crash.

> **`AsgiFastStream` note**: its lifespan receives an app-level `ContextRepo` separate from `broker.context`. Pass `broker.context` explicitly instead of the injected argument.

```python
from contextlib import asynccontextmanager
from faststream import ContextRepo
from faststream.asgi import AsgiFastStream
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.middlewares import AckPolicy
from faststream_concurrent_aiokafka import (
    KafkaConcurrentProcessingMiddleware,
    initialize_concurrent_processing,
    stop_concurrent_processing,
)

# Middleware applied globally to all subscribers
broker = KafkaBroker(middlewares=[KafkaConcurrentProcessingMiddleware])

# Or scope it to specific subscribers via a router
router = KafkaRouter(middlewares=[KafkaConcurrentProcessingMiddleware])

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

@router.subscriber("other-topic", group_id="other-group", ack_policy=AckPolicy.MANUAL)
async def handle_other(msg: str) -> None:
    ...

broker.include_router(router)
```

## Core Concepts

### KafkaConcurrentProcessingMiddleware

A FastStream `BaseMiddleware` subclass. Add it to your broker to enable concurrent processing. It wraps each incoming message in an asyncio task submitted to `KafkaConcurrentHandler`.

### KafkaConcurrentHandler

The processing engine. Manages:
- An `asyncio.Semaphore` to enforce `concurrency_limit`
- A set of in-flight asyncio tasks
- A background observer that periodically discards stale completed tasks
- Signal handlers for graceful shutdown

### KafkaBatchCommitter

Runs as a background asyncio task. Receives `KafkaCommitTask` objects, waits for each task's asyncio future to complete, then commits the max offset per partition to Kafka. Batching is triggered by size or timeout. If the committer's task dies, `CommitterIsDeadError` is raised to callers.

## API Reference

### `initialize_concurrent_processing(context, ...)`

Create and start the concurrent processing handler; store it in FastStream's context.

| Parameter | Default | Description |
|---|---|---|
| `context` | required | FastStream `ContextRepo` instance |
| `concurrency_limit` | `10` | Max concurrent asyncio tasks (minimum: 1) |
| `commit_batch_size` | `10` | Max messages per commit batch |
| `commit_batch_timeout_sec` | `10.0` | Max seconds before flushing a batch |

Returns the `KafkaConcurrentHandler` instance.

### `stop_concurrent_processing(context)`

Flush pending commits, wait for in-flight tasks (up to 10 s), then stop the handler.

### `is_kafka_handler_healthy(context)`

Returns `True` if the `KafkaConcurrentHandler` stored in `context` is running and healthy, `False` otherwise (not initialized, stopped, or observer task dead). Useful for readiness/liveness probes.

### `KafkaConcurrentProcessingMiddleware`

FastStream middleware class. Pass it to `KafkaBroker(middlewares=[...])`, `broker.add_middleware(...)`, or scope it to a subset of subscribers via `KafkaRouter`. See Quick Start for usage examples.

> **Must be listed first** in any middleware list. `consume_scope` fires the handler as a background task and returns `None` immediately — any middleware wrapping it on the outside would see that premature return and misfire (wrong timing, missed exceptions, early cleanup). Middlewares listed after it run correctly inside the background task.

## How It Works

1. **Message dispatch**: On each incoming message, `consume_scope` calls `handle_task()`, which acquires a semaphore slot then fires the handler coroutine as a background `asyncio.Task`.

2. **Concurrency control**: The semaphore blocks new tasks when `concurrency_limit` is reached. The slot is released via a done-callback when the task finishes or fails.

3. **Offset committing**: Each dispatched task is paired with its Kafka offset and consumer reference and enqueued in `KafkaBatchCommitter`. Once the task completes, the committer groups offsets by partition and calls `consumer.commit(partitions_to_offsets)` with `offset + 1` (Kafka's "next offset to fetch" convention).

4. **Graceful shutdown**: `stop_concurrent_processing` sets the shutdown event, flushes the committer, cancels the observer task, and calls `asyncio.gather` with a 10-second timeout to wait for all in-flight tasks.

## Requirements

- Python >= 3.11
- `faststream[kafka]`
