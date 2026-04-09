# faststream-concurrent-aiokafka

Concurrent message processing middleware for [FastStream](https://faststream.airt.ai/) with aiokafka.

By default FastStream processes Kafka messages sequentially. This library allows you to process multiple messages concurrently using asyncio tasks, with optional batch offset committing.

## Installation

```bash
pip install faststream-concurrent-aiokafka
```

## Usage

```python
from faststream import FastStream, ContextRepo
from faststream.kafka import KafkaBroker
from faststream_concurrent_aiokafka import (
    KafkaConcurrentProcessingMiddleware,
    initialize_concurrent_processing,
    stop_concurrent_processing,
)

broker = KafkaBroker(middlewares=[KafkaConcurrentProcessingMiddleware])
app = FastStream(broker)


@app.on_startup
async def on_startup(context: ContextRepo) -> None:
    await initialize_concurrent_processing(
        context=context,
        concurrency_limit=20,  # max concurrent tasks (0 = unlimited)
    )


@app.on_shutdown
async def on_shutdown(context: ContextRepo) -> None:
    await stop_concurrent_processing(context)


@broker.subscriber("my-topic", group_id="my-group")
async def handle(msg: str) -> None:
    # runs concurrently with other messages
    ...
```

## Batch offset committing

By default aiokafka auto-commits offsets. If you manage commits manually, enable `enable_batch_commit=True` to have the library commit offsets in batches after each task completes:

```python
await initialize_concurrent_processing(
    context=context,
    concurrency_limit=20,
    commit_batch_size=100,
    commit_batch_timeout_sec=5,
    enable_batch_commit=True,
)
```

With batch commit enabled, offsets are committed per partition at the highest completed offset in each batch.

## Consumer group filtering

When multiple consumer groups subscribe to the same topic, producers can tag messages with a `topic_group` header to direct them to a specific group. The middleware skips messages whose `topic_group` header doesn't match the consumer's group ID. Messages with no `topic_group` header are always processed.

```python
# Producer side — send to a specific consumer group only
await broker.publish(
    {"data": "..."},
    topic="my-topic",
    headers={"topic_group": "group-a"},
)
```

## Parameters

### `initialize_concurrent_processing`

| Parameter | Default | Description |
|---|---|---|
| `concurrency_limit` | `10` | Max concurrent asyncio tasks. `0` disables the limit. |
| `commit_batch_size` | `10` | Max messages per commit batch. |
| `commit_batch_timeout_sec` | `10` | Max seconds before flushing a batch. |
| `enable_batch_commit` | `False` | Enable manual batch offset committing. |

## Requirements

- Python >= 3.11
- `faststream[kafka]`
