# Concurrent handler

`KafkaConcurrentHandler` (`processing.py`) is the core engine that turns a
FastStream Kafka subscriber into a concurrent one. Exactly one handler exists
per `initialize_concurrent_processing` call. It is stored in FastStream's
`ContextRepo` under the key `"concurrent_processing"`.

The handler is **not** a singleton. `stop_concurrent_processing` clears the
context entry, so a fresh handler can be initialised afterwards — the lifecycle
is owned by whoever calls init/stop, not by module-level state.

## What it owns

- An `asyncio.Semaphore` (`_limiter`) bounding in-flight user tasks to the
  configured concurrency limit. The minimum is 1; a limit below that is
  rejected at construction.
- A `set[asyncio.Task]` (`_tracked_tasks`) holding the in-flight user tasks.
  This set exists only so `stop()` can reach in-flight tasks; the committer,
  not this set, is the source of truth for offset progress.
- The per-task done-callback `_finish_task`, which releases the semaphore and
  removes the task from `_tracked_tasks`. If the task failed (and was not
  cancelled), `_finish_task` logs the exception at ERROR with a traceback.
- A `KafkaBatchCommitter` that performs the actual offset commits.

## Dispatch

`handle_task()` acquires the semaphore, then fires-and-forgets the user
coroutine as an `asyncio.Task`, registers `_finish_task` as its done-callback,
and enqueues a `KafkaCommitTask` on the committer via `send_task`. The commit
task carries the asyncio task, the record offset, the consumer, and the
`TopicPartition`.

### The control-signal shield

The user coroutine is wrapped in `_absorb_control_signal` before it becomes a task.
A middleware registered *after* `KafkaConcurrentProcessingMiddleware` is **inner**
— FastStream wraps `middlewares[::-1]` in registration order, so that middleware's
`consume_scope` *is* the dispatched coroutine. A FastStream control signal it raises
would otherwise end the asyncio task, which costs three things no log level can fix:

- The task retains the exception, its traceback, and every frame the traceback
  references — including the message body — until the committer commits the
  offset. Measured under a commit backlog with an 8 KB body: 39.4 KB/message
  retained without the shield, 18.5 KB/message with it.
- asyncio task-factory wrappers see it *inside* the task. `sentry_sdk`'s
  `AsyncioIntegration` wraps every task coroutine in
  `try: await coro / except Exception: reraise(*_capture_exception())` and reports
  the signal as an unhandled error. All six signals subclass `Exception`.
- `StopApplication` subclasses `SystemExit`, and asyncio's `Task.__step`
  special-cases that: it sets the task exception **and re-raises into the event
  loop**, which propagates out of `run_forever`. Unshielded, one such signal kills
  the application with in-flight tasks abandoned and their offsets uncommitted.

Three ordered `except` groups decide the log level, letting Python's own exception
matching do the dispatch (so a user subclass of a known signal inherits its group).
The branch order is load-bearing: both named groups must precede the
`IgnoredException` catch-all. The level states whether absorbing the signal
actually honours the caller:

| signal | offset | log | honoured? |
|---|---|---|---|
| `AckMessage` | commit | DEBUG | yes — committing *is* the ack |
| `RejectMessage` | commit | DEBUG | yes — for Kafka `reject()` is `ack()` |
| `SkipMessage` | commit | DEBUG | yes — skip and move on |
| `NackMessage` | commit | ERROR | no — asks for redelivery, we advance |
| `StopConsume` | commit | ERROR | no — the subscriber keeps consuming |
| `StopApplication` | commit | ERROR | no — the application keeps running |
| unrecognised `IgnoredException` | commit | ERROR | unknown — absorbed so a future signal cannot crash the loop |

Absorbing is offset-neutral: the task completes normally, and a completed task is
committed exactly as one that ended with an exception was. Any `extra_options`
passed to `AckMessage(**extra_options)` are ignored — the ack here is a batched
offset commit, not a per-message `message.ack(**extra_options)` call. Every
exception outside the family still ends the task and is logged by `_finish_task`.

The three ERROR-level signals are a deliberate gap, not an oversight: a nacked
message is committed rather than redelivered, a subscriber sent `StopConsume`
keeps consuming, an application sent `StopApplication` keeps running, and the
ERROR is the whole mitigation. The rationale and the rejected alternatives live
in
[`planning/decisions/2026-07-28-control-signals-not-honoured.md`](../planning/decisions/2026-07-28-control-signals-not-honoured.md),
which also carries the revisit trigger.

Because the shield is a wrapper, `stop()` can cancel it *before its first step*,
in which case it never awaited the coroutine it holds. `_finish_task` closes that
coroutine on the cancelled path — otherwise it would be dropped un-awaited, raising
a `RuntimeWarning` through `sys.unraisablehook` into the very logs and error
reporter the shield exists to keep quiet.

Offsets are **not** committed when the message is dispatched — only after the
user task finishes does its offset become eligible for commit. This is the
at-least-once guarantee: a crash before a task completes leaves its offset
uncommitted, so the message is redelivered on restart.

If `send_task` raises `CommitterIsDeadError` (the committer's background task
has died), `handle_task` logs it, calls `stop()`, and re-raises.

## Shutdown

`stop()` is idempotent (it gates on `is_running`). It flips the running flag,
cancels every in-flight tracked task, then awaits `committer.close()`.

Cancelled tasks are a **hard offset boundary**: the committer drops the
cancelled task and everything after it on the same partition from pending, and
stops the offset advance at the cancelled task. The cancelled-and-after offsets
stay uncommitted and get redelivered on restart — preserving at-least-once even
during an abrupt stop.

Total shutdown wall-clock is bounded by the committer's own
`shutdown_timeout_sec` (default 20 s) and is sub-second under normal
conditions.

The handler installs **no** signal handlers. Shutdown is driven externally by
the FastStream lifespan calling `stop_concurrent_processing`.
