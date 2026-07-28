---
status: accepted
summary: Nack/StopConsume/StopApplication raised inside a dispatched task are absorbed and logged at ERROR, not honoured; honouring them needs machinery whose only "do not advance" mechanism poisons a partition.
---

# Control signals that cannot be honoured are absorbed and logged, not acted on

**Decision:** `NackMessage`, `StopConsume` and `StopApplication` raised inside a
concurrently dispatched task are **absorbed**, their offset commits, and each
logs an ERROR naming the signal. The library does not attempt to redeliver the
message, stop the subscriber, or exit the application.

## Context

A middleware registered *after* `KafkaConcurrentProcessingMiddleware` is
**inner** — FastStream wraps `middlewares[::-1]` in registration order — so its
`consume_scope` *is* the coroutine dispatched as a background `asyncio.Task`. A
signal raised there never leaves that task, and under `AckPolicy.MANUAL`
FastStream builds no `AcknowledgementMiddleware`
(`kafka/subscriber/config.py`), so nothing in the framework interprets it.

Before 0.6.4 these signals ended the task, which cost three things: the task
retained the exception, its traceback, and every frame the traceback referenced
— including the message body — until the offset committed; asyncio-aware error
reporters such as `sentry_sdk`'s `AsyncioIntegration` reported each as an
**unhandled** error; and `StopApplication`, which subclasses `SystemExit`, was
re-raised by asyncio's `Task.__step` into the event loop, killing the
application with in-flight offsets uncommitted.

0.6.4 absorbs the whole `IgnoredException` family. That fixed the crash and the
noise. It did not make the signals *work*, and this decision records why.

## Decision & rationale

**`NackMessage` — redelivery has no safe implementation here.** Stock FastStream
implements nack for Kafka as `consumer.seek(partition, offset)`
(`faststream/kafka/message.py`), rewinding the fetch position underneath every
other in-flight task on that partition — a duplicate-delivery storm. The
alternative, refusing to advance past the nacked offset, means reusing the
cancelled-task watermark in `_pending_state.py`. That watermark is cleared only
on rebalance, so a single nack would stop every later offset on the partition
from committing until the group rebalances, and a restart would redeliver
everything after it. Both options are worse than committing.

**`StopConsume` / `StopApplication` — the actor is out of reach, and the offset
question repeats.** Stopping the subscriber needs it captured from the scoped
`"handler_"` context at dispatch time (`usecase.py`), because that scope is gone
by the time the task runs, plus a `weakref` guard so concurrent stop requests do
not race on `self.consumer`. `StopApplication` additionally needs
`context.get("app").exit()`. That machinery is buildable, but it inherits the
same unanswerable offset question as nack: does the stopped-at message advance?
Not advancing reintroduces the watermark problem above.

**The ERROR log is the mitigation.** An absorbed-and-loud signal beats a silent
loop teardown that loses in-flight offsets, and beats a silent contradiction of
the caller's intent. The log names the signal so the cause is visible in the
first line, without a traceback — a traceback here is the noise 0.6.3 and 0.6.4
exist to remove, which is why the branches carry `# noqa: TRY400`.

**What this costs.** An application that raises `StopApplication` will not stop.
A subscriber sent `StopConsume` keeps consuming. A nacked message is committed
rather than redelivered. These are stated in the README's Limitations section
and in `architecture/concurrent-handler.md`, because a user must not discover
them from behaviour.

## Revisit trigger

Reopen if a bounded, non-poisoning "do not advance past offset N on this
partition" mechanism exists — one whose watermark clears without waiting for a
rebalance. That single primitive unblocks nack and both stop signals at once;
without it, each is a partial fix that trades a loud failure for a quiet one.

Also reopen if FastStream gains a supported way for a middleware to reach its
subscriber after `process_message` returns, which would remove the
capture-and-`weakref` half of the cost.
