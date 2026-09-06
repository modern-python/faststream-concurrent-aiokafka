# faststream-concurrent-aiokafka

Concurrent message processing for FastStream's Kafka broker: a middleware that turns each
`AckPolicy.MANUAL` message into a bounded background asyncio task, and a batch committer that
advances each partition's offset only once the task for that message has finished — so
concurrency does not cost the at-least-once guarantee.

## Language

A term is listed only when there is a synonym to reject, or a meaning subtle enough that code and
docs must agree on it. General programming vocabulary does not belong here, however heavily this
package uses it.

Most of the vocabulary is **not this package's**. `broker`, `subscriber`, `middleware`,
`ack policy` and `ContextRepo` are [FastStream](https://faststream.ag2.ai/)'s; `consumer`,
`consumer group`, `partition`, `offset`, `commit` and `rebalance` are Kafka's, reached through
[aiokafka](https://aiokafka.readthedocs.io/). Those projects are the authority for all of them and
nothing here redefines one. The six below are local to this package's concurrency model, and each
exists because getting the word wrong here has a concrete cost.

**Handler**:
The `KafkaConcurrentHandler` — the engine that owns the concurrency semaphore, the in-flight task
set, and the committer. Exactly one exists per `initialize_concurrent_processing` call. FastStream
uses the same word for the user's subscriber function, and keys that function under `"handler_"` in
its context, so a bare "handler" in this repo can mean either. Both senses are established here and
neither is being renamed; write `KafkaConcurrentHandler` or "the concurrent handler" whenever a bare
"handler" could be read as the subscriber function.

**User task**:
The `asyncio.Task` wrapping one message's user coroutine, from dispatch until its done-callback
runs. It is the unit the semaphore bounds, `stop()` cancels, and the committer waits on.
_Avoid_: in-flight handler — a handler in flight is a task, and "handler" is the ambiguous word
above.

**Route**:
The verdict `_classify` returns for one message: `_PassThrough`, `_Dispatch`, `_Skip` or
`_Refuse`. The distinction that must not blur is **pass-through** versus **skip**: a
passed-through message is processed normally and its offset belongs to whoever would own it if
this middleware were absent, while a skipped message is dropped mid-shutdown with its offset left
uncommitted for redelivery. They sound alike and their offset consequences are opposite.

**Ready prefix**:
The leading contiguous run of finished tasks on one partition — the only offsets eligible to
commit, and all `ready` ever means in `take_ready` / `ReadyCommit` / `extract_ready_prefixes`. It
is not "the finished tasks": a single unfinished task at the head holds back every finished task
behind it, which is what keeps commits from jumping past in-flight work.

**Cancellation watermark**:
The per-`(consumer, partition)` floor recorded when a cancelled task is seen, past which offsets
must not advance until a rebalance clears it. Distinct from the cancelled task itself being a
**hard boundary** within one commit round: the boundary stops that round, the watermark is what
makes the stop survive later rounds.

**Control signal**:
A member of FastStream's `IgnoredException` family — `AckMessage`, `RejectMessage`, `SkipMessage`,
`NackMessage`, `StopConsume`, `StopApplication` — raised inside a dispatched user task. The family
is FastStream's; the two verbs are this package's. Every signal is **absorbed** (it never ends the
task); only some are **honoured** (the library does what the signal asks). The words are not
interchangeable, and the gap between them is a documented limitation, not a bug — see
[ADR-0003](docs/adr/0003-control-signals-not-honoured.md).
