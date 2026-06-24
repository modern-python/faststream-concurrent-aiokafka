# Batch committer

`KafkaBatchCommitter` (`batch_committer.py`) owns all offset commits. It runs as
a background asyncio task launched via `spawn()`. Its streaming loop continuously
absorbs `KafkaCommitTask`s from an internal queue and commits each partition's
contiguous-done prefix as offsets become eligible.

The loop is split into two collaborators. `_run_commit_process` is the **async
driver**: it owns the `asyncio.wait` select over three wait-tasks (queue-get /
flush-event / task-completed-event) and the queue, but delegates every
when-to-commit decision to a pure synchronous `CommitScheduler`
(`_commit_scheduler.py`). The driver feeds it observations via `evaluate(...)` and
acts on the returned `Decision` (`should_commit` / `drain_queue_now` /
`timeout_fired`); it never writes a decision field itself. After each commit round
the driver calls `note_committed(...)` to settle the post-commit deadline reset and
the flush-flag clear.

`CommitScheduler` owns the timeout deadline, the flush lifecycle
(`flush_in_progress`), and the shutdown lifecycle (`should_shutdown`). It is
synchronous and I/O-free; the driver passes `now = loop.time()` in. The commit
triggers are computed inside `CommitScheduler.evaluate` (see Commit triggers
below).

Per-partition pending state, the pending count, and cancellation watermarks are
owned by a synchronous, I/O-free `PendingCommits` object (`_pending_state.py`).
The committer keeps the queue, the backpressure ceiling
(`max_uncommitted_tasks`), the `consumer.commit()` I/O, and the
transient-`KafkaError` re-queue logic.

## Commit triggers

A partition's ready prefix is committed when any of these fires — all evaluated
inside `CommitScheduler.evaluate`:

- total pending tasks reach `commit_batch_size`;
- the `commit_batch_timeout_sec` deadline elapses; or
- `commit_all` or `close` sets the flush event.

The driver reads `len(self._pending)` (the `__len__` exposed by `PendingCommits`)
and passes it to `evaluate`; the scheduler decides whether `take_ready()` should
be called.

## Per-partition prefix extraction

`PendingCommits.take_ready()` performs extraction and per-consumer offset
mapping in a single synchronous call before any commit I/O. Internally it calls
`extract_ready_prefixes` (in `_pending_state`), which sorts each partition's
pending tasks by offset, tolerating re-queued tasks that land out of order, and
walks the sorted list stopping at the first not-done task — only the leading
contiguous run of finished tasks is eligible.

A **cancelled** task is a hard boundary. The cancelled task and everything after
it on that partition is dropped from pending, and `map_offsets_per_partition`
(also in `_pending_state`) stops the offset advance at the cancelled task. Those
offsets stay uncommitted and get redelivered on restart (at-least-once).

## Committing

`take_ready()` groups ready tasks by consumer-id and applies the cancellation
watermark floor per `(consumer-id, TopicPartition)` key, returning a list of
`ReadyCommit(consumer, offsets, tasks)`. Each group then commits via
`consumer.commit({TopicPartition: max_offset + 1})` — Kafka commits the *next*
offset to read, hence the `+ 1`.

On partition revocation, the rebalance listener calls
`committer.clear_cancellation_watermarks(partitions)`, which is a thin
delegator to `PendingCommits.clear_watermarks(partitions)`. Clearing watermarks
lets the next assignment of those partitions start fresh with no inherited
"do not advance" floor.

## Error policy

- A transient `KafkaError` re-queues the batch for a later attempt.
- `CommitFailedError` / `IllegalStateError` (the consumer no longer owns the
  partition — rebalance or revocation) discards the batch; those offsets will be
  re-driven by whoever now owns the partition.
- If the committer's own main task dies, callers of `send_task` receive
  `CommitterIsDeadError`, which propagates up and triggers `handler.stop()`.

## Backpressure

`max_uncommitted_tasks` (default 10000; `None` disables) caps the number of
tasks admitted but not yet committed or dropped. `send_task` admits a task while
under the ceiling and blocks once it is reached, so the consume path stalls and
the consumer stops fetching new records. The count is released when a task is
finally committed or dropped. Hitting the ceiling also nudges a flush so the
backlog drains. Keep `max_uncommitted_tasks >= commit_batch_size`, otherwise the
ceiling can be reached before a full batch ever accumulates.
