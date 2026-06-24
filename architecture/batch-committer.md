# Batch committer

`KafkaBatchCommitter` (`batch_committer.py`) owns all offset commits. It runs as
a background asyncio task launched via `spawn()`. Its streaming loop continuously
absorbs `KafkaCommitTask`s from an internal queue and commits each partition's
contiguous-done prefix as offsets become eligible.

Per-partition pending state, the pending count, and cancellation watermarks are
owned by a synchronous, I/O-free `PendingCommits` object (`_pending_state.py`).
The committer keeps the queue, the backpressure ceiling
(`max_uncommitted_tasks`), the `consumer.commit()` I/O, and the
transient-`KafkaError` re-queue logic.

## Commit triggers

A partition's ready prefix is committed when any of these fires:

- total pending tasks reach `commit_batch_size`;
- the `commit_batch_timeout_sec` deadline elapses; or
- `commit_all` or `close` sets the flush event.

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

The trigger decision (whether to call `take_ready()` at all) stays in the
committer's loop, which reads `len(self._pending)` — the `__len__` exposed by
`PendingCommits`.

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
