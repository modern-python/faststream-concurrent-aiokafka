# Batch committer

`KafkaBatchCommitter` (`batch_committer.py`) owns all offset commits. It runs as
a background asyncio task launched via `spawn()`. Its streaming loop continuously
absorbs `KafkaCommitTask`s from an internal queue into per-partition pending
state and commits each partition's contiguous-done prefix as offsets become
eligible.

## Commit triggers

A partition's ready prefix is committed when any of these fires:

- total pending tasks reach `commit_batch_size`;
- the `commit_batch_timeout_sec` deadline elapses; or
- `commit_all` or `close` sets the flush event.

## Per-partition prefix extraction

For each partition, `_extract_ready_prefixes` sorts the pending tasks by offset.
Sorting tolerates re-queued tasks that land out of order. It walks the sorted
list and stops at the first not-done task — only the leading contiguous run of
finished tasks is eligible.

A **cancelled** task is a hard boundary. The cancelled task and everything after
it on that partition is dropped from pending, and `_map_offsets_per_partition`
stops the offset advance at the cancelled task. Those offsets stay uncommitted
and get redelivered on restart (at-least-once).

## Committing

Tasks are grouped per consumer-id, and each group commits via
`consumer.commit({TopicPartition: max_offset + 1})` — Kafka commits the *next*
offset to read, hence the `+ 1`.

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
