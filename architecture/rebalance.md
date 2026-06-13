# Rebalance listener

`ConsumerRebalanceListener` (`rebalance.py`) flushes pending offset commits when
Kafka revokes partitions during a rebalance. It is obtained from
`handler.create_rebalance_listener(flush_timeout_sec=...)` and passed to a
subscriber via `@broker.subscriber(..., listener=listener)`.

## Why it exists

Without this listener, in-flight tasks whose offsets have not yet been
batch-committed would be redelivered to another consumer after a rebalance,
causing duplicate processing. The listener closes that window by committing
completed offsets before the partitions move.

## On revocation

On `on_partitions_revoked`, the listener calls `committer.commit_all()` so the
completed offsets are flushed before the partition is reassigned. (It also clears
the cancellation watermarks for the revoked partitions, since the next
assignment starts fresh.)

The flush is bounded by `flush_timeout_sec` (default 10 s), which sits
comfortably under aiokafka's default `max.poll.interval.ms` of 300 s — so the
revoke callback never stalls the rebalance long enough to get the consumer
evicted from the group.

On timeout, `commit_all` logs a warning and returns. Already-completed offsets
have been committed; any still-in-flight tasks stay uncommitted and are
redelivered (at-least-once). Duplicate processing is therefore confined to the
timeout path — the healthy path commits everything before the partition moves.
