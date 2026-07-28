# Deferred Work

Items raised in reviews or audits that are real but not actionable now.
Each is parked here with the reason it's deferred and the concrete trigger
that should bring it back. This is the long-tail register — not a backlog
of planned work. When an item is picked up it graduates to a change file
in [`changes/active/`](changes/active/); see [CLAUDE.md](../CLAUDE.md#workflow).

## Open

### Direct msg.ack() / msg.nack() calls from an inner middleware

_Raised 2026-07-28 (same investigation)._

A middleware that calls the message methods *directly* reaches aiokafka without
passing through the committer.
`KafkaAckableMessage.ack()` issues a bare `consumer.commit()` with no offsets
(`faststream/kafka/message.py:70`), committing the consumer's **current fetch
position** — past every in-flight task on every assigned partition, which is
silent message loss. `nack()` issues `consumer.seek(partition, offset)`
(`kafka/message.py:78-91`), rewinding the fetch position under tasks already
processing that partition — a redelivery storm that can pin a CPU indefinitely.
Both defeat the at-least-once control the library exists to provide.

**Deferred because** the only robust guard is wrapping the `StreamMessage`
handed to inner middleware so `ack`/`nack`/`reject` raise a clear error, which
is a public-behavior change deserving its own design — and because no such
usage has been observed, only found by reading.

**Documented meanwhile** in the README's Limitations section, so users are told
not to do this even though nothing enforces it.

**Trigger:** a report of unexplained message loss, or of duplicate-delivery
storms, in an app whose middleware touches `msg.ack()` / `msg.nack()`.

### commit_all flush latency on a transient-error re-queue

_Raised 2026-06-24 (PendingCommits / CommitScheduler reviews)._

When a commit hits a transient `KafkaError`, `_call_committer`
(`batch_committer.py`) re-queues the batch and the loop's
`note_committed(pending_empty=True)` clears `flush_in_progress`. The re-queued
items stay tracked (so `commit_all`'s `messages_queue.join()` keeps waiting — no
lost work) but lose flush urgency: they re-commit only on the next
`commit_batch_timeout_sec`. With the default
`commit_batch_timeout_sec == flush_timeout_sec == 10.0`, `commit_all` can time
out just as they would have committed — needlessly redelivering those offsets
(duplicate processing) after a rebalance, which the flush exists to prevent.

**Deferred because** the obvious fix (keep flush urgency across the re-queue)
regresses the sustained-outage case into a busy-retry + log-spam loop:
`flush_in_progress` commits every iteration and a re-queued item makes
`queue_get` fire immediately, so urgency-across-retries means
retry-every-iteration (one prompt retry for a blip, a hot loop during an outage,
bounded only by `flush_timeout_sec`). A correct fix must distinguish a blip from
an outage — retry-with-backoff on the flush path — a real feature, not a
one-liner, and not justified by the niche impact (transient commit error
coinciding with a rebalance, on a sub-`commit_batch_size` batch). Operators can
mitigate today with `commit_batch_timeout_sec < flush_timeout_sec`.

**Trigger:** a report of the `commit_all flush timed out` warning or duplicate
processing after a rebalance under transient commit failures; or a decision to
add retry-backoff to the committer for any reason (fold this in).
