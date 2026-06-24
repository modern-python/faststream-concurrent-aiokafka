# Deferred Work

Items raised in reviews or audits that are real but not actionable now.
Each is parked here with the reason it's deferred and the concrete trigger
that should bring it back. This is the long-tail register — not a backlog
of planned work. When an item is picked up it graduates to a change bundle
in [`changes/active/`](changes/active/); see [CLAUDE.md](../CLAUDE.md#workflow).

## Open

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
