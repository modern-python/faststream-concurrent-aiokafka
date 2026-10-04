# A commit that hits a transient error stays pending; it is never re-queued

When `consumer.commit()` raises a transient `KafkaError`, the committer used to put the batch's
tasks back onto its intake queue. That is the obvious retry, and it caused two opposite failures
during a flush. If the re-queue emptied pending work, the flush ended, and the re-absorbed tasks
waited a full batch timeout, so with the default `commit_batch_timeout_sec == flush_timeout_sec`
the rebalance flush timed out and the offsets were redelivered. If other partitions still had
work in flight, the flush stayed open and every re-put woke the loop to commit again: a hot retry
loop with a traceback per attempt, outliving the `commit_all` that opened it. The failed
`ReadyCommit` is now held inside `PendingCommits` and merged into the next `take_ready()`, so
pending work never looks drained while a commit is outstanding, and nothing re-enters the queue
to wake the loop; retries are spaced by a capped exponential backoff owned by the
`CommitScheduler`. The rejected alternative was to keep re-queuing and add a retry-not-before
guard: it patches both symptoms but leaves the root cause, the queue carrying work that was
already absorbed, in place.
