# A non-retriable commit error is retried like any other failed commit

aiokafka's `commit_offsets` loops on every retriable error itself, so apart from the
`KafkaTimeoutError` from ADR-0005, every `KafkaError` that reaches the committer is one aiokafka marks
non-retriable. In aiokafka 0.14 that means `GroupAuthorizationFailedError` or
`TopicAuthorizationFailedError`. `OffsetMetadataTooLargeError` and `InvalidCommitOffsetSizeError`
concern commit metadata, which this package never sends. ADR-0004's hold-and-backoff retries these
errors anyway, and it keeps doing so. The batch stays pending and `is_kafka_commit_stalled` reports
the stall, because an authorization failure clears once an operator fixes the ACLs, and the next
retry then commits without a restart. Stopping the committer was rejected: `is_kafka_handler_healthy`
would turn `False`, and a liveness restart cannot fix ACLs, so the pod would crash-loop and add a
rebalance per restart, the cost ADR-0005 already declined for stalls. Dropping the batch the way
`CommitFailedError` is dropped was rejected too: the next round's success would reset the stall
clock, so commits would stop advancing with nothing reporting it, and a restart would redeliver
everything since. The decision changes only the log. The first non-retriable error in a failed
streak is logged at ERROR with its own message saying it needs fixing, even when the streak began
with a timeout, so one ERROR event names the cause. An error counts as retriable when aiokafka marks
it so or when it is the committer's own `KafkaTimeoutError`, which keeps a retriable error that a
future aiokafka lets through logging as a transient one.
