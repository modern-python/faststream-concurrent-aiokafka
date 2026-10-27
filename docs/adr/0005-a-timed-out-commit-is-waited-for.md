# A timed-out commit is waited for, not cancelled; a commit stall is not a liveness failure

ADR-0004's hold-and-backoff path never ran during a broker outage. aiokafka's `consumer.commit()`
retries every retriable error inside its own loop and blocks in `ensure_coordinator_known` until a
coordinator answers, so in a full-cluster outage the call simply did not return. The committer sat
inside it, `is_kafka_handler_healthy` stayed `True`, and `close()` hit `shutdown_timeout_sec`. Each
call is now bounded by `commit_timeout_sec`, and a call that runs past it is a transient error like
any other: the batch is held and retried with backoff. The obvious bound, `asyncio.wait_for`, was
rejected: cancelling `commit()` releases aiokafka's commit lock while the shielded request keeps
running, so the retry can overtake it and the late request moves the committed offset backwards
(pinned by `tests/test_aiokafka_commit_contract.py`). The committer instead keeps a timed-out call
running as that consumer's outstanding commit and sends nothing new for the consumer until it
finishes, so at most one commit per consumer is in flight and commit order is what it was before.
The failed streak also drives a separate signal, `is_kafka_commit_stalled`, rather than flipping
`is_kafka_handler_healthy`: a stall means the cluster is unreachable, a liveness restart cannot fix
that, and every restart adds a rebalance once the brokers return. Reopen the outstanding-commit
bookkeeping if the contract test starts failing, since that means aiokafka no longer releases the
lock on cancellation.
