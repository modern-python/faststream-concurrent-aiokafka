# Architecture

The living truth about what `faststream-concurrent-aiokafka` does **now** — one
file per capability, updated by hand whenever a change ships. The *why* and *how
it got here* live in [`../planning/changes/`](../planning/changes/), and
decisions deliberately taken (including options rejected) in
[`../planning/decisions/`](../planning/decisions/); this directory is the present.

Each capability file is an **implementation-detail** page. Its terse
**invariant summary** ("what must not break") lives in
[`../CLAUDE.md`](../CLAUDE.md) § Architecture.

These files carry **no frontmatter** — they are prose, dated by git.

## Capabilities

- [concurrent-handler.md](concurrent-handler.md) — `KafkaConcurrentHandler`, the
  core engine: semaphore-bounded concurrency, task tracking, fire-and-forget
  dispatch, and bounded shutdown.
- [batch-committer.md](batch-committer.md) — `KafkaBatchCommitter`, the
  background offset-commit task: per-partition contiguous-done prefixes,
  cancelled-task boundaries, and at-least-once redelivery.
- [middleware-lifecycle.md](middleware-lifecycle.md) — the FastStream
  middleware, `initialize_concurrent_processing` /
  `stop_concurrent_processing`, and the healthcheck.
- [rebalance.md](rebalance.md) — the `ConsumerRebalanceListener` that flushes
  offsets on partition revocation.
- [integration-tests.md](integration-tests.md) — the real-broker (Redpanda)
  test harness and its load-bearing FastStream/aiokafka driving invariants.

## Promotion rule

Shipping a change hand-edits the affected capability file(s) here to match the
new reality, in the same PR as the code. The change file stays in place under
[`../planning/changes/`](../planning/changes/) — no folder move.
