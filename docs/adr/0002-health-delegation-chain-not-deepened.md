# Health delegation chain left as-is

**Decision:** Do not deepen the three-hop health check (`is_kafka_handler_healthy(context)` →
`handler.is_healthy` → `committer.is_healthy`) into a single module or object.

## Context

An architecture review surfaced this as a "speculative" candidate: answering "is the processor
alive?" delegates through three one-line reads across `healthcheck.py`, `processing.py`, and
`batch_committer.py`. The deletion test on `healthcheck.py` only moves two trivial lines to
callers. The three sibling candidates from the same review — `PendingCommits`, `CommitScheduler`,
and `_classify` — shipped in 0.6.1; this one was explicitly deferred.

## Decision & rationale

The chain is short, cheap, and each hop is a meaningful boundary: `is_kafka_handler_healthy` is
the public probe surface, `handler.is_healthy` composes running-state with committer liveness, and
`committer.is_healthy` is the committer's own fact. There is no friction to relieve and nothing
varies across the seam — collapsing it would trade a clear, readable delegation for a module that
exists only to merge three booleans.

**Revisit trigger:** the health concept actually grows — health gains a second dimension beyond a
single boolean (queue depth, last-commit age, lag), or a readiness-vs-liveness split is needed. At
that point a single `health()` verdict object earns the seam.
