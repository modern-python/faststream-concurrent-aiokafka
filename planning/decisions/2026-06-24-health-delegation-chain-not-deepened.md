---
status: accepted
date: 2026-06-24
slug: health-delegation-chain-not-deepened
summary: Leave the is_kafka_handler_healthy → handler.is_healthy → committer.is_healthy chain as-is; not worth deepening yet.
supersedes: null
superseded_by: null
pr: null
---

# Health delegation chain left as-is (architecture candidate #4)

**Decision:** Do not deepen the three-hop health check
(`is_kafka_handler_healthy(context)` → `handler.is_healthy` →
`committer.is_healthy`) into a single module/object yet.

## Context

The `/improve-codebase-architecture` review surfaced this as candidate #4
("Speculative"): answering "is the processor alive?" delegates through three
one-line reads across `healthcheck.py`, `processing.py`, and `batch_committer.py`.
The deletion test on `healthcheck.py` only moves two trivial lines to callers.
Candidates #1–#3 from the same review (PendingCommits, CommitScheduler,
`_classify`) shipped; this one was explicitly deferred.

## Decision & rationale

The chain is short, cheap, and each hop is a meaningful boundary
(`is_kafka_handler_healthy` is the public probe surface; `handler.is_healthy`
composes running-state with committer liveness; `committer.is_healthy` is the
committer's own fact). There is no friction to relieve and nothing varies across
the seam — collapsing it would trade a clear, readable delegation for a module
that exists only to merge three booleans. The review itself rated it "leave it
until the health concept grows."

## Revisit trigger

Reopen when the health concept actually grows — e.g. health gains a second
dimension beyond a single boolean (queue depth, last-commit age, lag), or a
readiness-vs-liveness split is needed. At that point a single `health()` verdict
object earns the seam.
