---
status: accepted
date: 2026-06-24
slug: batch-subscribers-unsupported
summary: Batch subscribers (batch=True) are deliberately unsupported; the middleware rejects them with a clear RuntimeError.
supersedes: null
superseded_by: null
pr: null
---

# Batch subscribers (`batch=True`) are unsupported

**Decision:** The concurrent middleware does not support batch subscribers and
rejects them with an explicit `RuntimeError`, rather than growing batch handling.

## Context

The concurrent path is built around one message → one user task → one offset.
`_classify` / `consume_scope` cast the raw message to a single `ConsumerRecord`
and dispatch it via `handle_task`, which carries a single `(topic, partition,
offset)` to the committer. A `@broker.subscriber(batch=True, ack_policy=MANUAL)`
subscriber delivers a *tuple* of records, which has no single offset and does
not fit the per-message task/commit model. Earlier this crashed with a bare
`AttributeError` deep in dispatch; 0.6.0 added an explicit guard (now in
`_classify` → `_Refuse`) that raises a clear, actionable `RuntimeError`.

## Decision & rationale

Supporting batch mode would mean a parallel dispatch/commit path (a batch is N
records spanning offsets/partitions, with its own at-least-once and
backpressure semantics) — a substantial second engine for a use case the
library's concurrency model does not target. The per-message model is the whole
point: bounded concurrency over individual handlers with per-offset commit
control. The clear rejection is the right contract: it names the cause and tells
the user to use a non-batch subscriber or drop the middleware from that one.

## Revisit trigger

Reopen if there is real demand for concurrent processing *within* a batch
subscriber (e.g. fan-out over a batch's records under one MANUAL ack), with a
concrete at-least-once story for partial-batch failure.
