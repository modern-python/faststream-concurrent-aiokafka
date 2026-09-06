# Non-`MANUAL` ack policies pass through, they are not refused

**Decision:** A subscriber whose declared `ack_policy` is anything other than `AckPolicy.MANUAL`
gets `_PassThrough` — processed exactly as if this middleware were not registered. It is never
refused, and never refused for any *other* misconfiguration either.

## Context

`_classify` originally treated `kafka_message.committed is not None` as "auto-ack subscriber, pass
it through", assuming the complement meant `MANUAL`. It does not. FastStream picks the non-ackable
`KafkaMessage` (which starts `committed=AckStatus.ACKED`) only for `ACK_FIRST`; `ACK`,
`REJECT_ON_ERROR` and `NACK_ON_ERROR` all get `KafkaAckableMessage` with `committed=None`, exactly
like `MANUAL` — while `auto_ack_disabled` covers only `{MANUAL, ACK_FIRST}`, so those three *do*
get FastStream's own `AcknowledgementMiddleware`. Three policies were therefore being dispatched as
background tasks *and* acked by FastStream the instant `consume_scope` returned: a bare
`consumer.commit()` ahead of every in-flight task on every assigned partition. Fixing that meant
detecting them; what to *do* with them is this decision.

## Decision & rationale

**Refusing them was built and rejected.** The first cut raised a `RuntimeError`, on the theory that
silently dropping concurrency for a subscriber the user asked to process concurrently is worse than
a loud error. That reasoning ignores the registration model this middleware documents: it is added
**once at broker level** across a mix of subscribers, most of which are not `MANUAL` and are
expected to behave as if it were absent. Refusing turns a legitimate mixed-subscriber application
into a hard error on its `ACK` subscribers — punishing the user for the middleware's own breadth.

Pass-through is safe for the same reason dispatch was unsafe: each FastStream subscriber builds its
own `AIOKafkaConsumer`, so a passed-through subscriber's ack touches only its own consumer's
partitions and cannot commit past another subscriber's in-flight work. With no background task,
"consumed" and "processed" are the same moment, as in stock FastStream. The hazard existed only
because those subscribers were being dispatched.

**The branch order is part of the decision.** The pass-through sits immediately after the
`committed is not None` branch and *before* every refusal, so a non-`MANUAL` subscriber is not
rejected for being a batch subscriber, for a missing `initialize_concurrent_processing`, or for
anything else — none of it is this library's business. `batch=True` on `AckPolicy.ACK` therefore
passes through cleanly, while `batch=True` plus `MANUAL` is still refused
([ADR-0001](0001-batch-subscribers-unsupported.md)).

A policy that cannot be determined — no subscriber in context, or no `ack_policy` attribute — is
`None`, and `None` is never *assumed* to be non-`MANUAL`. Undetermined does not pass through on
that basis; it falls to the `MANUAL` path and is refused or dispatched on its other signals. The
asymmetry is deliberate: guessing wrong towards pass-through silently disables concurrency, while
guessing wrong towards the `MANUAL` path fails loudly.

**Revisit trigger:** FastStream stops giving each subscriber its own `AIOKafkaConsumer` (a shared
consumer would make a passed-through ack able to commit past another subscriber's in-flight work,
and pass-through would no longer be safe), **or** a supported way appears to run a concurrent
handler under a policy FastStream acknowledges itself, which would make refusal unnecessary rather
than merely hostile.
