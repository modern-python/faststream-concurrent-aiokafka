# Only `AckPolicy.MANUAL`, non-batch subscribers are managed

`KafkaConcurrentProcessingMiddleware` is registered once at broker level across a mix of
subscribers, so `_classify` routes everything it does not manage straight through: a `FakeConsumer`
under `TestKafkaBroker`, `ACK_FIRST` (recognised by its already-acked `committed` status), and
`ACK`, `REJECT_ON_ERROR` and `NACK_ON_ERROR`, which are message-shape-identical to `MANUAL` but get
FastStream's own `AcknowledgementMiddleware` and so would be acked the instant `consume_scope`
returns, ahead of the in-flight task. Refusing those policies was built and rejected: it turns a
legitimate mixed-subscriber application into a hard error. Hence pass-through precedes every
refusal in the branch order, and `batch=True` is refused only under `MANUAL`, where a tuple of
records has no single offset and supporting it would mean a second dispatch and commit engine
beside the per-message one. An undetermined policy is never assumed non-`MANUAL`: guessing towards
pass-through silently disables concurrency, guessing towards `MANUAL` fails loudly.
