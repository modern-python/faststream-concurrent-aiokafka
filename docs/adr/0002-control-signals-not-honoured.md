# Control signals that cannot be honoured are absorbed and logged, not acted on

A middleware registered after `KafkaConcurrentProcessingMiddleware` runs inside the dispatched
`asyncio.Task`, and under `AckPolicy.MANUAL` FastStream builds no `AcknowledgementMiddleware`, so a
control signal raised there reaches nothing that could interpret it. `_absorb_control_signal`
absorbs the whole `IgnoredException` family rather than letting it end the task, which would pin
the traceback and the message body until the offset committed, surface as an unhandled error to
asyncio-aware reporters, and, for `StopApplication` as a `SystemExit` subclass, kill the
application with offsets in flight. `NackMessage`, `StopConsume` and `StopApplication` are absorbed
but not honoured, and log an ERROR naming the signal, because nack for Kafka means `consumer.seek`,
rewinding the partition underneath every other in-flight task, and the alternative of holding the
partition at the nacked offset reuses the cancellation watermark, which clears only on rebalance
and would stall every later offset. Both stop signals inherit the same unanswerable offset
question.
