# The commit scheduler decides; the async loop stays in the driver

**Decision:** `CommitScheduler` is a synchronous, clock-free decider. The `asyncio.wait` select,
the three wait-tasks, and the `_run_commit_process` / `_streaming_iteration` orchestration stay in
`KafkaBatchCommitter`. Do not fold them into a `run()`-owning async `CommitLoop`.

## Context

Splitting `batch_committer.py` into "what to commit" (`PendingCommits`) and "when to commit"
(`CommitScheduler`) left an obvious-looking third move on the table: give the scheduler the loop as
well, so one module owns the whole streaming cycle end to end. It was proposed twice — once as the
follow-up the `PendingCommits` split "sets up but does not require", and again while designing the
scheduler — and rejected both times. It will be proposed a third time, because a decider that
cannot run itself reads like an unfinished extraction.

## Decision & rationale

The decision state carries invariants; the wait-tasks do not. `timeout_deadline`,
`should_shutdown` and `flush_in_progress` have rules relating them to each other and to pending
depth, and concentrating those in one synchronous object is what turns them from
documented-and-externally-maintained into enforced-in-one-place. `queue_get_task`,
`flush_wait_task` and `task_completed_wait_task` carry no such rule. Moving them buys nothing.

It costs the unit-test story, which is the whole return on the split. Because the scheduler reads
no clock and touches no asyncio object — the driver passes `now = loop.time()` in — its entire
decision surface is testable by feeding observation sequences with zero asyncio: no event loop, no
fake clock, no wait-task doubles. An async `CommitLoop` would force every one of those tests back
onto the event loop to assert a boolean.

The split is also what keeps the driver honest: the driver never writes a decision field, it only
feeds observations to `evaluate` and acts on the returned `Decision`. A module that owned both
sides could quietly reintroduce the write, and nothing structural would catch it.

**Revisit trigger:** a wait-task acquires a real invariant of its own — a rule relating one
wait-task's state to another's, or to the decision fields, that the driver currently maintains by
convention. That would put enforceable state on the async side of the seam, and the seam would be
in the wrong place.
