# The commit scheduler decides; the async loop stays in the driver

Splitting the committer into `PendingCommits` (what to commit) and `CommitScheduler` (when) leaves
an obvious-looking third move: hand the scheduler the `asyncio.wait` select and the three
wait-tasks as well, so one module owns the streaming cycle end to end. It was proposed and rejected
twice, and will be proposed again, because a decider that cannot run itself reads like an
unfinished extraction. The decision state carries invariants relating `timeout_deadline`,
`should_shutdown` and `flush_in_progress` to each other and to pending depth; the wait-tasks carry
none, so moving them buys nothing and costs the whole return on the split. Because the scheduler
reads no clock and touches no asyncio object, with the driver passing `now = loop.time()` in, its
entire decision surface is testable with no event loop, no fake clock and no wait-task doubles.
Reopen if a wait-task acquires an invariant of its own.
