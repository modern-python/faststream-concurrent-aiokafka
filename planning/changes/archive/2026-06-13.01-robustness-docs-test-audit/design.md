---
status: shipped
date: 2026-06-13
slug: robustness-docs-test-audit
supersedes: null
superseded_by: null
pr: "32"
outcome: "merged as #32; shipped in 0.6.0"
---

# Robustness, docs, and test audit — design

**Date:** 2026-06-13
**Status:** Approved for planning
**Scope:** Behavioral fixes (#1–#3), docs fixes (#4–#7), test fixes (#8–#9),
refactor/cleanup (#10–#12) surfaced by a deep read of code, tests, and docs.

## Background

A full read of the seven source modules, eight test modules, README, and
`CLAUDE.md` surfaced three production-impacting behavioral risks plus a set of
documentation drift, test-quality, and cleanup issues. This spec records the
agreed approach for each. Items are largely independent; the only ordering
constraint is that the `batch_committer.py` refactor (#10) lands last, on top of
green tests for the behavioral fixes.

The library's core contract is unchanged: **at-least-once** delivery, offsets
committed only after the user task completes, MANUAL ack required on concurrent
subscribers.

---

## Behavioral fixes

### #1 — `commit_all()` can hang a rebalance indefinitely

**Problem.** `KafkaBatchCommitter.commit_all()` (`batch_committer.py:361`) sets the
flush event then `await self._messages_queue.join()`. `Queue.join()` returns only
once `task_done()` has fired for every enqueued task, and `task_done()` is called
only when a task becomes *done* and is committed (`_commit_partitions`). So
`commit_all()` blocks until **every in-flight handler across all partitions**
finishes — with **no timeout**.

It is called synchronously from `ConsumerRebalanceListener.on_partitions_revoked`
(`rebalance.py:42`), which aiokafka awaits inside the consumer's group-coordination
path against the broker's rebalance clock (`max.poll.interval.ms` /
`rebalance.timeout.ms`). A single slow or hung handler overruns that budget → the
member is evicted → cascading rebalance storm. The wait is also over-broad: it
blocks on all partitions, not just the revoked ones.

**Decision: A-lite (bounded global wait) now; partition-scoping (full A) as a
documented follow-up.**

- Add a `flush_timeout_sec` parameter to `commit_all()`.
- Wrap the `messages_queue.join()` in `asyncio.wait_for(..., timeout=flush_timeout_sec)`.
- On `TimeoutError`: log a warning naming the timeout, and return. Offsets for
  already-completed tasks are committed; anything still in-flight stays
  uncommitted and is redelivered after reassignment (at-least-once — duplicates
  only on the timeout path).
- The flush event stays cleared correctly on both the normal and timeout paths so
  subsequent `commit_all`/`close` triggers still work (preserve the invariant
  exercised by `test_streaming_clears_flush_event_after_commit_all`).

**Default.** `flush_timeout_sec` default: **10.0 s** — comfortably under aiokafka's
default `max.poll.interval.ms` (300 s) while leaving room for normal handler
completion. Plumb it through the rebalance listener so callers can tune it; the
listener passes its configured value to `commit_all`.

**Wiring.** `ConsumerRebalanceListener` gains an optional `flush_timeout_sec`
(defaulting to the same constant) and forwards it to `committer.commit_all()`.
`KafkaConcurrentHandler.create_rebalance_listener()` accepts and forwards it so the
knob is reachable from user code.

**Follow-up (not in this spec): full Option A.** Scope the flush/wait to only the
revoked partitions using per-partition pending-count tracking + an event/condition
(cannot reuse the global `Queue.join()`). Pursue only if rebalance-time duplicate
volume proves to be a real problem in practice.

**Tests.** See #9(a).

### #2 — Batch subscribers crash with an opaque error

**Problem.** `consume_scope` (`middleware.py:100`) does
`typing.cast("ConsumerRecord", self.msg)` and `handle_task` then reads
`record.offset / topic / partition`. With `@broker.subscriber(batch=True,
ack_policy=AckPolicy.MANUAL)`, `self.msg` is a *tuple* of `ConsumerRecord`s, so the
attribute access raises a bare `AttributeError` deep in the call stack rather than a
clear "unsupported" message. Nothing in code or docs addresses batch subscribers.

**Decision: reject loudly and early.** Supporting batch mode is a larger feature and
is **out of scope**. We only stop the silent crash.

- In `consume_scope`, after the FakeConsumer and non-MANUAL pass-through checks and
  before the cast, detect a batch raw message via `isinstance(self.msg, (list, tuple))`.
- Raise a clear `RuntimeError`:
  > "KafkaConcurrentProcessingMiddleware does not support batch subscribers
  > (`batch=True`). Use a non-batch subscriber, or remove the middleware from this
  > subscriber."
- Placement: the check must come *after* the FakeConsumer short-circuit (so
  `TestKafkaBroker` is unaffected) and after the non-MANUAL pass-through (so an
  auto-ack batch subscriber still passes through untouched). It applies only on the
  MANUAL-ack concurrent path.

**Tests.** See #9(b).

### #3 — Unbounded memory growth during a commit outage

**Problem.** `_messages_queue` has no `maxsize`. On transient `KafkaError`,
`_call_committer` re-queues the batch (`batch_committer.py:144`). The semaphore caps
only *concurrent in-flight* tasks; completed-but-uncommitted tasks accumulate in
`pending` (and re-queued failures bounce through the queue) without bound while the
broker rejects commits. A sustained commit/broker outage grows memory until OOM.
There is no backpressure linking commit lag to consumption.

**Decision: opt-in backpressure via a configurable ceiling.**

- Add `max_uncommitted_tasks` config to `KafkaBatchCommitter` (threaded through
  `initialize_concurrent_processing`).
- Semantics: the number of tasks that have been accepted by the committer but not
  yet committed — i.e. items currently in `_messages_queue` plus all tasks in
  `pending`. When this count is at/above the ceiling, `send_task` blocks (awaits)
  before enqueuing, so the consume path (`handle_task` → `consume_scope`) stops,
  the semaphore slot is held, and the consumer stops fetching until commits catch
  up. Backpressure propagates naturally through the existing semaphore + poll loop.
- Implementation note: maintain an accurate counter as items are enqueued and as
  they are committed (`_commit_partitions` already iterates the committed/dropped
  set and can decrement). Re-queued batches on transient `KafkaError` must not be
  double-counted — a re-queue is a move within the uncommitted set, not a new
  admission, so the counter is unchanged across a re-queue. Wake blocked
  `send_task` waiters when the count drops below the ceiling (an `asyncio.Condition`
  or a simple event re-checked in a loop).
- The blocking wait must remain responsive to shutdown: if the committer is
  closing/dead, `send_task`'s existing `_check_is_commit_task_running()` guard and
  the `CommitterIsDeadError` path must still fire rather than block forever.

**Default.** `max_uncommitted_tasks` default: **10_000**. `None` opts out
(unbounded — the current behavior) for back-compat. Document the trade-off:
bounding caps memory but means a stuck committer eventually stalls consumption
(by design — fail-stop beats OOM).

**Tests.** See #9(c).

---

## Documentation fixes

### #4 — Stale README §Core Concepts → KafkaConcurrentHandler

`README.md:88–95` claims in-flight tracking via "a counter + `asyncio.Event` …
sets the event when it reaches zero" and lists "**Signal handlers for graceful
shutdown**." The code uses a `set[asyncio.Task]` (`_tracked_tasks`) and installs
**no** signal handlers — contradicting `README.md:18` and the Migration table.

**Fix.** Rewrite the bullet list to match reality:
- `asyncio.Semaphore` enforcing `concurrency_limit`.
- In-flight task tracking via `set[asyncio.Task]`; each task's done-callback
  (`_finish_task`) releases the semaphore, removes the task from the set, and logs
  any non-cancellation exception.
- A `KafkaBatchCommitter` for offset commits.
- An optional `ConsumerRebalanceListener` via `handler.create_rebalance_listener()`.
- Remove the "Signal handlers" line entirely (the lib does not own signals).

### #5 — Missing LICENSE file

`README.md:24` links `[License](LICENSE)` and `pyproject.toml` declares
`license = "MIT"`, but no `LICENSE` file is shipped — a broken link and a
license-compliance gap on PyPI.

**Fix.** Add a top-level `LICENSE` file with the standard MIT text. Copyright line:
`Copyright (c) 2026 Artur Shiriev` (matches the pyproject author). Confirm the build
backend includes it in the sdist/wheel.

### #6 — Rebalance flush cost undocumented

The README ("prevents duplicate processing") and `rebalance.py` docstring are only
true because `commit_all` blocks. Document the cost and the new knob.

**Fix.** In the README "How It Works" rebalance step and the `commit_all` /
`ConsumerRebalanceListener` docstrings, note that the revoke flush waits for
in-flight handlers up to `flush_timeout_sec`, and that on timeout the remaining
in-flight messages are redelivered after reassignment (at-least-once). Mention
partition-scoping as a future optimization.

### #7 — "observer task" wording

`is_kafka_handler_healthy` docs / README:123 say "observer task dead." There is no
observer task; health reflects the **committer task**.

**Fix.** Replace "observer task" with "committer task" in the README API reference
and any matching docstring.

---

## Test fixes

### #8 — Misleading graceful-shutdown test

`test_real_kafka_graceful_shutdown_waits_for_tasks` (`test_integration.py:266`)
asserts shutdown "waits for in-flight tasks," but `stop()` now **cancels** them. It
passes only because `POLL_SLEEP` (3 s) lets the 0.5 s handlers finish before
`stop()` runs — it never exercises cancellation and contradicts documented behavior.

**Fix.** Rewrite and rename (e.g. `test_real_kafka_shutdown_cancels_in_flight_tasks`):
- Publish messages whose handlers sleep well beyond the test window.
- Call `stop_concurrent_processing` *while* tasks are genuinely in-flight (no long
  pre-sleep that lets them complete).
- Assert in-flight handlers are cancelled (did not reach completion) and that, on a
  restart with the same group id, the uncommitted messages are redelivered
  (at-least-once) — mirroring the structure of
  `test_real_kafka_multi_subscriber_commits_all_offsets`'s two-phase replay check.

### #9 — Coverage gaps for the new behavior

Add tests alongside each behavioral fix:

- **(a) Rebalance flush timeout (#1).** With a handler that hangs, assert
  `commit_all(flush_timeout_sec=<small>)` returns within the bound (does not block
  on the hung task) and logs the timeout warning. Unit-level against
  `KafkaBatchCommitter` with a non-completing task is sufficient; no real broker
  needed.
- **(b) Batch-subscriber rejection (#2).** Drive `consume_scope` with a
  list/tuple `self.msg` on the MANUAL-ack path and assert the clear `RuntimeError`
  is raised; assert FakeConsumer and non-MANUAL paths are unaffected.
- **(c) Backpressure (#3).** With `max_uncommitted_tasks` set small and commits
  stalled, assert `send_task` blocks once the ceiling is reached and unblocks after
  commits drain the uncommitted count below the ceiling. Assert `None` preserves
  unbounded behavior.

All new unit tests follow existing conventions: `MockAIOKafkaConsumer` /
`MockAsyncioTask` / real `asyncio` tasks as appropriate, `typing.Final`, no
`from __future__ import annotations`, `# ty: ignore[...]` for the mock-typed args.

---

## Refactor / cleanup

### #10 — Split `batch_committer.py`

`batch_committer.py` (~430 lines) holds the whole streaming state machine — the
multi-event `asyncio.wait` loop, per-partition pending lists, contiguous-done prefix
extraction, cancellation watermarks, and offset mapping — in one unit. It is the
riskiest correctness surface.

**Direction.** Extract the *pending + watermark state* into a focused unit (working
name `_PendingState` / a small class) that owns: the per-partition pending lists,
`_insert_sorted`, `_extract_ready_prefixes`, `_map_offsets_per_partition`, the
cancellation watermarks, and `_partition_owner` — with its own unit tests. Leave
`KafkaBatchCommitter` as the thin loop driver (queue absorption, event handling,
commit dispatch, lifecycle). The uncommitted-count counter from #3 lives wherever it
can be maintained accurately (likely the new state unit, since it owns admission and
extraction).

**Constraint.** This lands **last**, after #1–#3 and #8–#9 are green, so the move is
a pure restructuring on top of passing tests. Pure mechanical extraction — no
behavior change; the existing committer test suite must pass unchanged (or with only
import/path updates).

### #11 — Orphaned bytecode

`faststream_concurrent_aiokafka/__pycache__/dead_letter_queue.cpython-314.pyc`
exists with no `dead_letter_queue.py` source — leftover from a removed DLQ feature.

**Fix.** Delete the orphaned `.pyc`, grep the tree for any dangling
`dead_letter_queue` references, and (if not already) ensure `__pycache__` is
git-ignored so stale artifacts don't reappear in the tree.

### #12 — Mixed imports in `processing.py`

`processing.py:8–9` mixes a module import (`from faststream_concurrent_aiokafka import
batch_committer, consts`) with a symbol import of the same module
(`from ...batch_committer import KafkaBatchCommitter`).

**Fix.** Pick one style consistently for `batch_committer` usage in this module
(module-qualified is fine since `batch_committer.KafkaCommitTask` /
`batch_committer.CommitterIsDeadError` are already used module-qualified). Keep all
imports at module level per project convention.

---

## Out of scope / explicit non-goals

- **Full batch-subscriber support.** #2 only rejects; supporting `batch=True` is a
  separate feature.
- **Partition-scoped rebalance flush (full Option A).** Documented follow-up to #1;
  not implemented here.
- **Re-introducing a dead-letter-queue feature.** #11 only removes leftover
  artifacts.

## Sequencing

1. #5, #7, #11, #12 — trivial, no behavior change (can land first/independently).
2. #2 + test #9(b) — small guard.
3. #1 + test #9(a) — rebalance timeout + wiring.
4. #3 + test #9(c) — backpressure.
5. #8 — rewrite the shutdown integration test.
6. #4, #6 — README updates reflecting the shipped behavior of #1/#3.
7. #10 — `batch_committer.py` extraction, last, on green tests.

## Defaults summary

| Knob | Default | Opt-out |
|---|---|---|
| `flush_timeout_sec` (rebalance flush) | `10.0` s | — |
| `max_uncommitted_tasks` (backpressure) | `10_000` | `None` (unbounded) |

New constants land in `consts.py` alongside the existing defaults.
