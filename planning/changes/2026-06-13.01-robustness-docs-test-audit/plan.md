---
status: shipped
date: 2026-06-13
slug: robustness-docs-test-audit
spec: robustness-docs-test-audit
pr: "32"
---

# Robustness, Docs, and Test Audit — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three production-impacting behavioral risks (rebalance-flush hang, batch-subscriber crash, unbounded memory growth) plus documentation drift, a misleading test, and code cleanup surfaced by a full audit of `faststream-concurrent-aiokafka`.

**Architecture:** Small, mostly-independent changes to an asyncio Kafka-offset-committing middleware. Behavioral fixes add a bounded timeout to the rebalance flush, an explicit batch-subscriber guard, and an opt-in backpressure ceiling on uncommitted tasks. A final pure-refactor extracts the committer's pending/watermark state into its own unit. The at-least-once contract is preserved throughout.

**Tech Stack:** Python ≥3.11, asyncio, FastStream, aiokafka, pytest + pytest-asyncio (`asyncio_mode=auto`), pytest-cov (**100% coverage enforced**), pytest-xdist (`-n auto`), ruff + ty.

---

## Conventions for every task

- **Imports at module level** (project rule — no local imports inside functions).
- **No `from __future__ import annotations`**; use `typing.Self`/`typing.Never` directly.
- **Type suppression** uses `# ty: ignore[rule-name]`, never `# type: ignore`.
- **Run a single test during TDD** (coverage gate disabled so the file-scoped run doesn't fail on <100%):
  `uv run --no-sync pytest tests/test_X.py::test_name -p no:cacheprovider --no-cov -v`
- **Validate the full unit suite + coverage** before each commit that touches `faststream_concurrent_aiokafka/`:
  `uv run --no-sync pytest tests/ --ignore=tests/test_integration.py`
  (Integration tests need a real broker; run them via `just test` only where a task says so.)
- **Lint before each commit:** `just lint`
- **Commit messages** end with the project's `Co-Authored-By` trailer (see existing history).

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `faststream_concurrent_aiokafka/consts.py` | Default constants | 3, 5 |
| `faststream_concurrent_aiokafka/batch_committer.py` | Committer loop, flush timeout, backpressure | 3, 5 |
| `faststream_concurrent_aiokafka/rebalance.py` | Rebalance listener; forwards flush timeout | 3 |
| `faststream_concurrent_aiokafka/processing.py` | Handler; forwards flush timeout; import cleanup | 3, 8 |
| `faststream_concurrent_aiokafka/middleware.py` | Batch guard; threads backpressure config | 2, 5 |
| `LICENSE` (new) | MIT license text | 1 |
| `README.md` | Docs corrections | 6 |
| `tests/test_middleware.py` | Batch-guard + backpressure-wiring tests | 2, 5 |
| `tests/test_kafka_committer.py` | Flush-timeout + backpressure-unit tests | 3, 5 |
| `tests/test_rebalance.py` | Flush-timeout forwarding test | 3 |
| `tests/test_integration.py` | Rewritten shutdown-cancels test | 7 |
| `faststream_concurrent_aiokafka/_pending_state.py` (new) | Extracted pending/watermark state | 9 |

---

## Task 1: Trivial cleanups (LICENSE, wording, orphaned pyc, imports)

**Files:**
- Create: `LICENSE`
- Modify: `README.md` (license link already present at line 24; API-reference wording near line 123)
- Modify: `faststream_concurrent_aiokafka/processing.py:8-9`
- Delete (local artifact): `faststream_concurrent_aiokafka/__pycache__/dead_letter_queue.cpython-314.pyc`

These have no runtime behavior and no new test code.

- [ ] **Step 1: Add the MIT LICENSE file (#5)**

Create `LICENSE` with the standard MIT text:

```text
MIT License

Copyright (c) 2026 Artur Shiriev

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

- [ ] **Step 2: Fix the "observer task" wording (#7)**

In `README.md`, the `is_kafka_handler_healthy` entry (around line 123) reads:
`False otherwise (not initialized, stopped, or observer task dead)`.
Replace `observer task dead` with `committer task dead`:

```markdown
### `is_kafka_handler_healthy(context)`

Returns `True` if the `KafkaConcurrentHandler` stored in `context` is running and healthy, `False` otherwise (not initialized, stopped, or committer task dead). Useful for readiness/liveness probes.
```

- [ ] **Step 3: Unify imports in `processing.py` (#12)**

`processing.py:8-9` currently is:

```python
from faststream_concurrent_aiokafka import batch_committer, consts
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter
```

`KafkaBatchCommitter` is used as a bare name in the type annotation/`__init__`; `batch_committer.KafkaCommitTask` and `batch_committer.CommitterIsDeadError` are used module-qualified. Keep both forms but order them per the project's isort config (already correct). No change is strictly required here unless ruff flags it — run `just lint` and accept its auto-fix. If ruff leaves it unchanged, leave lines 8-9 as-is (they are already consistent: one module import for qualified use, one symbol import for the annotated constructor arg). **This step is satisfied once `just lint` is clean.**

- [ ] **Step 4: Delete the orphaned bytecode (#11)**

```bash
rm -f faststream_concurrent_aiokafka/__pycache__/dead_letter_queue.cpython-314.pyc
grep -rn "dead_letter_queue" faststream_concurrent_aiokafka/ tests/ README.md CLAUDE.md --include='*.py' --include='*.md'
```
Expected: the `grep` prints nothing (no dangling source references). `__pycache__` is already git-ignored (`.gitignore` has `__pycache__/*` and `*.pyc`), so nothing is tracked — this is a local-filesystem cleanup only.

- [ ] **Step 5: Lint and commit**

```bash
just lint
git add LICENSE README.md faststream_concurrent_aiokafka/processing.py
git commit
```
Commit message: `docs: add LICENSE, fix healthcheck wording, tidy imports (#5,#7,#11,#12)`

---

## Task 2: Reject batch subscribers with a clear error (#2)

**Files:**
- Modify: `faststream_concurrent_aiokafka/middleware.py` (`consume_scope`, after the non-MANUAL pass-through near line 75)
- Test: `tests/test_middleware.py`

A batch subscriber delivers `self.msg` as a `tuple` of `ConsumerRecord`s; the current `typing.cast("ConsumerRecord", self.msg)` then crashes with a bare `AttributeError` on `record.offset`. Reject early with a clear message.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_middleware.py` (uses the existing `setup_broker` fixture and `patched_message` helper):

```python
async def test_middleware_batch_subscriber_rejected(setup_broker: KafkaBroker) -> None:
    """A batch subscriber (self.msg is a tuple/list of records) is rejected with a clear error."""

    @setup_broker.subscriber("batch-reject-topic", group_id="batch-reject-group")
    async def handler(msg: typing.Any) -> None: ...

    async with TestKafkaBroker(setup_broker) as test_broker:
        await initialize_concurrent_processing(
            context=test_broker.context, commit_batch_size=10, commit_batch_timeout_sec=5
        )

        # MANUAL-ack mock so we reach the batch check; raw message is a tuple of records.
        mock_msg: typing.Final = MagicMock()
        mock_msg.committed = None  # MANUAL ack path
        mock_msg.consumer._enable_auto_commit = False

        # Force self.msg (the raw middleware message) to look like a batch.
        with (
            patched_message(test_broker, mock_msg),
            patch.object(KafkaConcurrentProcessingMiddleware, "msg", ({"a": 1}, {"b": 2}), create=True),
            pytest.raises(RuntimeError, match="does not support batch subscribers"),
        ):
            await test_broker.publish({"id": 1}, topic="batch-reject-topic")

        await asyncio.sleep(0)
        await stop_concurrent_processing(test_broker.context)
```

> Note: `BaseMiddleware` stores the raw message as `self.msg`. The `patch.object(..., "msg", ...)` sets a class-level tuple so `consume_scope` sees a batch-shaped raw message regardless of what FastStream passed. If `self.msg` is set per-instance and the class patch does not take effect during `publish`, fall back to asserting via a direct unit call: construct the middleware, set `middleware.msg = (record_a, record_b)`, and `await middleware.consume_scope(call_next, mock_msg)` — but try the class-patch form first.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --no-sync pytest tests/test_middleware.py::test_middleware_batch_subscriber_rejected -p no:cacheprovider --no-cov -v`
Expected: FAIL — currently raises `AttributeError` (not `RuntimeError`), or the message does not match `does not support batch subscribers`.

- [ ] **Step 3: Add the guard in `consume_scope`**

In `faststream_concurrent_aiokafka/middleware.py`, insert the batch check **immediately after** the non-MANUAL pass-through block (after `if kafka_message.committed is not None: return await call_next(msg)`, around line 75) and **before** the `if not concurrent_processing:` check:

```python
        if isinstance(self.msg, (list, tuple)):
            err = (
                "KafkaConcurrentProcessingMiddleware does not support batch subscribers (batch=True). "
                "Use a non-batch subscriber, or remove the middleware from this subscriber."
            )
            raise RuntimeError(err)
```

Rationale for placement: the FakeConsumer short-circuit and non-MANUAL pass-through run first (so `TestKafkaBroker` and auto-ack subscribers are unaffected); the guard fires only on the MANUAL-ack concurrent path.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --no-sync pytest tests/test_middleware.py::test_middleware_batch_subscriber_rejected -p no:cacheprovider --no-cov -v`
Expected: PASS

- [ ] **Step 5: Run the full unit suite + lint**

Run: `uv run --no-sync pytest tests/ --ignore=tests/test_integration.py` then `just lint`
Expected: all pass, coverage 100%.

- [ ] **Step 6: Commit**

```bash
git add faststream_concurrent_aiokafka/middleware.py tests/test_middleware.py
git commit
```
Commit message: `fix: reject batch subscribers with a clear error (#2)`

---

## Task 3: Bounded rebalance flush timeout (#1)

**Files:**
- Modify: `faststream_concurrent_aiokafka/consts.py` (new constant)
- Modify: `faststream_concurrent_aiokafka/batch_committer.py` (`commit_all`)
- Modify: `faststream_concurrent_aiokafka/rebalance.py` (`__init__`, `on_partitions_revoked`)
- Modify: `faststream_concurrent_aiokafka/processing.py` (`create_rebalance_listener`)
- Test: `tests/test_kafka_committer.py`, `tests/test_rebalance.py`

- [ ] **Step 1: Add the constant**

In `faststream_concurrent_aiokafka/consts.py`, add (keeping the `typing.Final` style):

```python
DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC: typing.Final = 10.0
```

- [ ] **Step 2: Write the failing committer test (timeout path)**

Add to `tests/test_kafka_committer.py`:

```python
async def test_commit_all_times_out_on_hung_handler(caplog: pytest.LogCaptureFixture) -> None:
    """commit_all returns within flush_timeout_sec even if an in-flight task never completes."""
    caplog.set_level(logging.WARNING)
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(commit_batch_timeout_sec=10.0, commit_batch_size=100)
    committer.spawn()

    async def hangs() -> None:
        await asyncio.sleep(30)

    hung_task: typing.Final = asyncio.create_task(hangs())
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=hung_task,
            offset=1,
            consumer=consumer,
            topic_partition=TopicPartition(topic="t", partition=0),
        )
    )

    loop: typing.Final = asyncio.get_running_loop()
    started: typing.Final = loop.time()
    await committer.commit_all(flush_timeout_sec=0.1)
    elapsed: typing.Final = loop.time() - started

    assert elapsed < 1.0, f"commit_all blocked on the hung task ({elapsed:.2f}s)"
    assert "flush timed out" in caplog.text
    assert committer.is_healthy  # loop still running after a timed-out flush

    hung_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await hung_task
    await committer.close()
```

- [ ] **Step 3: Run it to verify it fails**

Run: `uv run --no-sync pytest tests/test_kafka_committer.py::test_commit_all_times_out_on_hung_handler -p no:cacheprovider --no-cov -v`
Expected: FAIL — `commit_all()` takes no `flush_timeout_sec` argument (TypeError), or blocks ~30 s.

- [ ] **Step 4: Implement the timeout in `commit_all`**

In `faststream_concurrent_aiokafka/batch_committer.py`, replace `commit_all`:

```python
    async def commit_all(
        self, flush_timeout_sec: float = consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC
    ) -> None:
        """Flush and commit pending tasks without stopping the committer loop.

        Bounded by ``flush_timeout_sec``: on timeout, already-completed offsets are
        committed and any still-in-flight tasks stay uncommitted (redelivered after
        reassignment — at-least-once). Safe to call during rebalance
        (on_partitions_revoked); the committer keeps running after this returns.
        """
        self._flush_batch_event.set()
        try:
            await asyncio.wait_for(self._messages_queue.join(), timeout=flush_timeout_sec)
        except TimeoutError:
            logger.warning(
                "Kafka middleware. commit_all flush timed out after %.1fs; "
                "in-flight offsets will be redelivered on restart/reassignment",
                flush_timeout_sec,
            )
```

(`consts` is already imported in `batch_committer.py`.)

- [ ] **Step 5: Run the committer test to verify it passes**

Run: `uv run --no-sync pytest tests/test_kafka_committer.py::test_commit_all_times_out_on_hung_handler -p no:cacheprovider --no-cov -v`
Expected: PASS

- [ ] **Step 6: Write the failing rebalance-forwarding test**

Add to `tests/test_rebalance.py`:

```python
async def test_rebalance_forwards_flush_timeout(committer: MockKafkaBatchCommitter) -> None:
    """The listener forwards its configured flush timeout to commit_all."""
    listener: typing.Final = ConsumerRebalanceListener(committer, flush_timeout_sec=2.5)  # ty: ignore[invalid-argument-type]
    await listener.on_partitions_revoked(set())
    committer.commit_all.assert_called_once_with(2.5)
```

> The existing `test_rebalance_on_partitions_revoked_calls_commit_all` asserts `commit_all` was called once with no specific args; `assert_called_once()` still passes when an argument is forwarded, so it needs no change.

- [ ] **Step 7: Run it to verify it fails**

Run: `uv run --no-sync pytest tests/test_rebalance.py::test_rebalance_forwards_flush_timeout -p no:cacheprovider --no-cov -v`
Expected: FAIL — `ConsumerRebalanceListener.__init__` takes no `flush_timeout_sec`.

- [ ] **Step 8: Wire the timeout through the listener**

In `faststream_concurrent_aiokafka/rebalance.py`, add the import and update `__init__` / `on_partitions_revoked`:

```python
from faststream_concurrent_aiokafka import consts
from faststream_concurrent_aiokafka.batch_committer import KafkaBatchCommitter
```

```python
    def __init__(
        self,
        committer: KafkaBatchCommitter,
        flush_timeout_sec: float = consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC,
    ) -> None:
        self._committer = committer
        self._flush_timeout_sec = flush_timeout_sec
```

```python
    async def on_partitions_revoked(self, revoked: object) -> None:
        await self._committer.commit_all(self._flush_timeout_sec)
        self._committer.clear_cancellation_watermarks(
            typing.cast("typing.Iterable[TopicPartition]", revoked)
        )
```

- [ ] **Step 9: Forward the timeout from the handler**

In `faststream_concurrent_aiokafka/processing.py`, update `create_rebalance_listener`:

```python
    def create_rebalance_listener(
        self, flush_timeout_sec: float = consts.DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC
    ) -> ConsumerRebalanceListener:
        """Return a ConsumerRebalanceListener that flushes pending commits on partition revocation.

        ``flush_timeout_sec`` bounds how long the revoke callback waits for in-flight
        handlers to finish before returning (keeps a slow handler from stalling the
        rebalance past max.poll.interval.ms). Pass the returned listener to
        ``@broker.subscriber(..., listener=listener)``.
        """
        return ConsumerRebalanceListener(self._committer, flush_timeout_sec)
```

- [ ] **Step 10: Run rebalance + committer tests to verify pass**

Run: `uv run --no-sync pytest tests/test_rebalance.py tests/test_kafka_committer.py -p no:cacheprovider --no-cov -v`
Expected: PASS (including the unchanged `test_rebalance_on_partitions_revoked_calls_commit_all`).

- [ ] **Step 11: Full unit suite + lint, then commit**

Run: `uv run --no-sync pytest tests/ --ignore=tests/test_integration.py` then `just lint`
Expected: all pass, 100% coverage.

```bash
git add faststream_concurrent_aiokafka/consts.py faststream_concurrent_aiokafka/batch_committer.py \
        faststream_concurrent_aiokafka/rebalance.py faststream_concurrent_aiokafka/processing.py \
        tests/test_kafka_committer.py tests/test_rebalance.py
git commit
```
Commit message: `fix: bound rebalance flush with a timeout (#1)`

---

## Task 4: Backpressure ceiling on uncommitted tasks (#3) — committer core

**Files:**
- Modify: `faststream_concurrent_aiokafka/consts.py` (new constant)
- Modify: `faststream_concurrent_aiokafka/batch_committer.py` (`__init__`, `send_task`, `_call_committer`, `_commit_partitions`, `_run_commit_process` finally)
- Test: `tests/test_kafka_committer.py`

**Accounting model.** `_uncommitted_count` counts tasks admitted via `send_task` but not yet finally committed or dropped. Increment on admission (`send_task`) and on re-queue (`_call_committer` transient-error branch, a re-admission). Decrement by `len(flat)` once per commit round (`_commit_partitions`). Net effect: a re-queued task is `+1` (re-queue) then `-1` (round) → stays counted; a committed/dropped task is `-1` → leaves. This mirrors the existing `queue.put`/`task_done` balance.

- [ ] **Step 1: Add the constant**

In `faststream_concurrent_aiokafka/consts.py`:

```python
DEFAULT_MAX_UNCOMMITTED_TASKS: typing.Final = 10_000
```

- [ ] **Step 2: Write the failing backpressure test**

Add to `tests/test_kafka_committer.py`:

```python
async def test_send_task_blocks_when_uncommitted_ceiling_reached() -> None:
    """send_task blocks once the uncommitted ceiling is hit, then unblocks as commits drain it."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    # Ceiling of 2; commits stalled by a gate so nothing drains until we open it.
    committer: typing.Final = KafkaBatchCommitter(
        commit_batch_timeout_sec=10.0, commit_batch_size=100, max_uncommitted_tasks=2
    )
    commit_gate: typing.Final = asyncio.Event()

    async def gated_commit(_offsets: object) -> None:
        await commit_gate.wait()

    consumer.commit.side_effect = gated_commit
    committer.spawn()

    async def done() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)

    async def send(offset: int) -> None:
        await committer.send_task(
            KafkaCommitTask(
                asyncio_task=asyncio.create_task(done()),
                offset=offset,
                consumer=consumer,
                topic_partition=tp,
            )
        )

    await send(1)
    await send(2)  # count now at ceiling (2)

    third: typing.Final = asyncio.create_task(send(3))
    await asyncio.sleep(0.05)
    assert not third.done(), "send_task should block at the uncommitted ceiling"

    # Let the stalled commit complete → count drops → blocked send_task proceeds.
    commit_gate.set()
    await asyncio.wait_for(third, timeout=1.0)
    assert third.done()

    await committer.close()


async def test_send_task_unbounded_when_ceiling_is_none() -> None:
    """max_uncommitted_tasks=None preserves unbounded admission (no blocking)."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(
        commit_batch_timeout_sec=10.0, commit_batch_size=100, max_uncommitted_tasks=None
    )
    consumer.commit.side_effect = lambda _o: asyncio.sleep(30)  # never drains
    committer.spawn()

    async def done() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    for offset in range(5):
        await asyncio.wait_for(
            committer.send_task(
                KafkaCommitTask(
                    asyncio_task=asyncio.create_task(done()),
                    offset=offset,
                    consumer=consumer,
                    topic_partition=tp,
                )
            ),
            timeout=1.0,
        )  # never blocks

    committer._commit_task.cancel()  # noqa: SLF001
    with contextlib.suppress(asyncio.CancelledError):
        await committer._commit_task  # noqa: SLF001


async def test_send_task_unblocks_with_dead_committer_error() -> None:
    """A send_task blocked on the ceiling raises CommitterIsDeadError if the loop dies."""
    consumer: typing.Final = MockAIOKafkaConsumer()
    committer: typing.Final = KafkaBatchCommitter(
        commit_batch_timeout_sec=10.0, commit_batch_size=100, max_uncommitted_tasks=1
    )
    consumer.commit.side_effect = lambda _o: asyncio.sleep(30)  # never drains
    committer.spawn()

    async def done() -> None:
        return None

    tp: typing.Final = TopicPartition(topic="t", partition=0)
    await committer.send_task(
        KafkaCommitTask(
            asyncio_task=asyncio.create_task(done()),
            offset=1,
            consumer=consumer,
            topic_partition=tp,
        )
    )  # count now at ceiling (1)

    blocked: typing.Final = asyncio.create_task(
        committer.send_task(
            KafkaCommitTask(
                asyncio_task=asyncio.create_task(done()),
                offset=2,
                consumer=consumer,
                topic_partition=tp,
            )
        )
    )
    await asyncio.sleep(0.05)
    assert not blocked.done()

    committer._commit_task.cancel()  # noqa: SLF001
    with contextlib.suppress(asyncio.CancelledError):
        await committer._commit_task  # noqa: SLF001

    with pytest.raises(CommitterIsDeadError):
        await asyncio.wait_for(blocked, timeout=1.0)
```

- [ ] **Step 3: Run the new tests to verify they fail**

Run: `uv run --no-sync pytest tests/test_kafka_committer.py -k uncommitted -p no:cacheprovider --no-cov -v`
Expected: FAIL — `KafkaBatchCommitter.__init__` takes no `max_uncommitted_tasks`.

- [ ] **Step 4: Add fields in `__init__`**

In `faststream_concurrent_aiokafka/batch_committer.py`, extend the constructor signature and body:

```python
    def __init__(
        self,
        commit_batch_timeout_sec: float = consts.DEFAULT_COMMIT_BATCH_TIMEOUT_SEC,
        commit_batch_size: int = consts.DEFAULT_COMMIT_BATCH_SIZE,
        shutdown_timeout_sec: float = consts.DEFAULT_SHUTDOWN_TIMEOUT_SEC,
        max_uncommitted_tasks: int | None = consts.DEFAULT_MAX_UNCOMMITTED_TASKS,
    ) -> None:
        ...  # existing body unchanged
        # Backpressure: count of tasks admitted via send_task but not yet finally
        # committed/dropped (in _messages_queue + pending). When it reaches the
        # ceiling, send_task blocks so the consume path stalls and the consumer stops
        # fetching until commits catch up. None disables the bound.
        self._max_uncommitted_tasks = max_uncommitted_tasks
        self._uncommitted_count: int = 0
        # Set whenever the count drops (a commit round) or the loop exits; wakes a
        # send_task blocked at the ceiling so it re-checks.
        self._uncommitted_drained = asyncio.Event()
        self._uncommitted_drained.set()
```

Add these two lines at the end of the constructor body (after the existing `_partition_owner` assignment).

- [ ] **Step 5: Add the blocking + admission in `send_task`**

Replace `send_task`:

```python
    async def send_task(self, new_task: KafkaCommitTask) -> None:
        self._check_is_commit_task_running()
        while (
            self._max_uncommitted_tasks is not None
            and self._uncommitted_count >= self._max_uncommitted_tasks
        ):
            self._uncommitted_drained.clear()
            # Re-check liveness before parking: if the loop died we must raise, not hang.
            self._check_is_commit_task_running()
            await self._uncommitted_drained.wait()
        self._uncommitted_count += 1
        await self._messages_queue.put(new_task)
```

- [ ] **Step 6: Count re-queues in `_call_committer`**

In the transient-`KafkaError` branch of `_call_committer`, count each re-admission:

```python
        except KafkaError:
            # Transient error — re-queue batch for retry on next cycle
            logger.exception("Error during commit to kafka, re-queuing batch")
            for task in tasks_batch:
                self._uncommitted_count += 1
                await self._messages_queue.put(task)
            return False
```

- [ ] **Step 7: Decrement once per commit round in `_commit_partitions`**

In `_commit_partitions`, after the `for _ in flat: self._messages_queue.task_done()` loop and before `return all(results)`, add:

```python
        self._uncommitted_count -= len(flat)
        self._uncommitted_drained.set()
```

- [ ] **Step 8: Wake blocked producers when the loop exits**

In `_run_commit_process`, the `finally` currently calls `state.cancel_outstanding()`. Add the drain-set so a blocked `send_task` wakes and hits the dead-committer check:

```python
        finally:
            state.cancel_outstanding()
            self._uncommitted_drained.set()
```

- [ ] **Step 9: Run the backpressure tests to verify they pass**

Run: `uv run --no-sync pytest tests/test_kafka_committer.py -k uncommitted -p no:cacheprovider --no-cov -v`
Expected: PASS (all three).

- [ ] **Step 10: Full unit suite + lint, then commit**

Run: `uv run --no-sync pytest tests/ --ignore=tests/test_integration.py` then `just lint`
Expected: all pass, 100% coverage. (Existing committer tests still pass — the default ceiling of 10_000 never trips in those small tests.)

```bash
git add faststream_concurrent_aiokafka/consts.py faststream_concurrent_aiokafka/batch_committer.py \
        tests/test_kafka_committer.py
git commit
```
Commit message: `fix: add opt-in backpressure ceiling on uncommitted tasks (#3)`

---

## Task 5: Thread `max_uncommitted_tasks` through initialization (#3 wiring)

**Files:**
- Modify: `faststream_concurrent_aiokafka/middleware.py` (`initialize_concurrent_processing`)
- Test: `tests/test_middleware.py`

- [ ] **Step 1: Write the failing wiring test**

Add to `tests/test_middleware.py`:

```python
async def test_middleware_initialize_passes_max_uncommitted_tasks(setup_broker: KafkaBroker) -> None:
    """initialize_concurrent_processing forwards max_uncommitted_tasks to the committer."""
    async with TestKafkaBroker(setup_broker) as test_broker:
        handler: typing.Final = await initialize_concurrent_processing(
            context=test_broker.context, max_uncommitted_tasks=500
        )
        try:
            assert handler._committer._max_uncommitted_tasks == 500
        finally:
            await stop_concurrent_processing(test_broker.context)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --no-sync pytest tests/test_middleware.py::test_middleware_initialize_passes_max_uncommitted_tasks -p no:cacheprovider --no-cov -v`
Expected: FAIL — `initialize_concurrent_processing` takes no `max_uncommitted_tasks`.

- [ ] **Step 3: Add the parameter**

In `faststream_concurrent_aiokafka/middleware.py`, extend `initialize_concurrent_processing`:

```python
async def initialize_concurrent_processing(
    context: ContextRepo,
    concurrency_limit: int = consts.DEFAULT_CONCURRENCY_LIMIT,
    commit_batch_size: int = consts.DEFAULT_COMMIT_BATCH_SIZE,
    commit_batch_timeout_sec: float = consts.DEFAULT_COMMIT_BATCH_TIMEOUT_SEC,
    shutdown_timeout_sec: float = consts.DEFAULT_SHUTDOWN_TIMEOUT_SEC,
    max_uncommitted_tasks: int | None = consts.DEFAULT_MAX_UNCOMMITTED_TASKS,
) -> KafkaConcurrentHandler:
```

and pass it into the `KafkaBatchCommitter(...)` construction:

```python
        committer=KafkaBatchCommitter(
            commit_batch_timeout_sec=commit_batch_timeout_sec,
            commit_batch_size=commit_batch_size,
            shutdown_timeout_sec=shutdown_timeout_sec,
            max_uncommitted_tasks=max_uncommitted_tasks,
        ),
```

- [ ] **Step 4: Run it to verify it passes**

Run: `uv run --no-sync pytest tests/test_middleware.py::test_middleware_initialize_passes_max_uncommitted_tasks -p no:cacheprovider --no-cov -v`
Expected: PASS

- [ ] **Step 5: Full unit suite + lint, then commit**

Run: `uv run --no-sync pytest tests/ --ignore=tests/test_integration.py` then `just lint`

```bash
git add faststream_concurrent_aiokafka/middleware.py tests/test_middleware.py
git commit
```
Commit message: `feat: expose max_uncommitted_tasks via initialize_concurrent_processing (#3)`

---

## Task 6: Update README for shipped behavior (#4, #6)

**Files:**
- Modify: `README.md`

No tests (docs only). Make the docs match the code shipped in Tasks 2–5.

- [ ] **Step 1: Rewrite §Core Concepts → KafkaConcurrentHandler (#4)**

In `README.md`, replace the `### KafkaConcurrentHandler` bullet list (currently claiming "counter + `asyncio.Event`" and "Signal handlers for graceful shutdown") with:

```markdown
### KafkaConcurrentHandler

The processing engine. Manages:
- An `asyncio.Semaphore` to enforce `concurrency_limit`
- In-flight task tracking via a `set[asyncio.Task]`; each task's done-callback releases the semaphore, removes the task from the set, and logs any non-cancellation exception
- A `KafkaBatchCommitter` for offset commits
- An optional `ConsumerRebalanceListener` (via `handler.create_rebalance_listener()`) that flushes pending commits when partitions are revoked

This library does **not** install signal handlers — shutdown is driven by your lifespan / process manager calling `stop_concurrent_processing`.
```

- [ ] **Step 2: Document the rebalance flush cost + timeout (#6)**

In `README.md` "How It Works", update the rebalance step (4) to note the bounded wait:

```markdown
4. **Rebalance handling**: When Kafka revokes a partition, the `ConsumerRebalanceListener` (returned by `handler.create_rebalance_listener(flush_timeout_sec=...)`) calls `committer.commit_all()` to flush pending offsets before the partition is reassigned. The flush waits for in-flight handlers up to `flush_timeout_sec` (default 10 s) so a slow handler cannot stall the rebalance past `max.poll.interval.ms`; on timeout, the remaining in-flight messages are redelivered after reassignment (at-least-once). A future optimization may scope the wait to only the revoked partitions.
```

Also add `max_uncommitted_tasks` to the `initialize_concurrent_processing` parameter table:

```markdown
| `max_uncommitted_tasks` | `10000` | Max tasks accepted but not yet committed before the consume path blocks (backpressure). `None` disables the bound. |
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit
```
Commit message: `docs: align README with shipped flush-timeout and backpressure behavior (#4,#6)`

---

## Task 7: Rewrite the misleading shutdown integration test (#8)

**Files:**
- Modify: `tests/test_integration.py` (`test_real_kafka_graceful_shutdown_waits_for_tasks`, lines ~266-289)

This test asserts shutdown "waits for in-flight tasks," but `stop()` **cancels** them. Rewrite it to assert cancellation + at-least-once redelivery. **Requires Docker** — validate via `just test`.

- [ ] **Step 1: Replace the test**

In `tests/test_integration.py`, replace `test_real_kafka_graceful_shutdown_waits_for_tasks` with a two-phase cancel/replay test (mirrors the structure of `test_real_kafka_multi_subscriber_commits_all_offsets`):

```python
async def test_real_kafka_shutdown_cancels_in_flight_tasks(kafka_bootstrap_servers: str) -> None:
    """stop_concurrent_processing cancels in-flight handlers; their offsets are redelivered (at-least-once)."""
    topic: typing.Final = _topic("shutdown-cancel")
    group: typing.Final = f"shutdown-cancel-group-{uuid.uuid4().hex[:6]}"
    await _create_topic(kafka_bootstrap_servers, topic)

    # Phase 1: dispatch a handler that blocks; stop() while it is genuinely in-flight.
    started: typing.Final = asyncio.Event()
    cancelled_seen: typing.Final[list[bool]] = []
    completed_phase1: typing.Final[list[int]] = []

    broker1: typing.Final = _broker(kafka_bootstrap_servers)

    @broker1.subscriber(topic, group_id=group, auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler1(msg: dict[str, int]) -> None:
        started.set()
        try:
            await asyncio.sleep(30)  # far longer than the test window
            completed_phase1.append(msg["id"])
        except asyncio.CancelledError:
            cancelled_seen.append(True)
            raise

    async with broker1:
        await broker1.start()
        await initialize_concurrent_processing(
            context=broker1.context, commit_batch_size=10, commit_batch_timeout_sec=5, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        await broker1.publish({"id": 1}, topic=topic)
        await asyncio.wait_for(started.wait(), timeout=POLL_SLEEP)
        # Stop while the handler is still sleeping → it must be cancelled, not awaited.
        await stop_concurrent_processing(broker1.context)

    assert cancelled_seen == [True], "in-flight handler was not cancelled on stop"
    assert completed_phase1 == [], "handler completed despite shutdown cancellation"

    # Phase 2: restart with the same group id → the uncommitted message is redelivered.
    replayed: typing.Final[list[int]] = []
    broker2: typing.Final = _broker(kafka_bootstrap_servers)

    @broker2.subscriber(topic, group_id=group, auto_offset_reset="earliest", ack_policy=AckPolicy.MANUAL)
    async def handler2(msg: dict[str, int]) -> None:
        replayed.append(msg["id"])

    async with broker2:
        await broker2.start()
        await initialize_concurrent_processing(
            context=broker2.context, commit_batch_size=10, commit_batch_timeout_sec=2, concurrency_limit=5
        )
        await asyncio.sleep(CONSUMER_READY_SLEEP)
        try:
            await asyncio.sleep(POLL_SLEEP)
            assert replayed == [1], f"cancelled message was not redelivered: {replayed}"
        finally:
            await stop_concurrent_processing(broker2.context)
```

- [ ] **Step 2: Run the integration suite (Docker)**

Run: `just test tests/test_integration.py::test_real_kafka_shutdown_cancels_in_flight_tasks`
Expected: PASS (Redpanda starts, the handler is cancelled in phase 1, message 1 replays in phase 2).

> If `just test` runs the whole file, that is fine; ensure the new test passes and no others regress.

- [ ] **Step 3: Commit**

```bash
git add tests/test_integration.py
git commit
```
Commit message: `test: assert shutdown cancels in-flight tasks and redelivers (#8)`

---

## Task 8: Extract pending/watermark state from `batch_committer.py` (#10)

**Files:**
- Create: `faststream_concurrent_aiokafka/_pending_state.py`
- Modify: `faststream_concurrent_aiokafka/batch_committer.py`
- (No new tests — existing `tests/test_kafka_committer.py` is the safety net.)

**This task is a pure refactor: no behavior change.** It lands LAST, on top of green tests from Tasks 2–7. The goal is to move the per-partition pending bookkeeping, offset mapping, and cancellation watermarks into a focused unit, leaving `KafkaBatchCommitter` as the loop driver.

> **Execution note:** Because the existing committer tests call several of these as `KafkaBatchCommitter._map_offsets_per_partition(...)`, `_extract_ready_prefixes(...)`, and `_insert_sorted(...)` directly (see `tests/test_kafka_committer.py`), do the extraction **without breaking those call sites**: keep thin static delegators on `KafkaBatchCommitter` (or update the tests in the same commit). The lowest-risk path that keeps tests untouched is to keep the static methods on `KafkaBatchCommitter` as one-line delegators to the new module. Verify by running the full committer suite unchanged.

- [ ] **Step 1: Create `_pending_state.py` with the moved pure functions**

Create `faststream_concurrent_aiokafka/_pending_state.py` and move the bodies of `_insert_sorted` (currently module-level in `batch_committer.py`), `_extract_ready_prefixes`, and `_map_offsets_per_partition` (currently static methods) into it verbatim, plus the `_OFFSET_KEY` helper:

```python
import bisect
import operator
import typing

from faststream.kafka import TopicPartition

from faststream_concurrent_aiokafka.batch_committer import KafkaCommitTask  # see Step 3 note


_OFFSET_KEY: typing.Final = operator.attrgetter("offset")


def insert_sorted(partition_pending: list["KafkaCommitTask"], new_ct: "KafkaCommitTask") -> None:
    # (verbatim body from batch_committer._insert_sorted)
    ...


def extract_ready_prefixes(
    pending: dict[TopicPartition, list["KafkaCommitTask"]],
) -> tuple[dict[TopicPartition, list["KafkaCommitTask"]], int]:
    # (verbatim body from KafkaBatchCommitter._extract_ready_prefixes)
    ...


def map_offsets_per_partition(
    consumer_id: int,
    consumer_tasks: list["KafkaCommitTask"],
    watermarks: dict[tuple[int, TopicPartition], int],
) -> dict[TopicPartition, int]:
    # (verbatim body from KafkaBatchCommitter._map_offsets_per_partition)
    ...
```

> **Circular-import note (Step 3):** `KafkaCommitTask` lives in `batch_committer.py`. To avoid a cycle, either (a) move the `KafkaCommitTask` dataclass into `_pending_state.py` and import it back into `batch_committer.py`, or (b) keep `KafkaCommitTask` in `batch_committer.py` and use a string/`TYPE_CHECKING` import in `_pending_state.py` (the functions only read `.offset`, `.topic_partition`, `.asyncio_task`, so no runtime import of the class is needed). Prefer (a) — `KafkaCommitTask` is the state's natural home — but (b) is acceptable if it keeps the diff smaller.

- [ ] **Step 2: Replace the originals in `batch_committer.py` with delegators**

In `batch_committer.py`, replace the three moved definitions with thin delegators so existing test call sites keep working:

```python
from faststream_concurrent_aiokafka import _pending_state


def _insert_sorted(partition_pending: list[KafkaCommitTask], new_ct: KafkaCommitTask) -> None:
    _pending_state.insert_sorted(partition_pending, new_ct)


class KafkaBatchCommitter:
    @staticmethod
    def _extract_ready_prefixes(
        pending: dict[TopicPartition, list[KafkaCommitTask]],
    ) -> tuple[dict[TopicPartition, list[KafkaCommitTask]], int]:
        return _pending_state.extract_ready_prefixes(pending)

    @staticmethod
    def _map_offsets_per_partition(
        consumer_id: int,
        consumer_tasks: list[KafkaCommitTask],
        watermarks: dict[tuple[int, TopicPartition], int],
    ) -> dict[TopicPartition, int]:
        return _pending_state.map_offsets_per_partition(consumer_id, consumer_tasks, watermarks)
```

Update the loop's internal calls (`_insert_sorted(...)`, `self._extract_ready_prefixes(...)`, `self._map_offsets_per_partition(...)`) to keep referencing these names — no change needed at call sites since the delegators preserve the signatures.

- [ ] **Step 3: Run the FULL committer suite unchanged to prove no behavior change**

Run: `uv run --no-sync pytest tests/test_kafka_committer.py -p no:cacheprovider --no-cov -v`
Expected: PASS — every existing test passes without modification (the delegators preserve the public-to-tests surface).

- [ ] **Step 4: Full unit suite + lint**

Run: `uv run --no-sync pytest tests/ --ignore=tests/test_integration.py` then `just lint`
Expected: all pass, 100% coverage. (`ty check` must be clean — watch the `KafkaCommitTask` import direction from Step 1's note.)

- [ ] **Step 5: Commit**

```bash
git add faststream_concurrent_aiokafka/_pending_state.py faststream_concurrent_aiokafka/batch_committer.py
git commit
```
Commit message: `refactor: extract pending/watermark helpers into _pending_state (#10)`

---

## Task 9: Final verification

- [ ] **Step 1: Run the complete suite in Docker (unit + integration + coverage)**

Run: `just test`
Expected: all tests pass; coverage 100%; no lint failures in `lint-ci` if invoked.

- [ ] **Step 2: Lint-CI parity check**

Run: `just lint-ci`
Expected: no changes needed (formatting, ruff, ty all clean).

- [ ] **Step 3: Confirm the branch is ready**

```bash
git log --oneline main..HEAD
git status
```
Expected: one commit per task, clean working tree. Hand off to `superpowers:finishing-a-development-branch`.

---

## Self-Review notes (already applied)

- **Spec coverage:** #1 → Task 3; #2 → Task 2; #3 → Tasks 4–5; #4,#6 → Task 6; #5,#7 → Task 1; #8 → Task 7; #9(a) → Task 3; #9(b) → Task 2; #9(c) → Task 4; #10 → Task 8; #11,#12 → Task 1. All twelve findings mapped.
- **Defaults:** `DEFAULT_REBALANCE_FLUSH_TIMEOUT_SEC = 10.0`, `DEFAULT_MAX_UNCOMMITTED_TASKS = 10_000` (matching the spec table).
- **Type consistency:** `flush_timeout_sec` and `max_uncommitted_tasks` names used identically across committer, listener, handler, and middleware. The backpressure accounting (`_uncommitted_count`, `_uncommitted_drained`) increments/decrements are symmetric with the existing `queue.put`/`task_done` balance.
- **Coverage gate:** every code-adding task pairs the change with a test in the same commit; the timeout branch, the three backpressure branches, and the batch-guard branch each have an explicit covering test.
