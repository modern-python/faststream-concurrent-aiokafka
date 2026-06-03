# FastStream 0.7 migration — PR2 (bump to >=0.7,<0.8, drop 0.6 support) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `faststream-concurrent-aiokafka` to `faststream[kafka]>=0.7,<0.8`. Drop 0.6 support entirely — single code path, no compat shim. Adopt no new 0.7 features.

**Architecture:** Discovery-driven. This package's 0.7 surface is small (no `Producer` subclass, no `TestKafkaBroker` subclass, no `Registrator` exposing per-call `middlewares=`, no `faststream._internal.*` imports). The known at-risk sites are `KafkaConcurrentProcessingMiddleware.consume_scope` (already carries `# ty: ignore[invalid-method-override]` on 0.6) and the `type(consumer).__name__ == "FakeConsumer"` string match. The plan: bump the pin, regenerate the lock, then iterate on `just lint && just test` until clean. Single bundled commit on `chore/faststream-0.7-migration`.

**Tech Stack:** `pyproject.toml`, `uv`, `pytest-cov`, `ruff`, `ty`, `just`, Docker (Redpanda).

**Related spec:** `planning/specs/2026-06-03-faststream-0.7-migration-design.md`.

**Branch prerequisite:** PR1 (`chore/pin-faststream-pre-0.7`) has been merged to `main`. This plan branches off `main` after PR1's merge commit.

---

## Task 1: Create the working branch

**Files:** None (git operation).

- [ ] **Step 1: Pull latest main**

Run: `git switch main && git pull --ff-only`
Expected: working tree on `main`, fast-forwarded to include PR1's merge commit. `git log -1 --oneline` should show the PR1 commit (or its merge commit).

- [ ] **Step 2: Confirm PR1 has merged**

Run: `grep -n '"faststream\[kafka\]' pyproject.toml`
Expected: `28:    "faststream[kafka]>=0.6,<0.7",`
If the line still shows `"faststream[kafka]",` (unbounded), PR1 has not merged — stop and do PR1 first.

- [ ] **Step 3: Create branch**

Run: `git switch -c chore/faststream-0.7-migration`
Expected: `Switched to a new branch 'chore/faststream-0.7-migration'`

## Task 2: Bump the pin to >=0.7,<0.8

**Files:**
- Modify: `pyproject.toml` (line 28)

- [ ] **Step 1: Read current dependencies block**

Run: `sed -n '26,30p' pyproject.toml`
Expected output:
```
version = "0"
dependencies = [
    "faststream[kafka]>=0.6,<0.7",
]
```

- [ ] **Step 2: Edit the pin**

Change `"faststream[kafka]>=0.6,<0.7"` on line 28 to `"faststream[kafka]>=0.7,<0.8"`.

- [ ] **Step 3: Verify edit**

Run: `grep -n '"faststream\[kafka\]' pyproject.toml`
Expected: `28:    "faststream[kafka]>=0.7,<0.8",`

## Task 3: Regenerate lockfile to a 0.7.x release

**Files:**
- Touched (gitignored, not committed): `uv.lock`

- [ ] **Step 1: Regenerate lock**

Run: `uv lock --upgrade`
Expected: `Resolved N packages in <time>` with no error. The lock should now resolve `faststream` to a `0.7.x` version.

- [ ] **Step 2: Verify the resolved version**

Run: `grep -A 1 '^name = "faststream"' uv.lock | head -4`
Expected output (first two lines):
```
name = "faststream"
version = "0.7.x"
```
(Where `x` is whatever the latest 0.7 release is — `0.7.0` or later.)

If the version line shows `0.6.x` or `0.8.x`, the pin edit did not take. Re-check Task 2.

- [ ] **Step 3: Sync environment**

Run: `uv sync --all-extras --all-groups --frozen`
Expected: Sync succeeds. Installed `faststream` is now 0.7.x.

## Task 4: Inspect 0.7's at-risk surface before changing source

**Files:** None (read-only inspection).

This task gathers ground truth from the installed 0.7 wheel so the source edits in Tasks 5-7 are informed. Each step writes its finding into the implementer's notes (e.g. as a temporary file or simply held in context for the next tasks). The decisions made here drive whether each subsequent task is a no-op, a one-line fix, or a refactor.

- [ ] **Step 1: Check `FakeConsumer` class name**

Run: `uv run python -c "from faststream.kafka import testing; print([n for n in dir(testing) if 'onsumer' in n.lower()])"`
Expected: a list containing class names with `consumer` substring.

- **If output includes `FakeConsumer`:** no change needed at `middleware.py:41`. Note this for Task 5.
- **If `FakeConsumer` is missing but a similar class (e.g. `MockConsumer`, `TestConsumer`) is present:** the string match in `middleware.py:41` needs updating. Note the new class name for Task 5.
- **If no consumer-like class is exported from `faststream.kafka.testing`:** the test-broker fast path needs to be re-derived. Inspect `faststream.kafka` and `faststream._internal.testing` (or wherever the class moved). Likely fallback: introspect a different attribute on the consumer that distinguishes the test broker, OR check `kafka_message` (e.g., `type(kafka_message).__name__` against the test message wrapper). Note the new approach for Task 5.

- [ ] **Step 2: Check `BaseMiddleware.consume_scope` signature**

Run: `uv run python -c "import inspect; from faststream import BaseMiddleware; print(inspect.signature(BaseMiddleware.consume_scope))"`
Expected: a signature string like `(self, call_next, msg, /)` or similar.

Compare to the current override in `faststream_concurrent_aiokafka/middleware.py:51-55`:
```python
async def consume_scope(  # ty: ignore[invalid-method-override]
    self,
    call_next: typing.Callable[[KafkaAckableMessage], typing.Awaitable[typing.Any]],
    msg: KafkaAckableMessage,
) -> typing.Any:
```

- **If the upstream signature now structurally matches the override:** the `# ty: ignore[invalid-method-override]` comment can be dropped. Note this for Task 6.
- **If the upstream signature still differs but `ty` reports the override as fine on 0.7:** drop the ignore comment. Note for Task 6.
- **If the upstream signature changed in a way that requires altering the override (new required parameter, different `call_next` shape, different return type):** plan the override edit. Note the new signature for Task 6.

Run `uv run ty check faststream_concurrent_aiokafka/middleware.py` to see ty's verdict on the current override under 0.7. If it reports no error with the ignore comment still present, the comment may now be a no-op (ruff's `PGH004` rule would flag an unused-ignore — let lint surface it in Task 8).

- [ ] **Step 3: Verify public import paths**

Run the following one-liners, each of which should exit 0 with no output:

```bash
uv run python -c "from faststream import BaseMiddleware, ContextRepo"
uv run python -c "from faststream.kafka import ConsumerRecord, TopicPartition, KafkaBroker, KafkaRouter, TestKafkaBroker"
uv run python -c "from faststream.kafka.message import KafkaAckableMessage"
uv run python -c "from faststream.middlewares import AckPolicy; AckPolicy.MANUAL"
uv run python -c "from faststream.asgi import AsgiFastStream"
```

For each command that fails with `ImportError` or `AttributeError`, note the failing symbol and the new home (find it via `uv run python -c "import faststream; help(faststream)"` or by grepping the installed wheel: `uv run python -c "import faststream; print(faststream.__file__)"` then `grep -rn 'class KafkaAckableMessage' /path/to/site-packages/faststream/`). Note each move for Task 7.

- [ ] **Step 4: Try the test broker fast path end-to-end (smoke check)**

Run: `uv run --no-sync pytest tests/test_middleware.py -k FakeConsumer -v 2>&1 | head -50`
Expected: the FakeConsumer-targeted assertions pass.

If the test fails with a message indicating the FakeConsumer branch did not trigger (e.g., the test expects sequential pass-through but observes the concurrent path), Step 1 found the wrong class name. Reconcile before continuing.

## Task 5: Fix `FakeConsumer` class-name match (conditional on Task 4 step 1)

**Files:**
- Modify: `faststream_concurrent_aiokafka/middleware.py` (line 41)

**Skip this task if Task 4 step 1 reported `FakeConsumer` is still present under that name.**

- [ ] **Step 1: Read the current site**

Run: `sed -n '36,48p' faststream_concurrent_aiokafka/middleware.py`
Expected output:
```python
def _consumer_attrs(consumer: typing.Any) -> _ConsumerAttrs:  # noqa: ANN401
    cached: typing.Final = _consumer_attrs_cache.get(consumer)
    if cached is not None:
        return cached
    attrs: typing.Final = _ConsumerAttrs(
        is_fake=type(consumer).__name__ == "FakeConsumer",
        auto_commit=bool(getattr(consumer, "_enable_auto_commit", False)),
    )
    # Consumer may not be weakreferable (rare, e.g. exotic mock subclasses); fall through.
    with contextlib.suppress(TypeError):
        _consumer_attrs_cache[consumer] = attrs
    return attrs
```

- [ ] **Step 2: Update the literal**

Decide based on Task 4 step 1's finding:

- **If the class was renamed** (e.g. `FakeConsumer` → `MockConsumer`): change the literal `"FakeConsumer"` on line 41 to the new class name. Single-line edit.
- **If the test-broker fast path needs a different signal** (Task 4 step 1's third branch): replace the `is_fake=` line with the new check. Update the docstring at lines 29-32 if it references the old approach. If a new helper is warranted, keep it in `middleware.py` — no need for a new file.

- [ ] **Step 3: Update tests if the rename surfaces in test code**

Run: `grep -rn 'FakeConsumer' tests/`
For each hit, decide whether it documents the old class name (update to the new one) or asserts on it (likely needs rewriting in lockstep with Step 2).

- [ ] **Step 4: Verify the test-broker tests still pass**

Run: `uv run --no-sync pytest tests/test_middleware.py -k FakeConsumer -v`
Expected: PASS.

If still failing, reconcile with Task 4 step 1 — the chosen rename or signal is wrong.

## Task 6: Fix `consume_scope` override (conditional on Task 4 step 2)

**Files:**
- Modify: `faststream_concurrent_aiokafka/middleware.py` (lines 50-55)

**Skip this task if Task 4 step 2 reported that `ty check` is clean on the existing override under 0.7 AND the `# ty: ignore` comment is still needed (i.e., the override is structurally correct as-is).**

- [ ] **Step 1: Decide the edit**

Based on Task 4 step 2's finding, one of:

- **(A) Drop the `# ty: ignore` comment.** The signature now structurally matches upstream. Edit: remove the trailing `# ty: ignore[invalid-method-override]` comment on line 51.
- **(B) Update the signature to match new upstream shape.** Edit the parameter list to match `inspect.signature(BaseMiddleware.consume_scope)` from Task 4. Keep `# ty: ignore[invalid-method-override]` if the upstream signature still differs at the type level (e.g., uses a Protocol with different `call_next` type variance than ours). Drop it if ty is now clean.

- [ ] **Step 2: Apply the edit**

Edit `faststream_concurrent_aiokafka/middleware.py:50-55` per Step 1.

- [ ] **Step 3: Verify ty is clean**

Run: `uv run ty check faststream_concurrent_aiokafka/middleware.py`
Expected: no errors.

If errors remain, the override shape still doesn't match upstream. Inspect upstream's `BaseMiddleware.consume_scope` source and reconcile.

- [ ] **Step 4: Verify the middleware unit tests still pass**

Run: `uv run --no-sync pytest tests/test_middleware.py -v 2>&1 | tail -20`
Expected: all tests pass.

## Task 7: Fix moved public imports (conditional on Task 4 step 3)

**Files:**
- Modify: any source/test file whose import broke (specific files determined by Task 4 step 3 findings).

**Skip this task if Task 4 step 3 reported all five imports succeed on 0.7.**

- [ ] **Step 1: For each moved symbol, locate every import site**

For each symbol that moved (per Task 4 step 3), run:

```bash
grep -rn "from faststream[^[:space:]]*import[^,]*<SYMBOL>" faststream_concurrent_aiokafka/ tests/
```

(Replace `<SYMBOL>` with the actual moved symbol, e.g. `KafkaAckableMessage`, `AckPolicy`, etc.)

Record every file:line hit.

- [ ] **Step 2: Update each import site**

For each hit, change the `from <old_path> import <SYMBOL>` to `from <new_path> import <SYMBOL>` (new path per Task 4 step 3).

- [ ] **Step 3: Verify imports resolve**

Run: `uv run python -c "from faststream_concurrent_aiokafka import KafkaConcurrentProcessingMiddleware, initialize_concurrent_processing, stop_concurrent_processing, is_kafka_handler_healthy, ConsumerRebalanceListener"`
Expected: exit 0 with no output.

If `ImportError` is raised, a site was missed — re-run Step 1 for the failing symbol with a broader grep (`grep -rn "<SYMBOL>" .`).

- [ ] **Step 4: Run the full test suite to surface any remaining missed sites**

Run: `just test`
Expected: tests pass (coverage gate may or may not pass yet — addressed in Task 8).

If a test fails with `ImportError`, repeat Step 2 for that site.

## Task 8: Lint and coverage clean-up

**Files:** Whatever lint surfaces.

- [ ] **Step 1: Run lint**

Run: `just lint`
Expected: All four steps green.

Common failure modes:
- **`ruff PGH004` (or similar) — unused ignore comment.** If `# ty: ignore[invalid-method-override]` is now a no-op (Task 6 left it in place but ty no longer reports an error), ruff flags it. Remove the comment.
- **`ruff` import sort.** If new imports were added in Task 7, ruff's isort may re-sort them. Auto-fix via `ruff check --fix` (already part of `just lint`); just re-stage.
- **`ty` residual errors.** Investigate each. Most likely a type whose definition moved (Task 7 sibling case for a type-only import).

- [ ] **Step 2: Re-run lint to confirm green**

Run: `just lint`
Expected: all four steps green.

- [ ] **Step 3: Run the full test suite at the 100% gate**

Run: `just test`
Expected: PASS at 100% coverage (gate adopted in PR1).

If coverage dropped below 100%:
- **Most likely cause:** a branch in the source code that was exercised by a now-passing test path is no longer reachable under 0.7 (e.g., the test-broker fast path's `if attrs.is_fake:` branch in `middleware.py:63-64` now triggers via a different signal and the dead branch is the literal name-match line). Identify the dead branch by reading the missing-lines section of the coverage report.
- **Fix:** if the branch is genuinely dead, delete it; if it's reachable under different inputs, add the missing test.

Do not lower the coverage gate as a workaround.

## Task 9: Final verification (smoke + import surface)

**Files:** None.

- [ ] **Step 1: Confirm the package's public surface still imports cleanly**

Run: `uv run python -c "from faststream_concurrent_aiokafka import KafkaConcurrentProcessingMiddleware, initialize_concurrent_processing, stop_concurrent_processing, is_kafka_handler_healthy, ConsumerRebalanceListener; print('OK')"`
Expected: `OK`.

- [ ] **Step 2: Confirm no lingering 0.6-pin references**

Run: `git grep -n "faststream\[kafka\]>=0.6,<0.7"`
Expected: no output.

Run: `git grep -n "faststream<0.7\|faststream~=0.6"`
Expected: no output.

- [ ] **Step 3: Confirm the integration test suite passes against Redpanda**

Run: `just test 2>&1 | tail -30`
Expected: all integration tests pass. Look specifically for `tests/test_integration.py::*` lines — these are the regression gate for the `FakeConsumer` class-name match (because they exercise a real `KafkaBroker` against Redpanda, NOT the test broker; if the FakeConsumer path silently broke during the migration, the real-broker tests still pass — *but* `tests/test_middleware.py` exercises the FakeConsumer path explicitly).

- [ ] **Step 4: Confirm test_middleware's FakeConsumer-related tests pass**

Run: `uv run --no-sync pytest tests/test_middleware.py -k "fake or sequential" -v`
Expected: PASS.

If this passes but Task 4 step 1 reported `FakeConsumer` was renamed, double-check Task 5 actually landed.

## Task 10: Stage and commit

**Files:** All modified files this PR.

- [ ] **Step 1: Review the cumulative diff**

Run: `git diff main..HEAD --stat`
Expected: a small number of files. Most likely:
- `pyproject.toml` (pin bump)
- `faststream_concurrent_aiokafka/middleware.py` (one or both of the FakeConsumer + consume_scope edits)
- Possibly: one or more files whose imports moved (Task 7).

- [ ] **Step 2: Stage all changes**

Run: `git add -A`

Then sanity-check:
Run: `git status --short`
Expected: only the files listed in Step 1. If `uv.lock` shows up, do not stage it — confirm `.gitignore` still has `uv.lock`.

- [ ] **Step 3: Commit**

Compose the commit body so each break point is enumerated with a file:line pointer. The Step 1 diff tells you what to enumerate. Template:

```bash
git commit -m "$(cat <<'EOF'
chore: migrate to faststream 0.7 (drop 0.6 support)

- pyproject.toml: faststream[kafka] >=0.6,<0.7 -> >=0.7,<0.8.
- <each break point, one bullet per file, with file:line pointer
  and a one-line explanation. E.g.:>
- middleware.py:41: update FakeConsumer string match to <NewName>
  (upstream renamed in 0.7).
- middleware.py:51: drop `# ty: ignore[invalid-method-override]` —
  upstream consume_scope signature now structurally matches.
- <etc>

Drops 0.6 support; users on 0.6 stay on previously-released wheels.
No new 0.7 features adopted (no broker-level AckPolicy default,
no multi-broker, etc.).
EOF
)"
```

If no source edits were required at all (i.e. Tasks 5, 6, 7 all skipped because 0.7 was structurally compatible), the body simplifies to:

```
- pyproject.toml: faststream[kafka] >=0.6,<0.7 -> >=0.7,<0.8.

0.7 is structurally compatible with this package; no source changes
required. Drops 0.6 support; users on 0.6 stay on previously-released
wheels.
```

- [ ] **Step 4: Verify commit**

Run: `git log -1 --stat`
Expected: single commit with the message above and the staged files.

## Task 11: Push and open PR

**Files:** None (git remote operation).

- [ ] **Step 1: Push branch**

Run: `git push -u origin chore/faststream-0.7-migration`
Expected: `* [new branch]` line and a hint URL for opening the PR.

- [ ] **Step 2: Open PR via gh**

```bash
gh pr create --title "chore: migrate to faststream 0.7 (drop 0.6 support)" --body "$(cat <<'EOF'
## Summary

- Bumps `faststream[kafka]` from `>=0.6,<0.7` to `>=0.7,<0.8`.
- Drops 0.6 support entirely — single code path, no compat shim.
- Pure compat migration — no new 0.7 features adopted.
- Break points (if any): see commit body for the enumerated list with file:line pointers.

## Test plan

- [ ] `just lint` passes (ruff + ty)
- [ ] `just test` passes at 100% coverage against a 0.7.x `faststream` resolve
- [ ] Integration tests (`tests/test_integration.py` against Redpanda) pass
- [ ] FakeConsumer fast-path tests (`tests/test_middleware.py -k "fake or sequential"`) pass
- [ ] Public import surface exits 0:
      `python -c "from faststream_concurrent_aiokafka import KafkaConcurrentProcessingMiddleware, initialize_concurrent_processing, stop_concurrent_processing, is_kafka_handler_healthy, ConsumerRebalanceListener"`
EOF
)"
```

- [ ] **Step 3: Note PR URL**

The `gh pr create` command outputs the PR URL. Record it for the user.

---

## Verification (acceptance gate)

The PR is done iff:

- [ ] `git diff main..chore/faststream-0.7-migration -- pyproject.toml` shows only the pin bump (`>=0.6,<0.7` → `>=0.7,<0.8`).
- [ ] Locally-resolved `faststream` (per `uv.lock`) is `0.7.x`.
- [ ] `just lint` clean on the branch tip.
- [ ] `just test` green at 100% coverage on the branch tip.
- [ ] `git grep -n "faststream\[kafka\]>=0.6,<0.7"` returns nothing.
- [ ] `git grep -n "faststream<0.7\|faststream~=0.6"` returns nothing.
- [ ] `uv run python -c "from faststream_concurrent_aiokafka import KafkaConcurrentProcessingMiddleware, initialize_concurrent_processing, stop_concurrent_processing, is_kafka_handler_healthy, ConsumerRebalanceListener"` exits 0.
- [ ] `tests/test_middleware.py` FakeConsumer-targeted tests pass (regression gate for the class-name match).
- [ ] PR is open with the prescribed title and body, and the commit body enumerates each break point with a file:line pointer (or notes "no source changes required" if 0.7 was structurally compatible).
