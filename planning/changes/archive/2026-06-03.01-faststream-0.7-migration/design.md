---
status: shipped
date: 2026-06-03
slug: faststream-0.7-migration
supersedes: null
superseded_by: null
pr: "28"
outcome: "merged as #28 (plan removed post-execution; design-only bundle)"
---

# Design: FastStream 0.7 migration (two-PR split)

**Date:** 2026-06-03
**Status:** Approved
**Slug:** `faststream-0.7-migration`

## Summary

Migrate `faststream-concurrent-aiokafka` from an unbounded `faststream[kafka]`
dependency (which currently resolves to `0.7.0` in fresh environments) to
`faststream[kafka]>=0.7,<0.8` in **two PRs against `main`**:

1. **PR1 — `chore/pin-faststream-pre-0.7`**: Defensive pin. Tighten the
   dependency to `faststream[kafka]>=0.6,<0.7`, regenerate `uv.lock`, adopt
   `--cov-fail-under=100`, and scaffold the `planning/` workflow directory
   (mirroring the sister `faststream-redis-timers` and `faststream-outbox`
   projects). This design doc ships in the same commit so PR2 has a home
   immediately.
2. **PR2 — `chore/faststream-0.7-migration`** (off `main` after PR1 merges):
   Bump to `faststream[kafka]>=0.7,<0.8`. Drop 0.6 support entirely (no compat
   shim). Fix whatever the migration surfaces — likely a small set of import
   paths, the `BaseMiddleware.consume_scope` override signature, and the
   `FakeConsumer` class-name string match. Single bundled commit.

No new 0.7 features (broker-level `AckPolicy` default, multi-broker, MQTT,
Redis Cluster broker) are adopted.

## Motivation

FastStream 0.7.0 is released. The current `pyproject.toml` declares
`"faststream[kafka]"` with no version constraint at all — fresh resolves
already pull 0.7.0 (verified locally: `uv.lock` shows `faststream 0.7.0`).
That means the project is silently exposed to whatever breaking changes 0.7
ships, with no documented intent and no migration commit on record.

PR1 makes the supported range explicit and walks the local environment back
to 0.6.x so the suite has a stable known-good baseline before PR2 takes the
break through review. PR2 then aligns the package with the same upstream
surface the sister `faststream-redis-timers` and `faststream-outbox`
migrations target.

The PR1/PR2 split (vs. a single PR) is policy carried over from the sister
projects: ship the safety pin in minutes, then take the migration through
review on its own merits. The package is at version `"0"` (sentinel /
pre-release) with no stability promise, so a hard break on 0.6 support is in
policy provided it is documented in the commit/PR.

## Scope decisions

- **Drop 0.6 support entirely.** Single code path, no `_compat.py` shim.
  Users still on 0.6 stay on the currently-released wheel.
- **Pure compat migration.** No adoption of new 0.7 features. Whatever
  mechanical breaks 0.7 surfaces are the entire scope of PR2.
- **Add a lower bound in PR1.** The current dep `"faststream[kafka]"` has no
  lower bound at all. PR1 sets `>=0.6,<0.7` (not just `<0.7`) so the
  resolvable range is bounded on both sides.
- **PR2 ships as a single bundled commit.** Splitting could create an
  incoherent intermediate state if a single failure mode requires changes
  in multiple files together.
- **Adopt `--cov-fail-under=100` in PR1.** Matches the sister projects'
  baseline and hardens the gate before PR2 disturbs anything; PR2's R1
  (coverage drops from removed branches, if any) then red-fails CI rather
  than degrading silently.
- **Conditional gate adoption.** If the current suite does *not* already
  hit 100% line coverage, the gate addition is either backfilled in PR1
  (preferred) or deferred to PR2 (recorded as a decision in the commit
  body). The PR1 implementation plan starts by running `just test` and
  inspecting the report.

## Project-specific surface differences from the sister migrations

The sister `faststream-redis-timers` and `faststream-outbox` migrations
touched a much larger 0.7-affected surface because those packages subclass
`Producer`, subclass `TestBroker`, and expose a `Registrator`/`Router` with
public per-call `middlewares=` kwargs. This package does **none** of those:

- **No `Producer` subclass.** The package adds no producer, so the new
  `codec: CodecProto` attribute on `ProducerProto` in 0.7 is not an issue.
- **No `TestKafkaBroker` subclass.** The new
  `create_publisher_fake_subscriber` instance form (vs. staticmethod) in
  0.7 is not an issue.
- **No `Registrator` / `Router` subclass exposing per-call `middlewares=`.**
  Upstream's removal of that kwarg from `subscriber()` / `publisher()` in
  0.7 is not an issue.
- **No `faststream._internal.*` imports.** Verified via
  `grep -rn "_internal" faststream_concurrent_aiokafka/ tests/` returning
  nothing. So 0.7's internal reorganisation is not an issue for this
  package's source — the surface is entirely public.

What the package *does* touch that 0.7 may break:

- `faststream_concurrent_aiokafka/middleware.py:50` —
  `KafkaConcurrentProcessingMiddleware(BaseMiddleware)` overrides
  `consume_scope(self, call_next, msg)`. The override already carries
  `# ty: ignore[invalid-method-override]` in 0.6, indicating the upstream
  signature is structurally awkward. 0.7 may have refined it.
- `faststream_concurrent_aiokafka/middleware.py:41` —
  `type(consumer).__name__ == "FakeConsumer"`. This is a string match,
  not an isinstance check. If 0.7 renamed the class (e.g., to `MockConsumer`
  or moved it to a different module), this check silently returns `False`,
  the test-broker fast path stops triggering, and integration tests against
  `TestKafkaBroker` will fail noisily on the missing handler context.
- Public imports the package and tests use:
  - `from faststream import BaseMiddleware, ContextRepo`
  - `from faststream.kafka import ConsumerRecord, TopicPartition, KafkaBroker, KafkaRouter, TestKafkaBroker`
  - `from faststream.kafka.message import KafkaAckableMessage`
  - `from faststream.middlewares import AckPolicy` (tests)
  - `from faststream.asgi import AsgiFastStream` (tests)

  Each of these is a public module path. The expectation is that none move,
  but the PR2 implementation step grep-checks the installed 0.7 wheel
  before assuming.

## Design

### PR1 — `chore/pin-faststream-pre-0.7`

#### `pyproject.toml`
- `dependencies`: `"faststream[kafka]"` → `"faststream[kafka]>=0.6,<0.7"`.
- `[tool.pytest.ini_options].addopts`: append `--cov-fail-under=100`.
- **Precondition for the gate:** current suite must already hit 100% line
  coverage when run via `just test`. If not, either fill the gaps in PR1
  (preferred — keeps PR1 cohesive as "lock + scaffold + raise quality bar")
  or revert the gate change and defer to PR2. PR1 implementation step 1 is
  running `just test` and inspecting the coverage report.

#### `uv.lock`
- `uv.lock` is gitignored in this repo (verified — `.gitignore` line 19).
  Regenerate locally via `uv lock` (no `--upgrade`) to confirm the resolver
  accepts the tightened pin and walks `faststream` back from the currently
  resolved 0.7.0 to a 0.6.x release. The file does not ship in the commit.
- After regeneration, run `uv sync` so the local virtualenv reflects the
  pin; then re-run `just test` to confirm the suite is green on 0.6.x.

#### `planning/specs/`, `planning/plans/`
- New directories with `.gitkeep` files (zero-byte). The `.gitkeep` in
  `planning/specs/` is defensive but technically redundant once this design
  doc populates the dir.

#### `planning/specs/2026-06-03-faststream-0.7-migration-design.md`
- This design doc, committed alongside the pin so PR2 has a home.

#### `CLAUDE.md`
- Insert `## Workflow` section between `## Commands` and `## Architecture`
  (this project's section ordering — there is no `## Tests` section, unlike
  the sister projects):

  ```markdown
  ## Workflow

  Per-feature workflow: brainstorming → spec in
  `planning/specs/YYYY-MM-DD-<slug>-design.md` → writing-plans →
  plan in `planning/plans/YYYY-MM-DD-<slug>-plan.md` →
  executing-plans / subagent-driven-development →
  requesting-code-review → finishing-a-development-branch.

  Topic slugs are kebab-case descriptions (e.g. `faststream-0.7-migration`),
  not story IDs.
  ```

#### `.gitignore`
- No change. The existing `plan.md` entry targets a literal basename;
  gitignore patterns without slashes match basenames exactly at any depth,
  so `plan.md` matches only files literally named `plan.md` — never
  `planning/plans/2026-06-03-<slug>-plan.md`. Verified via
  `git check-ignore -v planning/plans/...` during implementation.

### PR2 — `chore/faststream-0.7-migration`

#### `pyproject.toml`
- `dependencies`: `"faststream[kafka]>=0.6,<0.7"` → `"faststream[kafka]>=0.7,<0.8"`.

#### `uv.lock`
- `uv.lock` is gitignored. Regenerate locally via `uv lock --upgrade` to
  resolve `faststream` to a `0.7.x` release; the file does not ship in the
  commit.

#### Discovery loop — `faststream_concurrent_aiokafka/`
After bumping the pin, run `just lint && just test` and treat every error
as a discrete fix. Expected failure modes (in rough order of likelihood):

- **`middleware.py:50` — `consume_scope` signature.** If 0.7 changed the
  `BaseMiddleware.consume_scope` signature (the override already uses
  `# ty: ignore[invalid-method-override]` in 0.6), the ignore comment may
  need updating or removing. If the signature *types* changed (e.g. a new
  required parameter, a different `call_next` shape), update the override
  to match. If the structural override mismatch is gone, drop the
  `# ty: ignore` comment.
- **`middleware.py:41` — `FakeConsumer` string match.** Run
  `python -c "from faststream.kafka import testing; print([n for n in dir(testing) if 'onsumer' in n])"`
  against the installed 0.7 wheel. If `FakeConsumer` still exists under
  that exact name, no change needed. If renamed, update the literal in
  `middleware.py:41`. If the class itself was removed in favour of a
  different mechanism, the test-broker fast path needs to be re-derived
  (likely by introspecting a different attribute on the consumer, or by
  checking `kafka_message` rather than `consumer`).
- **`faststream.kafka.message.KafkaAckableMessage` import.** If the
  module moved (e.g. to `faststream.kafka.subscriber.message`), update the
  import in `processing.py:6` and `middleware.py:9`.
- **`faststream.kafka.{ConsumerRecord,TopicPartition}` imports.** Same
  approach — fix any moved symbol.
- **`AckPolicy.MANUAL` import path in tests.** `tests/test_integration.py`
  imports from `faststream.middlewares`. Verify the path and member name
  survived. If renamed, update.
- **`faststream.asgi.AsgiFastStream` import.** Verify the path in
  `tests/test_integration.py:8`.

#### Tests
- `tests/` files exercise the public surface (broker, router, middleware,
  ASGI app). No subscriber-level `middlewares=` kwarg appears in any
  subscriber decorator (verified via
  `grep -rn 'middlewares=' tests/` — only `KafkaRouter(middlewares=[...])`,
  which is broker-scope and unchanged in 0.7).
- `--cov-fail-under=100` (adopted in PR1) is the hard gate: every removed
  code branch must have its coverage line gone, and every new branch
  (e.g., a refactored `consume_scope` body, if any) must be exercised.

#### Docs
- `README.md` — spot-check for any 0.6-specific import paths or examples.
  Verified at spec-writing time: README references only the public
  package surface (`KafkaConcurrentProcessingMiddleware`,
  `initialize_concurrent_processing`, etc.) and does not import from
  `faststream._internal.*` or use any kwarg removed in 0.7. Re-verify
  during PR2 implementation in case intervening commits added one.

## Verification

### PR1

PR1 is done iff:

- Local `uv lock` regenerates without resolving `faststream>=0.7`
  (lockfile is gitignored — verified by inspecting the local lock, not
  by diff).
- `uv sync` succeeds against the regenerated lock and downgrades the
  installed `faststream` from 0.7.0 to a 0.6.x release.
- `just lint` clean.
- `just test` green (Redpanda-backed integration suite — unaffected by a
  pin tightening that excludes a version not yet adopted in source).
- `git grep -n '"faststream\[kafka\]"$\|faststream\[kafka\]>=0.7'` returns
  nothing on the branch (no unbounded dep, no 0.7 pin yet).
- `planning/specs/2026-06-03-faststream-0.7-migration-design.md` exists in
  the working tree and is tracked.
- `planning/specs/.gitkeep` and `planning/plans/.gitkeep` exist.
- `CLAUDE.md` contains the new `## Workflow` section between `## Commands`
  and `## Architecture`.

### PR2

PR2 is done iff:

- Local `uv lock --upgrade` resolves `faststream` to a `0.7.x` release.
- `uv sync` succeeds against the regenerated lock.
- `just lint` clean (ruff + ty).
- `just test` green at `--cov-fail-under=100`.
- `git grep -n "faststream\[kafka\]>=0.6,<0.7\|faststream<0.7" .` returns
  nothing.
- `python -c "from faststream_concurrent_aiokafka import KafkaConcurrentProcessingMiddleware, initialize_concurrent_processing, stop_concurrent_processing, is_kafka_handler_healthy, ConsumerRebalanceListener"`
  exits 0.
- `tests/test_integration.py` passes against Redpanda (the
  `FakeConsumer` fast path and the `KafkaAckableMessage` path are both
  exercised — the integration suite is the regression gate for the
  `consumer` class-name match).

## Risk register

- **R1 — Coverage drops.** If 0.7 collapses any branches the package's
  middleware exercises (e.g. by changing `consume_scope`'s call shape),
  the `--cov-fail-under=100` gate (adopted in PR1) catches orphaned
  branches by red-failing CI. Mitigation: normal TDD-on-test cleanup as
  part of PR2.
- **R2 — `BaseMiddleware.consume_scope` signature change.** The override
  in `middleware.py:50` carries `# ty: ignore[invalid-method-override]`,
  meaning even on 0.6 the signature doesn't structurally match upstream.
  0.7 may have tightened the protocol, requiring either an updated
  override or a refactor to match. Mitigation: implementation plan reads
  0.7's `BaseMiddleware` source before editing.
- **R3 — `FakeConsumer` class-name rename.** `middleware.py:41` uses a
  string compare on `type(consumer).__name__`. A rename in 0.7 silently
  breaks the test-broker fast path. Mitigation: implementation plan
  inspects `faststream.kafka.testing` in the installed 0.7 wheel before
  assuming. Integration tests against `TestKafkaBroker` (which use this
  path) are the regression gate.
- **R4 — `KafkaAckableMessage` / `ConsumerRecord` / `TopicPartition`
  module moves.** Public path moves would be a single-line import fix per
  site. Mitigation: rely on lint + test pipeline; each is a one-line fix.
- **R5 — `AckPolicy.MANUAL` rename or move.** Same as R4 but in tests.
  Single-line fix if renamed.
- **R6 — `AsgiFastStream` import path.** Same as R4 but in
  `tests/test_integration.py`. Single-line fix if renamed.
- **R7 — Baseline coverage may be <100%.** If `just test` does not
  already reach 100%, the gate addition either forces in-PR coverage
  backfill (acceptable scope creep for PR1) or moves to PR2. Mitigation:
  PR1 implementation plan starts by running the suite and inspecting the
  report.
- **R8 — Local lock currently resolves to 0.7.0.** Developer
  environments that ran `uv sync` recently are already on 0.7. After PR1
  merges and contributors pull, their next `uv lock && uv sync` will
  downgrade. Anyone who continued developing on 0.7 between now and PR1
  merge may have written code that breaks on the downgrade. Mitigation:
  ship PR1 immediately; PR2 follows on a short cycle.

## Unknowns the implementation plan will resolve

1. Whether the current suite already reaches 100% coverage (gates whether
   PR1 can adopt `--cov-fail-under=100` directly).
2. Whether `FakeConsumer` survived under that exact name and module path
   in 0.7.
3. Whether `BaseMiddleware.consume_scope`'s signature changed in 0.7, and
   if so, whether the `# ty: ignore` comment can be dropped or needs
   updating.
4. Whether `KafkaAckableMessage`, `ConsumerRecord`, `TopicPartition`,
   `AckPolicy`, or `AsgiFastStream` moved module paths.

Each is a small inspection step; none materially shifts the design.

## Out of scope (deferred to follow-up specs)

- Adopting broker-level `AckPolicy` default (per-broker default that
  subscribers inherit unless overridden).
- Adopting multi-broker capability (run `KafkaBroker` alongside another
  in a single `FastStream` app).
- Adopting `RedisClusterBroker` or any other new transport.
- MQTT or any other new transport.
- README/docs rewrite beyond mechanical fixes for any moved import paths.
- Any `CHANGELOG.md` entry — none exists; package version is `"0"`.

## Order of operations

### PR1 — `chore/pin-faststream-pre-0.7`

1. `git switch -c chore/pin-faststream-pre-0.7`.
2. `just test` — record baseline coverage. Decide gate adoption branch
   per "Conditional gate adoption" above.
3. `pyproject.toml`: `"faststream[kafka]"` →
   `"faststream[kafka]>=0.6,<0.7"`; append `--cov-fail-under=100` to
   pytest `addopts` (if step 2 supported it).
4. `uv lock && uv sync` (lockfile is gitignored — local resolve only).
   Confirm local `faststream` walks back from 0.7.0 to a 0.6.x release.
5. `just test` — confirm green at 100% (if gate adopted) or just green
   (if deferred).
6. `mkdir -p planning/specs planning/plans` (already done as part of
   writing this spec).
7. `touch planning/specs/.gitkeep planning/plans/.gitkeep`.
8. Confirm `planning/specs/2026-06-03-faststream-0.7-migration-design.md`
   is staged (this file).
9. Edit `CLAUDE.md`: insert `## Workflow` section between `## Commands`
   and `## Architecture`.
10. `just lint`.
11. Single commit:
    `chore: pin faststream <0.7 and adopt planning/ workflow`.
    Body lists the pin change, the coverage-gate adoption (or its
    deferral), and the planning-dir layout.
12. Push; open PR. Body re-states the pin reason ("guard users against
    the silent pull of unfinished 0.7 work the unbounded dep currently
    allows; companion PR migrates to 0.7 and drops 0.6 support").

### PR2 — `chore/faststream-0.7-migration` (off `main` after PR1 merges)

1. `git switch main && git pull && git switch -c chore/faststream-0.7-migration`.
2. `pyproject.toml`: `"faststream[kafka]>=0.6,<0.7"` →
   `"faststream[kafka]>=0.7,<0.8"`.
3. `uv lock --upgrade && uv sync` (lockfile is gitignored — local
   resolve only).
4. Inspect 0.7's surface for the affected sites:
   - `python -c "from faststream.kafka import testing; print([n for n in dir(testing) if 'onsumer' in n])"`
     to verify `FakeConsumer` name.
   - `python -c "from faststream import BaseMiddleware; help(BaseMiddleware.consume_scope)"`
     to verify the override signature.
   - `python -c "from faststream.kafka.message import KafkaAckableMessage"`
     etc. to verify each public import path.
5. Fix import/attribute errors as ty and ruff surface them. Expected
   sites: `middleware.py:50` (override), `middleware.py:41` (name
   match), and any moved public import.
6. `just lint && just test` until green at 100% coverage.
7. Single commit:
   `chore: migrate to faststream 0.7 (drop 0.6 support)`.
   Body enumerates each break point with file pointers (or notes "no
   break points required" if 0.7 is structurally compatible — possible
   but unlikely given this package's public-only surface).
8. Open PR; body re-states the drop of 0.6 support for downstream
   consumers.

## Acceptance criteria

- Both PRs' verification commands pass.
- PR1 commit message documents the pin + planning scaffold + coverage
  gate (or its deferral).
- PR2 commit message documents each break point (or its absence).
- No grep hit for an unbounded `"faststream[kafka]"` dep, for
  `"faststream[kafka]>=0.6,<0.7"`, or for `"faststream<0.7"` after PR2.
