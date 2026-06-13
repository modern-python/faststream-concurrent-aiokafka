---
status: shipped
date: 2026-06-13
slug: portable-planning-convention
spec: portable-planning-convention
pr: "34"
---

# Portable planning convention — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate `faststream-concurrent-aiokafka`'s `planning/` to the portable
two-axis convention from `faststream-outbox` — `architecture/` truth home +
`planning/changes/` bundles — and seed the truth home.

**Spec:** [`design.md`](./design.md)

**Architecture:** Pure file moves + new docs. Create `architecture/` (4 seed
files), restructure `planning/` into `changes/{active,archive}/` bundles +
empty `audits/` + `retros/`, copy the portable README Conventions + templates
byte-identical from `faststream-outbox`, author a fresh Index, rewrite
`CLAUDE.md`'s `## Workflow` (preserving the #33 release-notes step), and remove
the `.gitignore` `plan.md` trap. No runtime/test/API code is touched. This repo
has **no mkdocs site**, so there is no `docs-build` step.

**Tech Stack:** Markdown, `git mv`, `just lint-ci`.

**Branch:** `docs/portable-planning-convention` (already created; `design.md` is
already committed there).

**Source repo (copy-from):** `/Users/kevinsmith/src/pypi/faststream-outbox`.

**Commit strategy:** Per-task commits.

**`.NN` assignment (final, by merge order):**

| Bundle id | design.md source | plan.md source | PR |
|-----------|------------------|----------------|----|
| `2026-06-03.01-faststream-0.7-migration` | `specs/2026-06-03-faststream-0.7-migration-design.md` | — (design-only) | #28 |
| `2026-06-04.01-faststream-0.7.1-testbroker-typing` | `specs/2026-06-04-faststream-0.7.1-testbroker-typing-design.md` | `plans/2026-06-04-faststream-0.7.1-testbroker-typing-plan.md` | #29 |
| `2026-06-13.01-robustness-docs-test-audit` | `specs/2026-06-13-robustness-docs-test-audit-design.md` | `plans/2026-06-13-robustness-docs-test-audit-plan.md` | #32 |
| `2026-06-13.02-codify-release-notes` | `specs/2026-06-13-codify-release-notes-design.md` | `plans/2026-06-13-codify-release-notes-plan.md` | #33 |

All commands assume CWD `/Users/kevinsmith/src/pypi/faststream-concurrent-aiokafka`.

---

### Task 1: Remove the `plan.md` ignore trap + scaffold the skeleton + copy templates

**Files:**
- Modify: `.gitignore` (delete line 22, `plan.md`)
- Create: `planning/changes/active/.gitkeep`, `planning/audits/.gitkeep`, `planning/retros/.gitkeep`
- Create: `planning/_templates/{design,plan,change}.md` (copied byte-identical)
- Create: `planning/deferred.md`

- [ ] **Step 1: Remove the stray `plan.md` ignore rule**

  `.gitignore:22` ignores the literal filename `plan.md` — it collides with
  every change bundle's `plan.md` under the new convention. Delete the line:

  ```bash
  sed -i '' '/^plan\.md$/d' .gitignore
  git check-ignore planning/changes/active/2026-06-13.03-portable-planning-convention/plan.md && echo "STILL IGNORED (bad)" || echo "plan.md no longer ignored"
  ```
  Expected: `plan.md no longer ignored`.

- [ ] **Step 2: Create the directory skeleton**

  ```bash
  mkdir -p planning/changes/active planning/changes/archive planning/audits planning/retros planning/_templates
  touch planning/changes/active/.gitkeep planning/audits/.gitkeep planning/retros/.gitkeep
  ```

- [ ] **Step 3: Copy the three templates byte-identical**

  ```bash
  cp /Users/kevinsmith/src/pypi/faststream-outbox/planning/_templates/design.md planning/_templates/design.md
  cp /Users/kevinsmith/src/pypi/faststream-outbox/planning/_templates/plan.md   planning/_templates/plan.md
  cp /Users/kevinsmith/src/pypi/faststream-outbox/planning/_templates/change.md planning/_templates/change.md
  ```

- [ ] **Step 4: Verify templates are byte-identical**

  ```bash
  diff /Users/kevinsmith/src/pypi/faststream-outbox/planning/_templates/design.md planning/_templates/design.md \
    && diff /Users/kevinsmith/src/pypi/faststream-outbox/planning/_templates/plan.md planning/_templates/plan.md \
    && diff /Users/kevinsmith/src/pypi/faststream-outbox/planning/_templates/change.md planning/_templates/change.md \
    && echo "TEMPLATES IDENTICAL"
  ```
  Expected: `TEMPLATES IDENTICAL` (no diff output).

- [ ] **Step 5: Create `planning/deferred.md`**

  Write `planning/deferred.md` with exactly this content:

  ```markdown
  # Deferred Work

  Items raised in reviews or audits that are real but not actionable now.
  Each is parked here with the reason it's deferred and the concrete trigger
  that should bring it back. This is the long-tail register — not a backlog
  of planned work. When an item is picked up it graduates to a change bundle
  in [`changes/active/`](changes/active/); see [CLAUDE.md](../CLAUDE.md#workflow).

  ## Open

  _None._
  ```

- [ ] **Step 6: Commit**

  ```bash
  git add .gitignore planning/changes planning/audits planning/retros planning/_templates planning/deferred.md
  git commit -m "docs(planning): scaffold changes/audits/retros/_templates; drop plan.md ignore trap

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 2: Seed `architecture/` truth home (4 capability files)

**Files:**
- Create: `architecture/concurrent-handler.md`
- Create: `architecture/batch-committer.md`
- Create: `architecture/middleware-lifecycle.md`
- Create: `architecture/rebalance.md`

**Source to synthesize from:** `CLAUDE.md` `## Architecture` section (already in
this repo — read it before writing). Living prose, **no frontmatter** (dated by
git). Each file describes what the capability does *now* — invariants, not
change history. Aim for ~2–5 KB each, matching the granularity of
`faststream-outbox/architecture/*.md`. Use `file.py:line` references where
`CLAUDE.md` does.

- [ ] **Step 1: Write `architecture/concurrent-handler.md`**

  Title `# Concurrent handler`. Cover (from `CLAUDE.md` → `processing.py —
  KafkaConcurrentHandler`):
  - One handler per `initialize_concurrent_processing`, stored in FastStream's
    `ContextRepo` under key `"concurrent_processing"`; **not** a singleton —
    `stop_concurrent_processing` clears the entry so a fresh handler can start.
  - State it owns: `asyncio.Semaphore` (concurrency limit, minimum 1); the
    `set[asyncio.Task]` `_tracked_tasks`; the per-task done-callback
    `_finish_task` (releases the semaphore, removes the task from the set); a
    `KafkaBatchCommitter`.
  - `handle_task()` fires-and-forgets the user coroutine as an asyncio task and
    enqueues a `KafkaCommitTask`; offsets are not committed until the user task
    finishes (**at-least-once**).
  - `stop()`: cancels every in-flight tracked task, then awaits
    `committer.close()`; cancelled tasks are a hard offset boundary (cancelled +
    after stay uncommitted → redelivered on restart); shutdown wall-clock bounded
    by the committer's `shutdown_timeout_sec` (default 20 s), sub-second normally.
  - No signal handlers — shutdown is driven by the FastStream lifespan calling
    `stop_concurrent_processing`.

- [ ] **Step 2: Write `architecture/batch-committer.md`**

  Title `# Batch committer`. Cover (from `CLAUDE.md` → `batch_committer.py —
  KafkaBatchCommitter`):
  - Runs as a background asyncio task (`spawn()`); streaming loop absorbs
    `KafkaCommitTask`s into per-partition pending state.
  - Commit triggers: total pending ≥ `commit_batch_size`,
    `commit_batch_timeout_sec` fires, or `commit_all`/`close` sets the flush event.
  - `_extract_ready_prefixes` sorts by offset (tolerates re-queued tasks landing
    out of order), stops at the first not-done task; a cancelled task is a hard
    boundary (cancelled + after dropped from pending);
    `_map_offsets_per_partition` stops the offset advance at the cancelled task.
  - Per consumer-id group: `consumer.commit({TopicPartition: max_offset+1})`.
  - Error policy: transient `KafkaError` re-queues the batch;
    `CommitFailedError` / `IllegalStateError` (rebalance/revocation) discards it;
    `CommitterIsDeadError` is raised to callers when the main task has died →
    triggers `handler.stop()`.
  - `max_uncommitted_tasks` backpressure (default 10000, `None` disables):
    `send_task` admits/blocks; the count releases once a task is committed or
    dropped; hitting the ceiling also nudges a flush; keep
    `max_uncommitted_tasks >= commit_batch_size`.

- [ ] **Step 3: Write `architecture/middleware-lifecycle.md`**

  Title `# Middleware & lifecycle`. Cover (from `CLAUDE.md` → `middleware.py` and
  `healthcheck.py`):
  - `KafkaConcurrentProcessingMiddleware` (FastStream `BaseMiddleware`):
    `consume_scope` retrieves the handler from `self.context`. Pass-through
    cases: (a) FakeConsumer (`TestKafkaBroker`); (b) any subscriber whose ack
    policy is not MANUAL (`kafka_message.committed is not None`). Refuses if
    `_enable_auto_commit=True`. Batch raw messages (`isinstance(self.msg, (list,
    tuple))`) raise a clear `RuntimeError` (batch subscribers unsupported), after
    the pass-throughs. If the handler is stopped, logs a warning and skips the
    message (offset stays uncommitted → redelivered on restart).
  - `initialize_concurrent_processing(context, ...)`: create + start a handler,
    store under `"concurrent_processing"`.
  - `stop_concurrent_processing(context)`: gates on `is_running`; calls
    `handler.stop()`; clears the context entry; safe when the committer task has
    already died.
  - `is_kafka_handler_healthy(context)` (`healthcheck.py`): `True` iff the
    handler is present and `is_healthy` (`_is_running` AND committer task alive);
    intended for readiness/liveness probes.

- [ ] **Step 4: Write `architecture/rebalance.md`**

  Title `# Rebalance listener`. Cover (from `CLAUDE.md` → `rebalance.py` and the
  `flush_timeout_sec` notes):
  - `ConsumerRebalanceListener` returned by
    `handler.create_rebalance_listener(flush_timeout_sec=...)`.
  - On `on_partitions_revoked`, calls `committer.commit_all()` to flush completed
    offsets before the partition is reassigned, preventing duplicate processing
    after rebalance.
  - The flush is bounded by `flush_timeout_sec` (default 10 s, comfortably under
    aiokafka's default `max.poll.interval.ms` of 300 s); on timeout it logs a
    warning and returns — already-completed offsets commit, in-flight stay
    uncommitted (at-least-once; duplicates only on the timeout path).

- [ ] **Step 5: Verify the four files exist and are non-empty**

  ```bash
  wc -l architecture/*.md
  ```
  Expected: four files, each with substantive line counts (not empty).

- [ ] **Step 6: Commit**

  ```bash
  git add architecture/
  git commit -m "docs(architecture): seed truth home (handler, committer, middleware, rebalance)

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 3: Migrate the four change bundles into `changes/archive/`

**Files:** four bundle folders under `planning/changes/archive/`; `git mv` each
design (+ plan where present); prepend YAML frontmatter; fix the two plan `Spec`
links.

- [ ] **Step 1: `git mv` the designs and plans into bundle folders**

  ```bash
  # 2026-06-03.01 faststream-0.7-migration (design-only)
  mkdir -p planning/changes/archive/2026-06-03.01-faststream-0.7-migration
  git mv planning/specs/2026-06-03-faststream-0.7-migration-design.md planning/changes/archive/2026-06-03.01-faststream-0.7-migration/design.md

  # 2026-06-04.01 faststream-0.7.1-testbroker-typing
  mkdir -p planning/changes/archive/2026-06-04.01-faststream-0.7.1-testbroker-typing
  git mv planning/specs/2026-06-04-faststream-0.7.1-testbroker-typing-design.md planning/changes/archive/2026-06-04.01-faststream-0.7.1-testbroker-typing/design.md
  git mv planning/plans/2026-06-04-faststream-0.7.1-testbroker-typing-plan.md   planning/changes/archive/2026-06-04.01-faststream-0.7.1-testbroker-typing/plan.md

  # 2026-06-13.01 robustness-docs-test-audit
  mkdir -p planning/changes/archive/2026-06-13.01-robustness-docs-test-audit
  git mv planning/specs/2026-06-13-robustness-docs-test-audit-design.md planning/changes/archive/2026-06-13.01-robustness-docs-test-audit/design.md
  git mv planning/plans/2026-06-13-robustness-docs-test-audit-plan.md    planning/changes/archive/2026-06-13.01-robustness-docs-test-audit/plan.md

  # 2026-06-13.02 codify-release-notes
  mkdir -p planning/changes/archive/2026-06-13.02-codify-release-notes
  git mv planning/specs/2026-06-13-codify-release-notes-design.md planning/changes/archive/2026-06-13.02-codify-release-notes/design.md
  git mv planning/plans/2026-06-13-codify-release-notes-plan.md   planning/changes/archive/2026-06-13.02-codify-release-notes/plan.md
  ```

- [ ] **Step 2: Prepend frontmatter to each `design.md`**

  Each design currently opens with a prose `# Title`. Insert a YAML frontmatter
  block at the very top (above the `#`), leaving the body intact.

  `…/2026-06-03.01-faststream-0.7-migration/design.md`:
  ```yaml
  ---
  status: shipped
  date: 2026-06-03
  slug: faststream-0.7-migration
  supersedes: null
  superseded_by: null
  pr: "28"
  outcome: "merged as #28 (plan removed post-execution; design-only bundle)"
  ---
  ```

  `…/2026-06-04.01-faststream-0.7.1-testbroker-typing/design.md`:
  ```yaml
  ---
  status: shipped
  date: 2026-06-04
  slug: faststream-0.7.1-testbroker-typing
  supersedes: null
  superseded_by: null
  pr: "29"
  outcome: "merged as #29"
  ---
  ```

  `…/2026-06-13.01-robustness-docs-test-audit/design.md`:
  ```yaml
  ---
  status: shipped
  date: 2026-06-13
  slug: robustness-docs-test-audit
  supersedes: null
  superseded_by: null
  pr: "32"
  outcome: "merged as #32; shipped in 0.6.0"
  ---
  ```

  `…/2026-06-13.02-codify-release-notes/design.md`:
  ```yaml
  ---
  status: shipped
  date: 2026-06-13
  slug: codify-release-notes
  supersedes: null
  superseded_by: null
  pr: "33"
  outcome: "merged as #33"
  ---
  ```

- [ ] **Step 3: Prepend frontmatter to each `plan.md` (the three that exist)**

  `…/2026-06-04.01-faststream-0.7.1-testbroker-typing/plan.md`:
  ```yaml
  ---
  status: shipped
  date: 2026-06-04
  slug: faststream-0.7.1-testbroker-typing
  spec: faststream-0.7.1-testbroker-typing
  pr: "29"
  ---
  ```

  `…/2026-06-13.01-robustness-docs-test-audit/plan.md`:
  ```yaml
  ---
  status: shipped
  date: 2026-06-13
  slug: robustness-docs-test-audit
  spec: robustness-docs-test-audit
  pr: "32"
  ---
  ```

  `…/2026-06-13.02-codify-release-notes/plan.md`:
  ```yaml
  ---
  status: shipped
  date: 2026-06-13
  slug: codify-release-notes
  spec: codify-release-notes
  pr: "33"
  ---
  ```

- [ ] **Step 4: Repoint the two plan `Spec` links to `./design.md`**

  Both migrated plans link their spec by the old `../specs/...` path. Fix each:

  In `…/2026-06-04.01-faststream-0.7.1-testbroker-typing/plan.md`, replace:
  ```
  **Spec:** [`planning/specs/2026-06-04-faststream-0.7.1-testbroker-typing-design.md`](../specs/2026-06-04-faststream-0.7.1-testbroker-typing-design.md)
  ```
  with:
  ```
  **Spec:** [`design.md`](./design.md)
  ```

  In `…/2026-06-13.02-codify-release-notes/plan.md`, replace:
  ```
  Spec: [`planning/specs/2026-06-13-codify-release-notes-design.md`](../specs/2026-06-13-codify-release-notes-design.md).
  ```
  with:
  ```
  Spec: [`design.md`](./design.md).
  ```

  (The robustness plan references its spec by section name, not a relative link —
  no change. The `../specs/<date>-<slug>-design.md` placeholder on line 40 of the
  codify plan is archived prose quoting the release template and is left
  verbatim — see Non-goals.)

- [ ] **Step 5: Verify frontmatter parses on all bundle design/plan files**

  ```bash
  for f in planning/changes/archive/*/design.md planning/changes/archive/*/plan.md; do
    uv run python -c "import sys,yaml; t=open('$f').read(); assert t.startswith('---'),'$f'; yaml.safe_load(t.split('---')[1]); print('OK','$f')"
  done
  ```
  Expected: `OK` for all seven files (four designs + three plans).

- [ ] **Step 6: Commit**

  ```bash
  git add planning/changes planning/specs planning/plans
  git commit -m "docs(planning): migrate the four shipped changes into archive bundles

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 4: Remove the old dirs + repoint the live cross-links

**Files:**
- Remove: `planning/plans/.gitkeep`, now-empty `planning/specs/`, `planning/plans/`
- Modify: `planning/releases/0.6.0.md` (audit-spec link), `planning/releases/TEMPLATE.md` (spec-link placeholder)

- [ ] **Step 1: Remove the leftover `.gitkeep` and empty dirs**

  ```bash
  git rm planning/plans/.gitkeep
  rmdir planning/specs planning/plans 2>/dev/null || true
  ls planning/
  ```
  Expected listing: `_templates  audits  changes  deferred.md  releases  retros`
  (no `specs`, no `plans`; `README.md` is added in Task 5).

- [ ] **Step 2: Repoint the `0.6.0.md` audit-spec link**

  In `planning/releases/0.6.0.md`, replace:
  ```
  Audit spec: [`planning/specs/2026-06-13-robustness-docs-test-audit-design.md`](../specs/2026-06-13-robustness-docs-test-audit-design.md).
  ```
  with:
  ```
  Audit spec: [`planning/changes/archive/2026-06-13.01-robustness-docs-test-audit/design.md`](../changes/archive/2026-06-13.01-robustness-docs-test-audit/design.md).
  ```

- [ ] **Step 3: Repoint the `TEMPLATE.md` spec-link placeholder**

  In `planning/releases/TEMPLATE.md`, replace:
  ```
  Spec: [`planning/specs/<date>-<slug>-design.md`](../specs/<date>-<slug>-design.md).
  ```
  with:
  ```
  Spec: [`planning/changes/archive/<YYYY-MM-DD.NN-slug>/design.md`](../changes/archive/<YYYY-MM-DD.NN-slug>/design.md).
  ```

- [ ] **Step 4: Verify no live link points at the old paths**

  ```bash
  grep -rn "(\.\./specs/\|(\.\./plans/" planning/releases && echo "STILL BROKEN (bad)" || echo "release links repointed"
  ```
  Expected: `release links repointed`.

- [ ] **Step 5: Commit**

  ```bash
  git add planning/
  git commit -m "docs(planning): drop empty specs/plans dirs; repoint release links to bundles

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 5: Author `planning/README.md` (Conventions byte-identical + fresh Index)

**Files:**
- Create: `planning/README.md`

- [ ] **Step 1: Extract the byte-identical Conventions block**

  ```bash
  awk '/^## Conventions/{f=1} /^## Index/{f=0} f' \
    /Users/kevinsmith/src/pypi/faststream-outbox/planning/README.md > /tmp/conventions.md
  head -1 /tmp/conventions.md   # should print: ## Conventions
  ```

- [ ] **Step 2: Assemble `planning/README.md` from three parts**

  Write `planning/README.md` as: the intro below, then the **exact** contents of
  `/tmp/conventions.md` (do not edit a word), then the Index + Other below.

  Intro (top of file):
  ```markdown
  # Planning

  Specs, plans, and change history for `faststream-concurrent-aiokafka`. The
  living truth about *what the system does now* lives in
  [`architecture/`](../architecture/) at the repo root; this directory records
  *how it got there*.
  ```

  Index + Other (after the Conventions block):
  ```markdown
  ## Index

  ### Active

  - **[portable-planning-convention](changes/active/2026-06-13.03-portable-planning-convention/design.md)**
    (2026-06-13) — Adopt the portable two-axis convention: `architecture/` truth
    home + `changes/` bundles, fresh Index. *This change.*

  ### Archived (shipped)

  - **[codify-release-notes](changes/archive/2026-06-13.02-codify-release-notes/design.md)**
    (#33, 2026-06-13) — Codify `planning/releases/` as a workflow step; add the
    release-notes template.
  - **[robustness-docs-test-audit](changes/archive/2026-06-13.01-robustness-docs-test-audit/design.md)**
    (#32, 2026-06-13) — Rebalance-flush timeout, batch-subscriber guard,
    uncommitted-task backpressure, plus docs/test/refactor. Shipped in 0.6.0.
  - **[faststream-0.7.1-testbroker-typing](changes/archive/2026-06-04.01-faststream-0.7.1-testbroker-typing/design.md)**
    (#29, 2026-06-04) — Adopt FastStream 0.7.1's `TestBroker` typing fix; drop
    `# ty: ignore` directives.
  - **[faststream-0.7-migration](changes/archive/2026-06-03.01-faststream-0.7-migration/design.md)**
    (#28, 2026-06-03) — Migrate to `faststream>=0.7` (drop 0.6 support).
    Design-only bundle (execution plan removed post-merge).

  ## Other

  - **[`architecture/`](../architecture/)** at the repo root — the living
    capability truth (concurrent handler, batch committer, middleware &
    lifecycle, rebalance listener). The promotion target on every ship.
  - **[releases/](releases/)** — per-release user-facing notes, plus
    [`releases/TEMPLATE.md`](releases/TEMPLATE.md), a repo-specific release-notes
    template (not part of the portable core).
  - **[audits/](audits/)** — findings reports from code/docs/bug-hunt sweeps
    (none yet).
  - **[retros/](retros/)** — what we learned after a body of work (none yet).
  - **[deferred.md](deferred.md)** — the long-tail register of real-but-
    unscheduled items with revisit triggers.
  ```

- [ ] **Step 3: Verify the Conventions block is byte-identical**

  ```bash
  diff <(awk '/^## Conventions/{f=1} /^## Index/{f=0} f' /Users/kevinsmith/src/pypi/faststream-outbox/planning/README.md) \
       <(awk '/^## Conventions/{f=1} /^## Index/{f=0} f' planning/README.md) \
    && echo "CONVENTIONS IDENTICAL"
  ```
  Expected: `CONVENTIONS IDENTICAL` (no diff).

- [ ] **Step 4: Verify every Index/Other link resolves**

  ```bash
  grep -oE '\]\(([^)]+\.md)\)' planning/README.md | sed -E 's/\]\(|\)//g' | while read p; do
    case "$p" in
      ../*) [ -f "planning/$p" ] || echo "BROKEN: $p" ;;
      *) [ -f "planning/$p" ] || echo "BROKEN: $p" ;;
    esac
  done; echo "done"
  ```
  Expected: only `done` (no `BROKEN:` lines).

- [ ] **Step 5: Commit**

  ```bash
  git add planning/README.md
  git commit -m "docs(planning): add README — portable Conventions + fresh Index

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 6: Rewrite `CLAUDE.md` `## Workflow` (preserve the release-notes step)

**Files:**
- Modify: `CLAUDE.md` (`## Workflow` section, currently lines 24–39)

- [ ] **Step 1: Replace the whole `## Workflow` section**

  Replace everything from the `## Workflow` line up to (but **not** including) the
  `## Architecture` line with this exact text:

  ```markdown
  ## Workflow

  Per-feature: brainstorming → spec in `planning/changes/active/YYYY-MM-DD.NN-<slug>/design.md` → writing-plans → plan in `planning/changes/active/YYYY-MM-DD.NN-<slug>/plan.md` → executing-plans / subagent-driven-development → requesting-code-review → finishing-a-development-branch. Each change is a folder bundle; `<slug>` is a kebab-case description, not a story ID; `.NN` is a zero-padded intra-day counter that breaks same-date ties so the timeline sorts stably. On merge, the bundle moves to `planning/changes/archive/` with `status: shipped`, `pr:`, and `outcome:` filled, **and the change promotes its conclusions into the affected `architecture/<capability>.md`** — that hand-edit is what keeps `architecture/` true. See [`planning/README.md`](planning/README.md) for the conventions + index and [`planning/_templates/`](planning/_templates/) for copy-and-fill starting points.

  **Spec** (`design.md`) captures the *thinking* — why, what the design is, trade-offs, scope. Written before code; rarely revised after merge. **Plan** (`plan.md`) captures the *sequencing* — the ordered checklist an executor walks; references the spec for the "why". **`architecture/`** captures the *invariants* of shipped systems — the living truth, promoted from a change on merge. A plan paragraph that would still read correctly with all task numbers and checkboxes removed is design content and belongs in the spec.

  **Three lanes.** Scale the artifact to the change. **Full** — a `design.md` + `plan.md` bundle — for real design judgment, a new file/module, a public-API change, cross-cutting/multi-file work, or non-trivial test design. **Lightweight** — a single `change.md` — for small-but-real changes (≲30 LOC net, ≤2 files, no new file, no public-API change, a single straightforward test). **Tiny** — no bundle, just a conventional commit — for a typo, dep bump, linter/formatter/CI tweak, a mechanical rename, or a single-line config change. Heavier lane wins on ambiguity; a `change.md` that outgrows its lane splits into `design.md` + `plan.md`.

  Release notes are written when cutting a release (not per-feature): copy `planning/releases/TEMPLATE.md` to `planning/releases/<version>.md` (bare version, no `v` prefix) and link back to the driving change bundle under `planning/changes/`.

  ```

- [ ] **Step 2: Verify the section reads cleanly and `## Architecture` still follows**

  ```bash
  grep -n -A 9 "^## Workflow" CLAUDE.md
  grep -n "^## Architecture" CLAUDE.md
  ```
  Expected: the new four-paragraph Workflow section; `## Architecture` still
  present immediately after it. No leftover `planning/specs/` or `planning/plans/`
  mentions in the Workflow section.

- [ ] **Step 3: Commit**

  ```bash
  git add CLAUDE.md
  git commit -m "docs: rewrite CLAUDE.md Workflow for the change-bundle convention

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```

---

### Task 7: Full verification sweep

**Files:** none (verification only).

- [ ] **Step 1: Lint (markdown/format gate; no code changed)**

  ```bash
  just lint-ci
  ```
  Expected: passes (eof-fixer --check, ruff format --check, ruff check --no-fix,
  ty all green). If eof-fixer flags a new markdown file missing a trailing
  newline, run `just lint` to autofix, then re-stage and amend/commit.

- [ ] **Step 2: Stale-pointer sweep (CLAUDE.md / README / justfile only)**

  ```bash
  grep -rn "planning/specs\|planning/plans" CLAUDE.md README.md justfile 2>/dev/null \
    && echo "STALE POINTER (review)" || echo "no stale pointers in CLAUDE.md/README/justfile"
  ```
  Expected: `no stale pointers in CLAUDE.md/README/justfile`. (Mentions inside
  migrated archive prose under `planning/changes/archive/` are historical and
  acceptable.)

- [ ] **Step 3: Final tree check**

  ```bash
  ls -R architecture planning | sed -n '1,60p'
  ```
  Expected: `architecture/` has the four `.md` files; `planning/` has
  `README.md`, `_templates/`, `audits/`, `changes/{active,archive}`, `deferred.md`,
  `releases/`, `retros/`; **no** `specs/`, `plans/`, or `templates/`.
  `changes/active/` holds only this convention bundle (+ `.gitkeep`);
  `changes/archive/` holds the four migrated bundles.

- [ ] **Step 4: Frontmatter parse sweep across every bundle file**

  ```bash
  for f in $(find planning/changes -name design.md -o -name plan.md); do
    uv run python -c "import yaml; t=open('$f').read(); assert t.startswith('---'),'$f'; yaml.safe_load(t.split('---')[1])" && echo "OK $f" || echo "BAD $f"
  done
  ```
  Expected: `OK` for every `design.md` / `plan.md` (the active convention bundle +
  all archived bundles). No `BAD` lines.

- [ ] **Step 5: Commit any lint autofix**

  ```bash
  git add -A && git commit -m "docs(planning): lint/format fixups from convention migration

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>" || echo "nothing to commit"
  ```

---

### Task 8: On merge — self-migrate this bundle from `active/` to `archive/`

This convention bundle self-migrates (it defines the convention, so **no
`architecture/` promotion applies**). Do it within the shipping PR so merged
`main` lands in its final archived state.

- [ ] **Step 1: Move the bundle and update its frontmatter**

  ```bash
  git mv planning/changes/active/2026-06-13.03-portable-planning-convention \
         planning/changes/archive/2026-06-13.03-portable-planning-convention
  ```
  Then in both `design.md` and `plan.md` of the moved bundle set `status:
  shipped` and `pr: "<this PR number>"`; in `design.md` also set `outcome:
  "ships in #<PR>; defines the convention, no architecture/ promotion"`.

- [ ] **Step 2: Move its README Index line from Active to Archived**

  In `planning/README.md`, move the `portable-planning-convention` entry from
  `### Active` to the top of `### Archived (shipped)`, change its path from
  `changes/active/...` to `changes/archive/...`, add the PR number, and drop the
  *This change.* suffix. Leave `### Active` as `_None._`.

- [ ] **Step 3: Verify and commit**

  ```bash
  grep -rn "changes/active/2026-06-13.03" planning/README.md && echo "STALE ACTIVE LINK (bad)" || echo "index updated"
  git add planning/
  git commit -m "docs(planning): archive the portable-planning-convention bundle

  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
  ```
  Expected: `index updated`.

---

## Self-Review

- **Spec coverage:**
  - Design §2 layout → Tasks 1, 3, 4 (skeleton, bundles, dir removal). ✓
  - Design §3 four `architecture/` files → Task 2. ✓
  - Design §4 migration mapping (4 bundles, `.NN`, frontmatter, design-only
    0.7-migration) → Task 3. ✓
  - Design §5 README (byte-identical Conventions + fresh Index/Other) → Task 5. ✓
  - Design §6 `_templates/` byte-identical → Task 1 Steps 3–4. ✓
  - Design §7 CLAUDE.md Workflow + preserved release-notes step → Task 6. ✓
  - Design §8 `.gitignore` trap → Task 1 Step 1. ✓
  - Design §9 `deferred.md` → Task 1 Step 5. ✓
  - Design §10 dogfood self-migration → Task 8. ✓
  - Non-goals (no docs-build, no releases/ move, no bundle backfill for #30/#31,
    no archived-prose rewrite) → respected; no task violates them. ✓
  - Testing (lint-ci, grep sweeps, frontmatter parse, byte-identical diff, tree)
    → Task 7. ✓
- **Placeholders:** The `<YYYY-MM-DD.NN-slug>` / `<version>` / `<this PR number>`
  markers are intentional convention placeholders, not plan gaps; every concrete
  step has exact paths, commands, and content. `architecture/` seed steps give
  per-file section outlines bound to named `CLAUDE.md` content and code symbols.
- **Naming consistency:** bundle ids, slugs, and frontmatter `slug`/`spec` values
  match across Tasks 3, 5, and 8; `commit_batch_size` / `max_uncommitted_tasks` /
  `flush_timeout_sec` match `CLAUDE.md`.
