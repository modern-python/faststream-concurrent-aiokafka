# Planning

Specs, plans, and change history for `faststream-concurrent-aiokafka`. The
living truth about *what the system does now* lives in
[`architecture/`](../architecture/) at the repo root; this directory records
*how it got there*.

## Conventions

> This section is the portable convention — identical across the
> modern-python repos. The Index below is repo-specific. To adopt elsewhere,
> copy this section plus [`_templates/`](_templates/) and point that repo's
> `CLAUDE.md` Workflow + truth home at it.

### Two axes, never mixed

- **`architecture/` (repo root) — the present.** One file per capability,
  living prose, updated whenever a change ships. The truth home.
- **`planning/changes/` — the past-and-pending.** One folder per change,
  frozen once shipped.

Shipping a change **promotes** its conclusions into the affected
`architecture/<capability>.md` by hand, then archives the bundle. That
hand-edit is what keeps `architecture/` true; the archived bundle carries the
*why*.

### Change bundles

A change is a folder `changes/active/YYYY-MM-DD.NN-<slug>/`:

- `YYYY-MM-DD` — proposal date; `.NN` — zero-padded intra-day counter
  (`.01`, `.02`, …) that breaks same-date ties so the timeline sorts stably.
- `<slug>` — kebab-case description, not a story ID.

On merge the folder moves to `changes/archive/` with `status: shipped`, `pr:`,
and `outcome:` filled, and its line moves from **Active** to **Archived** in
the Index below.

### Three lanes

| Lane | Artifacts | Use when |
|------|-----------|----------|
| **Full** | `design.md` + `plan.md` | design judgment; new file/module; public-API change; cross-cutting/multi-file; non-trivial test design |
| **Lightweight** | `change.md` | small-but-real: ≲30 LOC net, ≤2 files, no new file, no public-API change, single straightforward test |
| **Tiny** | none — conventional commit | typo, dep bump, linter/formatter/CI tweak, mechanical rename, single-line config |

Heavier lane wins on ambiguity. A `change.md` that outgrows its lane splits
into `design.md` + `plan.md`.

### Artifacts at a glance

- **`design.md`** — the spec: the *thinking* (why, design, trade-offs, scope).
- **`plan.md`** — the plan: the *sequencing* (the executor's task checklist).
- **`change.md`** — both, condensed, for the lightweight lane.
- **`releases/<semver>.md`** — per-release user-facing notes.
- **`audits/<date>-<slug>.md`** — findings from a code/docs/bug-hunt sweep;
  spawns fix changes.
- **`retros/<date>-<slug>.md`** — what we learned after a body of work.
- **`deferred.md`** — real-but-unscheduled items, each with a revisit trigger.

Templates live in [`_templates/`](_templates/).

### Frontmatter

`design.md` / `change.md`: `status` (draft|approved|shipped|superseded),
`date`, `slug`, `supersedes`, `superseded_by`, `pr`, `outcome`.
`plan.md`: `status`, `date`, `slug`, `spec`, `pr`. Files in `architecture/`
carry **no** frontmatter — living prose, dated by git.

## Index

### Active

_None._

### Archived (shipped)

- **[portable-planning-convention](changes/archive/2026-06-13.03-portable-planning-convention/design.md)**
  (#34, 2026-06-13) — Adopt the portable two-axis convention: `architecture/`
  truth home + `changes/` bundles, fresh Index.
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
