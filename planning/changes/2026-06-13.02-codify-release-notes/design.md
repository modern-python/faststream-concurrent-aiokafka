---
status: shipped
date: 2026-06-13
slug: codify-release-notes
summary: Codify `planning/releases/` as a workflow step; add the release-notes template.
supersedes: null
superseded_by: null
pr: "33"
outcome: "merged as #33"
---

# Codify `planning/releases/` as a workflow step

**Status:** approved · **Date:** 2026-06-13

## Problem

The `0.6.0` release added `planning/releases/0.6.0.md` — a rich, structured
release-notes document. It is the first and only artifact of its kind in the
repo, and the convention it embodies is undocumented:

- **CLAUDE.md's Workflow section** ends at
  `... → requesting-code-review → finishing-a-development-branch`. It never
  mentions `planning/releases/`, so the release-notes step exists only as one
  example file rather than as a documented part of the workflow.
- **There is no template.** The next release has nothing to copy from except
  `0.6.0.md` itself, so its structure will drift.

## Goal

Make the release-notes pattern shipped in `0.6.0.md` a documented, repeatable
part of the per-feature/release workflow, so the next release reliably matches
its shape. This is purely codification — no new format is invented; the
template is descriptive of what `0.6.0.md` already does.

## Non-goals

- Backfilling release notes for the 16 prior tags (`0.0.1`–`0.5.1`). They
  shipped without notes; nobody is asking for them retroactively.
- Changing the release/publish CI (`.github/workflows/publish.yml`,
  `just publish`).
- Touching the `planning/specs/` or `planning/plans/` conventions.

## Deliverables

### 1. `planning/releases/TEMPLATE.md`

A skeleton capturing `0.6.0.md`'s structure with placeholders and a one-line
guidance comment per section. Sections, in order:

1. `# <pkg> <version> — <headline>` — title with a short headline.
2. **Summary paragraph** — release type (major/minor/patch), whether there are
   breaking changes, and any changed defaults.
3. **Spec link** — `[`planning/specs/<date>-<slug>-design.md`](...)` back to the
   driving spec.
4. **Numbered behavioral changes** — each a `## N. <change>` with
   `### The gap` / `### The fix` subsections.
5. `## Packaging`, `## Docs`, `## Tests`, `## Internal` — include only those
   that apply to a given release.
6. `## Upgrade notes` — call out changed defaults, new errors, migration steps.
7. `## Release process` — the tag-and-release note (GitHub release with bare
   tag → `publish.yml` → `just publish`).

The template's section set must be a superset of what `0.6.0.md` uses, so
`0.6.0.md` reads as a valid instance of the template.

### 2. CLAUDE.md Workflow edit

Extend the per-feature chain with a trailing release-notes step:

> ... → requesting-code-review → finishing-a-development-branch → **release
> notes in `planning/releases/<version>.md`** (from
> `planning/releases/TEMPLATE.md`) on version bump.

Add one sentence clarifying that release notes are written when cutting a
release (not per-feature), and that each notes file links back to its driving
spec in `planning/specs/`.

## Verification

- The template's sections are a superset of those in `planning/releases/0.6.0.md`.
- The CLAUDE.md workflow chain reads cleanly and the new step is unambiguous.
- No code, no tests — docs only.
