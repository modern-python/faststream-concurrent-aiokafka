# Codify `planning/releases/` as a Workflow Step — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the release-notes pattern shipped in `planning/releases/0.6.0.md` a documented, repeatable part of the project workflow.

**Architecture:** Two docs-only changes — add a `planning/releases/TEMPLATE.md` skeleton that is descriptive of `0.6.0.md`'s existing structure, and extend the CLAUDE.md Workflow chain with a trailing release-notes step. No code, no automated tests; verification is by inspection.

**Tech Stack:** Markdown only.

Spec: [`planning/specs/2026-06-13-codify-release-notes-design.md`](../specs/2026-06-13-codify-release-notes-design.md).

---

### Task 1: Create the release-notes template

**Files:**
- Create: `planning/releases/TEMPLATE.md`
- Reference (do not modify): `planning/releases/0.6.0.md`

- [ ] **Step 1: Write `planning/releases/TEMPLATE.md`**

Create the file with exactly this content:

````markdown
<!--
Release-notes template for faststream-concurrent-aiokafka.
Copy to planning/releases/<version>.md when cutting a release (bare version,
no `v` prefix). Fill placeholders; delete any Packaging/Docs/Tests/Internal
section that does not apply to the release. See planning/releases/0.6.0.md for
a worked example.
-->

# faststream-concurrent-aiokafka <version> — <headline>

**<Major|Minor|Patch> release. <one-line summary of what changed>.** <State
whether there are breaking API changes, and call out any defaults that
changed behavior — point readers at Upgrade notes.>

Spec: [`planning/specs/<date>-<slug>-design.md`](../specs/<date>-<slug>-design.md).

## 1. <Behavioral change title> (`<new_param_or_symbol>`)

**<One-line impact statement.>**

### The gap

<What the previous behavior was and why it was a problem.>

### The fix

<What changed, the new default, and the resulting semantics.>

<!-- Repeat "## N. ..." with The gap / The fix for each behavioral change. -->

## Packaging

- <Packaging/distribution changes — files added, metadata, etc. Omit section if none.>

## Docs

- <Documentation changes. Omit section if none.>

## Tests

- <Test changes; note total test count / coverage if relevant. Omit section if none.>

## Internal

- <Refactors and CI with no public-API or behavior change. Omit section if none.>

## Upgrade notes

<Drop-in or not? List every changed default, new error, and migration step a
consumer must know about. Omit section only if truly nothing to note.>

## Release process

Tag-and-release: publishing runs on a GitHub **release: published** event
(`.github/workflows/publish.yml` → `just publish`, which sets the version from
the tag name). Create a GitHub release with tag **`<version>`** (bare, no `v`
prefix) targeting `main`; the workflow builds and publishes to PyPI.
````

- [ ] **Step 2: Verify the template is a superset of `0.6.0.md`**

Run: `grep -E '^(#|##|###) ' planning/releases/0.6.0.md`
Then: `grep -E '^(#|##|###) ' planning/releases/TEMPLATE.md`

Expected: every heading shape present in `0.6.0.md` (title, `## N.` numbered
changes with `### The gap` / `### The fix`, `## Packaging`, `## Docs`,
`## Tests`, `## Internal`, `## Upgrade notes`, `## Release process`) has a
corresponding placeholder in the template. If any section in `0.6.0.md` is
missing from the template, add it before continuing.

- [ ] **Step 3: Commit**

```bash
git add planning/releases/TEMPLATE.md
git commit -m "docs(releases): add release-notes template"
```

---

### Task 2: Document the release-notes step in CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` (the `## Workflow` section)

- [ ] **Step 1: Read the current Workflow section**

Run: `grep -n -A 12 '^## Workflow' CLAUDE.md`
Expected: the per-feature chain ending in
`requesting-code-review → finishing-a-development-branch.` followed by the
"Topic slugs" paragraph.

- [ ] **Step 2: Extend the workflow chain**

In `CLAUDE.md`, replace this exact text:

```
executing-plans / subagent-driven-development →
requesting-code-review → finishing-a-development-branch.
```

with:

```
executing-plans / subagent-driven-development →
requesting-code-review → finishing-a-development-branch →
release notes in `planning/releases/<version>.md`.
```

- [ ] **Step 3: Add the release-notes clarifying sentence**

In `CLAUDE.md`, immediately after the "Topic slugs are kebab-case..." paragraph
in the `## Workflow` section, add this new paragraph:

```
Release notes are written when cutting a release (not per-feature):
copy `planning/releases/TEMPLATE.md` to `planning/releases/<version>.md`
(bare version, no `v` prefix) and link back to the driving spec in
`planning/specs/`.
```

- [ ] **Step 4: Verify the edit reads cleanly**

Run: `grep -n -A 16 '^## Workflow' CLAUDE.md`
Expected: the chain now ends with the `planning/releases/<version>.md` step,
and the new clarifying paragraph appears below the Topic-slugs paragraph. No
duplicated or dangling arrows.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add release-notes step to the workflow"
```

---

## Self-Review

- **Spec coverage:**
  - Deliverable 1 (`planning/releases/TEMPLATE.md`) → Task 1. ✓
  - Deliverable 2 (CLAUDE.md Workflow edit + clarifying sentence) → Task 2. ✓
  - Non-goals (no backfill, no CI change, no specs/plans changes) → respected;
    no task touches them. ✓
  - Verification (template superset of `0.6.0.md`; CLAUDE.md reads cleanly) →
    Task 1 Step 2, Task 2 Step 4. ✓
- **Placeholders:** The `<...>` markers in the template are intentional
  authoring placeholders for future releases, not plan gaps; all plan steps
  contain concrete content and exact commands.
- **Naming consistency:** `planning/releases/TEMPLATE.md` and
  `planning/releases/<version>.md` are referred to identically across both
  tasks and the spec.
