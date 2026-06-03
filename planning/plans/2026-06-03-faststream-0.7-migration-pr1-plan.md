# FastStream 0.7 migration — PR1 (defensive pin + planning scaffold) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tighten the FastStream dependency from unbounded `"faststream[kafka]"` to `"faststream[kafka]>=0.6,<0.7"`, adopt `--cov-fail-under=100`, and scaffold the `planning/` workflow directory mirroring `faststream-redis-timers` / `faststream-outbox`. Lands the PR2 design doc in the same commit so PR2 has a home.

**Architecture:** Pure config + docs change. No runtime code touched. Single commit on `chore/pin-faststream-pre-0.7`. The coverage gate is conditional: if `just test` is not at 100% today, the gate addition gets reverted and deferred to PR2 (recorded as a decision in the commit body). The local `uv.lock` currently resolves to `faststream 0.7.0` (verified) — the pin will actively downgrade the local environment to a 0.6.x release.

**Tech Stack:** `pyproject.toml`, `uv`, `pytest-cov`, markdown, `just`.

**Related spec:** `planning/specs/2026-06-03-faststream-0.7-migration-design.md`.

---

## Task 1: Create the working branch

**Files:** None (git operation).

- [ ] **Step 1: Confirm clean tree**

Run: `git status`
Expected: `nothing to commit, working tree clean` (a fresh `planning/` dir from the brainstorming session is acceptable — `.gitkeep` files and the design doc will be staged in Task 12).

- [ ] **Step 2: Create branch**

Run: `git switch -c chore/pin-faststream-pre-0.7`
Expected: `Switched to a new branch 'chore/pin-faststream-pre-0.7'`

## Task 2: Establish coverage baseline (precondition for gate adoption)

**Files:** None (read-only test run).

The spec's R7 flags that adopting `--cov-fail-under=100` in PR1 requires the suite to already hit 100%. This task discovers the baseline. Note: `just test` builds the Docker image and runs the full suite against a Redpanda container — expect a few minutes for cold starts.

- [ ] **Step 1: Run full suite via Justfile**

Run: `just test`
Expected: All tests pass. Final coverage line at bottom of output shows total `%` covered. The `pytest-cov` output ends with a `TOTAL` row.

- [ ] **Step 2: Record baseline**

Note the total coverage percentage from the report. There are three branches:

- **If `100%`:** Continue to Task 3 — gate adoption is safe.
- **If `<100%` AND missing lines are trivially testable:** Continue to Task 3 but write the backfill tests in a new Task 3a (insert before Task 4) before adding the gate.
- **If `<100%` AND gaps are non-trivial:** Skip Task 4 (gate adoption) entirely; remove the gate adoption from this PR's scope; record decision in the commit body in Task 12; gate moves to PR2.

## Task 3: Tighten the FastStream pin

**Files:**
- Modify: `pyproject.toml` (line 28)

- [ ] **Step 1: Read current dependencies block**

Run: `sed -n '26,30p' pyproject.toml`
Expected output:
```
version = "0"
dependencies = [
    "faststream[kafka]",
]
```

- [ ] **Step 2: Edit the pin**

Change `"faststream[kafka]"` on line 28 to `"faststream[kafka]>=0.6,<0.7"`.

- [ ] **Step 3: Verify edit**

Run: `grep -n '"faststream\[kafka\]' pyproject.toml`
Expected: `28:    "faststream[kafka]>=0.6,<0.7",`

## Task 4: Adopt `--cov-fail-under=100` (conditional on Task 2)

**Files:**
- Modify: `pyproject.toml` (line 85)

**Skip this task if Task 2 step 2 selected the third branch (`<100%`, non-trivial gaps).**

- [ ] **Step 1: Read current pytest config**

Run: `sed -n '84,88p' pyproject.toml`
Expected output:
```
[tool.pytest.ini_options]
addopts = "-n auto --cov=. --cov-report term-missing"
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
```

- [ ] **Step 2: Append the coverage gate to addopts**

Change `addopts = "-n auto --cov=. --cov-report term-missing"` to `addopts = "-n auto --cov=. --cov-report term-missing --cov-fail-under=100"`.

- [ ] **Step 3: Verify edit**

Run: `grep -n "addopts" pyproject.toml`
Expected: `85:addopts = "-n auto --cov=. --cov-report term-missing --cov-fail-under=100"`

## Task 5: Regenerate lockfile

**Files:**
- Touched (gitignored, not committed): `uv.lock`

`uv.lock` is gitignored in this repo (`.gitignore` line 19), so the file is local-only. The regeneration confirms the resolver is happy with the tightened pin and walks `faststream` back from the currently-resolved 0.7.0 to a 0.6.x release.

- [ ] **Step 1: Regenerate lock**

Run: `uv lock`
Expected: `Resolved N packages in <time>` with no error. The lock should now resolve `faststream` to a `0.6.x` version.

- [ ] **Step 2: Verify the resolved version**

Run: `grep -A 1 '^name = "faststream"' uv.lock | head -4`
Expected output (first three lines should be):
```
name = "faststream"
version = "0.6.x"
```
(Where `x` is whatever the latest 0.6 release is — `0.6.0` or later, but NOT `0.7.0`.)

If the version line shows `0.7.x`, the pin edit did not take. Re-check Task 3.

- [ ] **Step 3: Sync environment**

Run: `uv sync --all-extras --all-groups --frozen`
Expected: Sync succeeds against the frozen lock. Installed `faststream` is now 0.6.x.

## Task 6: Verify the gate (re-run suite)

**Files:** None.

- [ ] **Step 1: Re-run full suite under the new gate**

Run: `just test`
Expected: All tests pass. If Task 4 ran, the final line shows `Required test coverage of 100% reached`. If Task 4 was skipped, this is just a re-confirmation that the pin downgrade (0.7 → 0.6) did not break the suite.

If tests fail here that passed at Task 2, the source code may have been written against 0.7-specific behavior (R8 in the design). Investigate the failure, then decide: fix the regression, or revert PR1 and roll the fix into PR2.

## Task 7: Scaffold `planning/` (likely already present from spec-writing)

**Files:**
- Create (if missing): `planning/specs/.gitkeep`
- Create (if missing): `planning/plans/.gitkeep`

- [ ] **Step 1: Check current state**

Run: `ls -la planning/specs planning/plans 2>&1`
Expected: Both directories already exist with `.gitkeep` files and a design doc in `planning/specs/`. (Created at spec-writing time.)

- [ ] **Step 2: Touch any missing `.gitkeep` files**

If either `.gitkeep` is missing:
Run: `touch planning/specs/.gitkeep planning/plans/.gitkeep`

## Task 8: Confirm design + plan docs are on disk

**Files:**
- Verify: `planning/specs/2026-06-03-faststream-0.7-migration-design.md`
- Verify: `planning/plans/2026-06-03-faststream-0.7-migration-pr1-plan.md` (this file)
- Verify: `planning/plans/2026-06-03-faststream-0.7-migration-pr2-plan.md`

- [ ] **Step 1: List planning files**

Run: `ls planning/specs planning/plans`
Expected output:
```
planning/plans:
.gitkeep
2026-06-03-faststream-0.7-migration-pr1-plan.md
2026-06-03-faststream-0.7-migration-pr2-plan.md

planning/specs:
.gitkeep
2026-06-03-faststream-0.7-migration-design.md
```

If any of the three markdown files is missing, regenerate it via the matching skill (writing-plans for the plan files, brainstorming for the spec).

## Task 9: Add `## Workflow` section to CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Read current CLAUDE.md to confirm anchors**

Run: `grep -n "^## " CLAUDE.md`
Expected output:
```
5:## Commands
24:## Architecture
52:## Key patterns
58:## Integration tests
```

- [ ] **Step 2: Insert `## Workflow` between `## Commands` and `## Architecture`**

Read lines 22-25 to find the boundary:

Run: `sed -n '22,25p' CLAUDE.md`
Expected: lines 22 and 23 are the closing backticks + blank line of the `## Commands` block; line 24 is `## Architecture`.

Insert the following block immediately above line 24 (`## Architecture`), with a trailing blank line so the result is "blank — `## Workflow` block — blank — `## Architecture`":

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

- [ ] **Step 3: Verify section order**

Run: `grep -n "^## " CLAUDE.md`
Expected output:
```
5:## Commands
24:## Workflow
36:## Architecture
64:## Key patterns
70:## Integration tests
```
(Line numbers approximate; the important thing is `## Workflow` appears between `## Commands` and `## Architecture` and the relative order of the others is preserved.)

## Task 10: Verify gitignore does not eat new plan files

**Files:** None (read-only check).

- [ ] **Step 1: Sanity check the gitignore rule**

The repo's `.gitignore` line 20 has a bare `plan.md` entry. Bare basenames in `.gitignore` match files of that exact name at any depth — they do NOT match `planning/plans/2026-06-03-...-plan.md` (different basename). Verify:

Run: `git check-ignore -v planning/plans/2026-06-03-faststream-0.7-migration-pr1-plan.md`
Expected: exit code `1` (no output) — the file is NOT ignored.

If exit code is `0` and output names `plan.md` as the rule: the gitignore rule is matching too aggressively (would surprise me but verify). In that case, edit `.gitignore` to anchor the rule: change the bare `plan.md` line to `/plan.md` so it only matches the root.

## Task 11: Run lint

**Files:** None.

- [ ] **Step 1: Run lint**

Run: `just lint`
Expected: All four steps (`eof-fixer`, `ruff format`, `ruff check --fix`, `ty check`) green. Note: `eof-fixer` may auto-add newlines to `.gitkeep`, `CLAUDE.md`, or the new markdown files — re-stage afterward if it modifies anything.

If `ty check` reports errors that did not exist before the pin change, the local environment may still be on 0.7 (Task 5 step 3 didn't complete). Re-run `uv sync --all-extras --all-groups --frozen` and try again.

## Task 12: Stage and commit

**Files:** All modified/created files this PR.

- [ ] **Step 1: Stage changes**

Run:
```bash
git add pyproject.toml \
        CLAUDE.md \
        planning/
```

- [ ] **Step 2: Confirm staged files**

Run: `git status --short`
Expected output (order may vary):
```
M  CLAUDE.md
M  pyproject.toml
A  planning/plans/.gitkeep
A  planning/plans/2026-06-03-faststream-0.7-migration-pr1-plan.md
A  planning/plans/2026-06-03-faststream-0.7-migration-pr2-plan.md
A  planning/specs/.gitkeep
A  planning/specs/2026-06-03-faststream-0.7-migration-design.md
```

If `uv.lock` shows up in `git status` despite being gitignored, do **not** stage it — confirm `.gitignore` still lists `uv.lock` (line 19).

- [ ] **Step 3: Verify pyproject diff is clean**

Run: `git diff --cached pyproject.toml`
Expected: only the two single-line edits (pin + addopts, or just pin if Task 4 was skipped). No unrelated changes.

- [ ] **Step 4: Commit**

```bash
git commit -m "$(cat <<'EOF'
chore: pin faststream <0.7 and adopt planning/ workflow

- pyproject.toml: faststream[kafka] (unbounded) -> >=0.6,<0.7. Explicit
  range walks the local resolve back from the currently-resolved 0.7.0
  to a 0.6.x release and lets the 0.7 migration land in a feature branch
  without users pulling unfinished work.
- pyproject.toml: addopts adds --cov-fail-under=100 (matches sister
  faststream-redis-timers / faststream-outbox projects). Hardens the
  coverage gate before the 0.7 migration (PR2) disturbs anything.
- planning/: scaffold planning/specs/ + planning/plans/ with .gitkeep
  files; mirrors the sister projects' layout. Includes the faststream-0.7
  migration design and the two plan files so PR2 has a home.
- CLAUDE.md: new ## Workflow section between ## Commands and
  ## Architecture pointing at the planning/ layout.

Companion PR migrates to faststream 0.7 and drops 0.6 support
(>=0.7,<0.8).
EOF
)"
```

(If Task 4 was skipped, drop the `--cov-fail-under=100` bullet from the body.)

- [ ] **Step 5: Verify commit**

Run: `git log -1 --stat`
Expected: single commit with the message above and the staged files. No unstaged changes remain that should have been part of this commit.

## Task 13: Push and open PR

**Files:** None (git remote operation).

- [ ] **Step 1: Push branch**

Run: `git push -u origin chore/pin-faststream-pre-0.7`
Expected: `* [new branch]` line and a hint URL for opening the PR.

- [ ] **Step 2: Open PR via gh**

```bash
gh pr create --title "chore: pin faststream <0.7 and adopt planning/ workflow" --body "$(cat <<'EOF'
## Summary

- Tightens the FastStream dep from unbounded `"faststream[kafka]"` to `"faststream[kafka]>=0.6,<0.7"`. Local `uv.lock` previously resolved to `faststream 0.7.0`; this pin walks it back to 0.6.x. The companion PR migrates to 0.7 and drops 0.6 support.
- Adopts `--cov-fail-under=100` (matches sister `faststream-redis-timers` and `faststream-outbox` projects).
- Scaffolds `planning/specs/` and `planning/plans/` and lands the 0.7 migration design + two-PR plan files.
- Adds a `## Workflow` section to `CLAUDE.md` pointing at the new layout.

## Test plan

- [ ] `just lint` passes
- [ ] `just test` passes at 100% coverage (against a 0.6.x `faststream` resolve)
- [ ] `planning/specs/` and `planning/plans/` are tracked by git
- [ ] `CLAUDE.md` shows the new `## Workflow` section between `## Commands` and `## Architecture`
EOF
)"
```

(If Task 4 was skipped: drop the second bullet of the Summary block and the `at 100% coverage` clause from the second test-plan line.)

- [ ] **Step 3: Note PR URL**

The `gh pr create` command outputs the PR URL. Record it for the user.

---

## Verification (acceptance gate)

The PR is done iff:

- [ ] `git diff main..chore/pin-faststream-pre-0.7 -- pyproject.toml` shows only the pin edit and (if applicable) the `--cov-fail-under=100` addition.
- [ ] `git grep -n '"faststream\[kafka\]",'` returns nothing on the branch (no unbounded dep).
- [ ] `git grep -n 'faststream\[kafka\]>=0.7'` returns nothing on the branch (no 0.7 pin yet — that's PR2).
- [ ] `planning/specs/2026-06-03-faststream-0.7-migration-design.md` exists and is tracked.
- [ ] `planning/plans/2026-06-03-faststream-0.7-migration-pr1-plan.md` (this file) exists and is tracked.
- [ ] `planning/plans/2026-06-03-faststream-0.7-migration-pr2-plan.md` exists and is tracked.
- [ ] `planning/specs/.gitkeep` and `planning/plans/.gitkeep` exist and are tracked.
- [ ] `CLAUDE.md` contains the `## Workflow` section between `## Commands` and `## Architecture`.
- [ ] `just lint` clean on the branch tip.
- [ ] `just test` green (at 100% if Task 4 ran).
- [ ] Locally-resolved `faststream` (per `uv.lock`) is `0.6.x`, not `0.7.x`.
- [ ] PR is open with the prescribed title and body.
