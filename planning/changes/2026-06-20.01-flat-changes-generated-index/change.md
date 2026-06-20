---
status: shipped
date: 2026-06-20
slug: flat-changes-generated-index
summary: Flatten changes/ (drop active/archive), make status frontmatter the sole lifecycle state, add a summary field, and replace the hand-maintained README Index with a stdlib generator (just index).
supersedes: null
superseded_by: null
pr: TBD-AFTER-PR
outcome: Flattened planning/changes/; backfilled summary: into every bundle; added stdlib planning/index.py + just index; slimmed README Index to a generator note; ported the single-step in-branch lifecycle into CLAUDE.md.
---

# flat-changes-generated-index

Adopts the planning change shipped in `faststream-outbox` (#105/#106): the flat
`changes/` directory, `status:`-as-sole-lifecycle, the `summary:` frontmatter
field, and the generated index (`just index`). See that repo's
`planning/changes/2026-06-20.01-flat-changes-generated-index/design.md` for the
full rationale. `pr:` is backfilled once the PR opens.
