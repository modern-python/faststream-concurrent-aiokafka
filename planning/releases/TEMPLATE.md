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

Spec: [`planning/changes/<YYYY-MM-DD.NN-slug>.md`](../changes/<YYYY-MM-DD.NN-slug>.md).

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

Tag-driven (`.github/workflows/release.yml`): pushing a bare semver tag
**`<version>`** (no `v` prefix) to `main` runs `just publish` — which derives the
version from the tag name — publishing to PyPI via Trusted Publishing (OIDC) and
then creating the matching GitHub Release. The tag is the sole entry point; do
not create the GitHub Release by hand. Curated notes at
`planning/releases/<tag>.md` are mandatory for a stable tag and must exist at
the tagged commit, or the workflow fails before anything reaches PyPI.
