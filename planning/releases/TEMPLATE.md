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

Spec: [`planning/changes/archive/<YYYY-MM-DD.NN-slug>/design.md`](../changes/archive/<YYYY-MM-DD.NN-slug>/design.md).

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
