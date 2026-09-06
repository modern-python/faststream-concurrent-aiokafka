# AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`faststream-concurrent-aiokafka` gives FastStream's Kafka broker bounded concurrent message
processing without giving up at-least-once delivery. [`CONTEXT.md`](CONTEXT.md) opens with what it
does and owns the vocabulary — read it before naming a concept in code, a test name, or an issue
title. Most of the vocabulary is FastStream's and aiokafka's; only six terms are this package's.

## Commands

`just` (task runner) and `uv` (package manager). The [`justfile`](justfile) is the source of truth —
`just --list`, or read it. Two things it does not say:

- `just test` needs Docker; it starts Redpanda, runs pytest, and tears down. To run one test
  against an already-running stack, `uv run --no-sync pytest tests/test_kafka_committer.py -k <name>`.
- A `ty` suppression is written `# ty: ignore[rule]`, never `# type: ignore`.

## Architecture

`faststream_concurrent_aiokafka/` is nine short modules; read the ones you are changing. The one
thing not visible from any single module is the ownership split inside the committer:
`_pending_state.py` owns *what* to commit, `_commit_scheduler.py` owns *when*, and
`batch_committer.py` owns the queue, the backpressure ceiling, and the `consumer.commit()` I/O.
Both collaborators are synchronous and clock-free by design — see
[ADR-0005](docs/adr/0005-commit-scheduler-decides-the-driver-awaits.md) before moving anything
across that seam.

`middleware.py` must stay registerable **once at broker level** across a mix of subscribers: only
`AckPolicy.MANUAL` is dispatched concurrently and everything else behaves as if the middleware were
absent ([ADR-0004](docs/adr/0004-non-manual-ack-policies-pass-through.md)). The `_classify` branch
order is load-bearing and pinned by tests.

## Workflow

**The spec for a change is its PR body**, not a committed file: why, design, non-goals,
verification, reviewed with the diff. There is no change file and no lane to choose. A trivial PR
(typo, dep bump, formatter, CI tweak) ships a conventional-commit title with no body ceremony.

Two things outlive the PR, and there are exactly two places to put them: an alternative **rejected**
with reasoning becomes an ADR in [`docs/adr/`](docs/adr/) (`NNNN-slug.md`, sequential, with a
revisit trigger), and real work **not scheduled** becomes a GitHub issue. There is no third state,
and no separate truth-home directory — a behaviour change is reviewed with the diff, not promoted
to a page.

### Where a fact goes

Four homes, one owner each:

| Home | Holds |
|---|---|
| `faststream_concurrent_aiokafka/` | anything readable from the module — the default |
| a named test | an **invariant**: must stay true, and a change could silently break it |
| `docs/adr/` | a rejected alternative, with the reasoning that would otherwise be re-litigated |
| `README.md` | anything a user needs |

Before writing a line anywhere:

> Can an agent get this by reading `faststream_concurrent_aiokafka/`? → **don't write it.**
> Would a wrong change here fail a test? → it belongs **in the test**, not in prose.
> Does a user need it? → **`README.md`**.
> Otherwise it does not get written.

**Prose about mechanism has no home. There is no file to add a paragraph to.** This file included:
it is always loaded, so a line that restates a docstring, the justfile, or `pyproject.toml` costs
every turn and rots in two places at once. This package's modules carry unusually dense comments;
that makes restating them here unusually tempting, and unusually wasteful.

An invariant is a test whose name is the claim, with a docstring opening `INVARIANT:` and a second
paragraph naming **what breaks it** — design rationale, not a report of what this one test catches.
Nothing enforces that docstring shape; it is read at review time. A relative link to an ADR *is*
checked — CI runs lychee `--offline` over every `.md` — but a path named in a docstring or a
comment is not. Both ADRs and `INVARIANT:` docstrings ratchet: nothing prunes a record once its
call is settled. Keeping them lean is a standing habit.

## Conventions

- **No `from __future__ import annotations`**: annotations are evaluated eagerly; `typing.Self` /
  `typing.Never` are used directly (requires Python ≥ 3.11).
- **Imports at module level**: no local imports inside functions.
