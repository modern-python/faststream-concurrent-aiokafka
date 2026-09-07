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

Real work **not scheduled** becomes a GitHub issue.

An invariant is a test whose name is the claim, with a docstring opening `INVARIANT:` and a second
paragraph naming **what breaks it** — design rationale, not a report of what this one test catches.
Nothing enforces that docstring shape; it is read at review time.

## Conventions

- **No `from __future__ import annotations`**: annotations are evaluated eagerly; `typing.Self` /
  `typing.Never` are used directly (requires Python ≥ 3.11).
- **Imports at module level**: no local imports inside functions.
