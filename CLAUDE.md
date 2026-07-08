# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

`just --list` is the source of truth; run `just install` after pulling or
changing `pyproject.toml`. Non-obvious notes:

- `just test` / `just test-branch` run in Docker (start Redpanda, run pytest,
  tear down) — they require Docker.
- Run one test without Docker via the already-running stack:
  `uv run --no-sync pytest tests/test_kafka_committer.py -k <name>`.
- `just lint` auto-fixes; `just lint-ci` is the check-only CI variant (and runs
  the planning validator).

## Workflow

Planning uses a portable two-axis convention: `architecture/` (repo root) is the
living **truth home** and promotion target; `planning/changes/` holds the
per-change files. **Start at the Quick path** in
[`planning/README.md`](planning/README.md) to choose a lane (Full / Lightweight
/ Tiny), create a change file, and ship — that file is the authoritative spec. Run
`just check-planning` to validate changes and `just index` to print the listing.
Release notes: copy `planning/releases/TEMPLATE.md` to
`planning/releases/<version>.md` (bare version, no `v` prefix) when cutting a
release.

## Architecture

The library provides concurrent Kafka message processing for FastStream. The
authoritative, code-current account of each capability lives in
[`architecture/`](architecture/). **When a change alters a capability's
behavior, update the matching `architecture/<capability>.md` in the same PR** —
that promotion is what keeps `architecture/` true.

Invariants (what must not break):

- **At-least-once offsets.** Offsets are committed only *after* the user task
  finishes; a crash, cancellation, or rebalance before completion leaves the
  offset uncommitted so the message is redelivered. A cancelled task is a hard
  offset boundary — cancelled-and-after offsets on its partition stay
  uncommitted.
- **One handler per init, not a singleton.** A handler lives in `ContextRepo`
  under `"concurrent_processing"`; `stop_concurrent_processing` clears it so a
  fresh handler can be initialised. Lifecycle is owned by whoever calls
  init/stop — no module-level state, no signal handlers.
- **Middleware gates on manual acks.** It passes through FakeConsumer and
  non-MANUAL-ack subscribers, refuses `_enable_auto_commit=True`, rejects batch
  subscribers, and skips (logs, leaves offset uncommitted) once the handler is
  stopped. The `_classify(...) -> _Route` branch *order* is load-bearing.
- **Bounded shutdown / rebalance flush.** Shutdown is bounded by the committer's
  `shutdown_timeout_sec` (default 20 s); the rebalance listener's `commit_all`
  flush is bounded by `flush_timeout_sec` (default 10 s, well under aiokafka's
  300 s `max.poll.interval.ms`).
- **Real-broker tests.** Integration tests drive a real Redpanda container; the
  FastStream/aiokafka harness invariants (start subscribers explicitly,
  `auto_offset_reset="earliest"`, pre-create topics, etc.) are load-bearing.

| Capability | File |
|---|---|
| `KafkaConcurrentHandler` (`processing.py`) — engine, dispatch, shutdown | [`architecture/concurrent-handler.md`](architecture/concurrent-handler.md) |
| `KafkaBatchCommitter` (`batch_committer.py`) — offset-commit task | [`architecture/batch-committer.md`](architecture/batch-committer.md) |
| Middleware, init/stop lifecycle, healthcheck (`middleware.py`, `healthcheck.py`) | [`architecture/middleware-lifecycle.md`](architecture/middleware-lifecycle.md) |
| `ConsumerRebalanceListener` (`rebalance.py`) | [`architecture/rebalance.md`](architecture/rebalance.md) |
| Real-broker integration-test harness (`tests/test_integration.py`) | [`architecture/integration-tests.md`](architecture/integration-tests.md) |

## Conventions

- **Type suppression**: use `# ty: ignore[rule-name]` (not `# type: ignore`).
- **No `from __future__ import annotations`**: annotations are evaluated eagerly; `typing.Self`/`typing.Never` are used directly (requires Python ≥ 3.11).
- **Imports at module level**: no local imports inside functions.
