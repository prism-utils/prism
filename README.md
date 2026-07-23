# prism

> **Status:** foundation complete for the edge agent; the **store** track (#21)
> is in progress. See [`docs/PLAN.md`](docs/PLAN.md) and [`TASKS.md`](TASKS.md).
>
> **Name is provisional.** `prism` = one input stream refracted through an
> ordered pipeline into one or more encoded outputs. Easy to rename now.

`prism` is a small, memory-efficient, **config-driven edge collector** written
in Go. It follows the OpenTelemetry Collector mental model —
**input → processors → output** — but is purpose-built to **pre-aggregate and
re-encode logs and metrics at the edge** and emit them as compact columnar
artifacts (Parquet) to cheap sinks.

It is designed to run identically on **Linux bare metal** and **in a
container**, as a **single static, CGO-free binary** (`cmd/prism`).

## Components

| Binary | Role | Build |
|---|---|---|
| **`prism`** (agent) | Config-driven edge collector; produces Parquet artifacts per [`docs/OUTPUT_CONTRACT.md`](docs/OUTPUT_CONTRACT.md). | Static, `CGO_ENABLED=0` |
| **`prism-store`** (store) | Durable tiered columnar store + query server; consumes agent output. | CGO-linked (DuckDB, later slices) |

Store design: [`docs/STORE.md`](docs/STORE.md). Architecture ADR: [`docs/DESIGN.md`](docs/DESIGN.md) §15.

## What it does

```
                ┌────────── pipeline (ordered) ──────────┐
  input  ──►    parse ──► processors… ──► encode ──►      output
 (file /        (rows→    (built-in                (parquet/
  batch /        Arrow)    compiled +               raw)
  stdin)                   scripted)
```

- **Inputs:** follow a file (`tail`), process a whole file then exit (`batch`),
  or read `stdin`.
- **Processors** (run in a deterministic, configured order):
  - **Built-in, compiled** processors you toggle on/off — e.g. a logging
    template (normalization), ML/anomaly detection, summary/roll-up, field
    auto-discovery. Compiled in = no per-record interpreter cost.
  - **Scripted** processors — inject logic at runtime (no rebuild) for the
    long tail of custom transforms.
- **Encoders:** serialize the internal record batch to the wire format —
  **Parquet** first, plus a raw/JSON passthrough for debugging.
- **Outputs:** `stdout`, `file` (rotating), `http` (binary upload with retry).

Config is **YAML or JSON** (one schema, both formats).

## Design in one line

Everything is a **registered component behind a small interface**, wired by a
**config-driven pipeline builder** over an **Apache Arrow** in-memory batch.
Add a capability = implement an interface + register a factory. No core edits.

Read these, in order:

1. [`docs/DESIGN.md`](docs/DESIGN.md) — architecture, patterns, data model.
2. [`docs/CONFIG.md`](docs/CONFIG.md) — complete config reference (every component, its options, defaults).
3. [`docs/STORE.md`](docs/STORE.md) — store/query server layout and env (stub).
4. [`docs/PLAN.md`](docs/PLAN.md) — phased, test-first build plan.
5. [`CONTRIBUTING.md`](CONTRIBUTING.md) — TDD workflow, data patterns, dos/don'ts.
6. [`docs/TESTING.md`](docs/TESTING.md) — test layers and how to run them.
7. [`docs/REVIEW.md`](docs/REVIEW.md) — the reviewer checklist.

### Working with agents

`prism` ships an orchestrator → developer → reviewer agent loop. Start at
[`AGENTS.md`](AGENTS.md); the process lives in
[`.ai/workflows/feature-loop.md`](.ai/workflows/feature-loop.md). Every task
begins from `main` in a fresh worktree, is specified in `.ai/specs/<slug>.md`,
and finishes only when the reviewer signs `ALL_OK` and it merges.

## Quickstart (target UX — not all wired yet)

```bash
make build                      # -> ./bin/prism (static, CGO_ENABLED=0)
go build ./cmd/prism-store      # store skeleton (CGO when engine lands)
./bin/prism validate -c prism.yaml
cat app.log | ./bin/prism run -c prism.yaml
make test                       # fast unit tests
make full-tests                 # unit + integration (docker-compose) + e2e
```

## Requirements

- Go 1.25+ (build/test only; the shipped artifact is a single static binary).
- Docker + docker-compose (for `make full-tests` integration layer only).
