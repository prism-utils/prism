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

## Releases

A `v*` git tag triggers
[`.github/workflows/release.yml`](.github/workflows/release.yml) (never
merge-to-main). One pipeline publishes **two artifacts**:

| Artifact | Image | Binary build | Runtime base | UID |
|---|---|---|---|---|
| **`prism`** (agent) | `ghcr.io/elk-utilities/prism` | static, `CGO_ENABLED=0` | distroless static | 65532 |
| **`prism-store`** (store) | `ghcr.io/elk-utilities/prism-store` | CGO + DuckDB, linux amd64/arm64 | debian:bookworm-slim + `libstdc++6` | 472 |

Per-arch tags: `:<version>-amd64`, `:<version>-arm64`, plus combined manifests
`:<version>`, `:sha-<short>`, and `:latest`. GitHub Release assets include
`prism_*` and `prism-store_*` tarballs, `checksums.txt`, SBOMs, and cosign
signatures.

Verify a published store manifest (keyless OIDC signature from the release
workflow):

```bash
cosign verify ghcr.io/elk-utilities/prism-store:v1.0.0 \
  --certificate-identity-regexp='https://github.com/elk-utilities/prism/.github/workflows/release.yml@refs/tags/v*' \
  --certificate-oidc-issuer=https://token.actions.githubusercontent.com
```

Replace `v1.0.0` with the pinned tag. Agent images verify the same way against
`ghcr.io/elk-utilities/prism:<tag>`.

Local dry run: `make release-check` (validate config) and `make snapshot`
(build everything, push nothing). Store image: `make docker-store`.

## Benchmark: prism-store vs ClickHouse

Reproducible comparison harness under [`bench/`](bench/) (Epic #21 deliverable).
Default profile: **1M metrics + 1M logs** (~2M rows total), deterministic seed,
correctness gate on logs `LIKE '%deadline exceeded%'` (10,000 matches).

```bash
make bench              # requires Docker + CGO; ~15s on Apple M1 Pro 16 GiB
make bench BENCH_SCALE=2
```

### Measured on this host (2026-07-23, `make bench`)

| Environment | |
|---|---|
| OS / arch | darwin / arm64 |
| CPU | Apple M1 Pro |
| RAM | 16 GiB |
| ClickHouse | 24.8.14.39 (`clickhouse/clickhouse-server:24.8`) |
| DuckDB | v1.1.3 |
| Dataset | 1,000,000 metrics + 1,000,000 logs |

| Workload | prism-store | ClickHouse |
|----------|-------------|------------|
| ingest | 1.17s · 1,714,212 rows/s | 1.82s · 1,100,331 rows/s |
| count (p50 / p95 / min ms) | 0.8 / 0.9 / 0.8 | 4.4 / 16.5 / 3.8 |
| aggregation | 6.4 / 6.6 / 5.3 | 11.8 / 26.1 / 11.1 |
| logs LIKE | 19.6 / 19.9 / 19.3 | 15.9 / 16.3 / 15.5 |

**Interpretation:** prism-store wins ingest, count, and aggregation on this laptop.
ClickHouse wins the logs `LIKE` workload (p50 15.9 ms vs 19.6 ms) despite fair
tuning (`tokenbf_v1` skip index, batched inserts). Store metrics queries filter
on **ingest-time** `ts` (the real HTTP ingest path); ClickHouse stores sample
timestamps from the dataset. Store logs `LIKE` is **engine-level** (DuckDB over
a logs-shaped Parquet tier) — not a shipping logs API.

Full tables, fairness notes, and cleanup: [`bench/README.md`](bench/README.md),
[`bench/RESULTS.md`](bench/RESULTS.md).

## Requirements

- Go 1.25+ (build/test only; the shipped agent artifact is a static binary).
- Docker + docker-compose (for `make full-tests` agent integration layer only).
- CGO toolchain for `prism-store` builds (`go build ./cmd/prism-store`).
