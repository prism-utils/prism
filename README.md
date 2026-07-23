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
| **`prism-store`** (store) | Durable tiered columnar store + query server; consumes agent output. Deployable `standalone`/`client`/`cluster`, with optional **RBAC** and an **arbitrary read-only SQL API**. | CGO-linked (DuckDB) |

Store design: [`docs/STORE.md`](docs/STORE.md). Architecture ADR: [`docs/DESIGN.md`](docs/DESIGN.md) §15. Cutover from `prism-proxy`: [`docs/MIGRATION.md`](docs/MIGRATION.md).

**Store security (RBAC).** When `AUTHZ_POLICY_FILE` is set, HTTP query/ingest/admin
routes require a verified **JWT (OIDC/JWKS)** and a **deny-by-default, per-tenant
policy** — fixed roles **`reader`** (query), **`writer`** (ingest), **`admin`**
(query + ingest + provision + stats). A principal can only act on tenants it is
explicitly bound to; unauthorized tenants return `404` (no existence leak) and no
role can escalate. Native fit for **k8s** (projected ServiceAccount tokens +
ConfigMap policy) and **Vault** (Agent-rendered JWT + policy). Enforced in every
mode, including the cluster coordinator (edge) and clients (defense-in-depth). See
[`docs/STORE.md`](docs/STORE.md#rbac-jwtoidc--per-tenant-roles).

**Arbitrary SQL API.** `POST {ROUTE_PREFIX}/{ns}/sql` runs read-only SQL over a
tenant's `metrics` relation inside a per-request, tenant-scoped DuckDB sandbox
(no cross-tenant or host-filesystem access), subject to the same RBAC. See
[`docs/STORE.md`](docs/STORE.md#arbitrary-sql-api).

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
3. [`docs/STORE.md`](docs/STORE.md) — store/query server: modes, ingest, query, arbitrary SQL API, RBAC, admin/`/stats`, and env.
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
correctness gates on full-table metrics `COUNT(*)` and logs `LIKE '%deadline exceeded%'` (10,000 matches).
Both systems share a **minimal equal resource cap** (default **2 vCPU / 1 GiB** per system).

```bash
make bench              # requires Docker + CGO; ~45s on Apple M1 Pro 16 GiB
make bench BENCH_SCALE=2
```

### Measured on this host (2026-07-23, `make bench`)

| Environment | |
|---|---|
| OS / arch | darwin / arm64 |
| CPU | Apple M1 Pro |
| RAM | 16 GiB |
| Resource cap (per system) | 2 vCPU / 1024 MiB |
| ClickHouse | 24.8.14.39 (`clickhouse/clickhouse-server:24.8`) |
| DuckDB | v1.1.3 |
| Dataset | 1,000,000 metrics + 1,000,000 logs |
| Idle baseline | 5 s before workloads |

**Latency** (p50 / p95 / min ms; ingest: wall + rows/s):

| Workload | prism-store | ClickHouse |
|----------|-------------|------------|
| ingest | 1.52s · 1,319,232 rows/s | 2.05s · 974,396 rows/s |
| count | 0.6 / 4.4 / 0.6 | 1.6 / 3.7 / 1.5 |
| aggregation | 13.3 / 15.0 / 12.5 | 11.1 / 36.3 / 9.6 |
| logs LIKE | 73.5 / 76.2 / 70.4 | 39.5 / 47.4 / 38.2 |

**Resource usage** (dense continuous sampling; idle baseline row; store queries sample the embedded DuckDB engine in `prism-bench`; process I/O/IOPS **`n/a`** on macOS; **Docker Desktop often reports container blkio as 0** — use native Linux Docker for meaningful ClickHouse I/O/IOPS):

| Workload | System | CPU mean / peak | Peak RSS | I/O | IOPS |
|----------|--------|-----------------|----------|-----|------|
| idle (baseline) | prism-store | 0.00 / 0.00 cores | 22.3 MiB | n/a | n/a |
| idle (baseline) | ClickHouse | 0.06 / 1.02 cores | 260.1 MiB | 0.2 MiB | 0 |
| ingest | prism-store | 0.04 / 1.67 cores | 90.3 MiB | n/a | n/a |
| ingest | ClickHouse | 0.10 / 1.56 cores | 421.2 MiB | 0.7 MiB | 0 |
| count | prism-store | 0.36 / 1.02 cores | 633.0 MiB | n/a | n/a |
| count | ClickHouse | 0.06 / 0.22 cores | 421.2 MiB | n/a | n/a |
| aggregation | prism-store | 0.81 / 1.97 cores | 628.1 MiB | n/a | n/a |
| aggregation | ClickHouse | 0.40 / 1.30 cores | 423.0 MiB | 0.2 MiB | 0 |
| logs LIKE | prism-store | 1.23 / 2.69 cores | 625.2 MiB | n/a | n/a |
| logs LIKE | ClickHouse | 0.59 / 1.86 cores | 425.2 MiB | n/a | n/a |

**Charts** (same run): [`bench/charts/cpu-cores.svg`](bench/charts/cpu-cores.svg), [`bench/charts/memory-rss.svg`](bench/charts/memory-rss.svg), [`bench/charts/disk-io.svg`](bench/charts/disk-io.svg)

**Interpretation:** Metrics **count** and **aggregation** scan the full ingested
table on both systems (no `ts` range pruning) — apples-to-apples over the same N
rows. On this laptop prism-store leads **ingest** and **count** (p50). ClickHouse
wins **aggregation** (p50 11.1 ms vs 13.3 ms) and **logs LIKE** (p50 39.5 ms vs
73.5 ms) with fair tuning (`tokenbf_v1` skip index, typed schema, batched inserts).
Logs LIKE uses the same dataset-`ts` window on both sides. Store logs `LIKE` is
**engine-level** (DuckDB over a logs-shaped Parquet tier) — not a shipping logs API.
Both systems ran under the same **2 vCPU / 1 GiB** envelope so neither could allocate the full host.

Full tables, fairness notes, and cleanup: [`bench/README.md`](bench/README.md),
[`bench/RESULTS.md`](bench/RESULTS.md).

### RBAC + HTTP SQL API profile — attached, not a replacement (2026-07-23, `make bench-api`)

The baseline above runs the store queries in-process (embedded DuckDB). This
**attached** profile instead drives the store's **count** and **aggregation**
through the RBAC-guarded HTTP SQL API (`POST /{ns}/sql`, #51): ingest sends a
**JWT** (`writer`/`admin`), queries send a JWT and execute inside the per-request,
tenant-scoped **materialize-then-lock** sandbox. It measures the *end-to-end
production path* (HTTP + JWT verify + policy check + sandbox) — a deliberately
different, heavier cost than the embedded baseline. Same host/dataset/caps.

**Latency** (p50 / p95 / min ms; ingest: wall + rows/s):

| Workload | prism-store (HTTP `/sql` + RBAC) | ClickHouse (native) |
|----------|----------------------------------|---------------------|
| ingest | 1.40s · 1,431,753 rows/s | 1.84s · 1,084,057 rows/s |
| count | 286.5 / 286.8 / 277.6 | 2.0 / 2.1 / 1.3 |
| aggregation | 296.9 / 300.8 / 291.3 | 10.6 / 25.3 / 8.7 |
| logs LIKE | 62.5 / 66.6 / 58.5 | 38.6 / 48.8 / 34.0 |

**Resource usage** (store count/aggregation now sample the `prism-store` **binary** serving HTTP):

| Workload | System | CPU mean / peak | Peak RSS | I/O | IOPS |
|----------|--------|-----------------|----------|-----|------|
| idle (baseline) | prism-store | 0.00 / 0.00 cores | 22.7 MiB | n/a | n/a |
| idle (baseline) | ClickHouse | 0.06 / 1.08 cores | 244.2 MiB | 0.2 MiB | 0 |
| ingest | prism-store | 0.18 / 1.87 cores | 92.3 MiB | n/a | n/a |
| ingest | ClickHouse | 0.28 / 1.75 cores | 390.9 MiB | 0.0 MiB | 0 |
| count | prism-store | 1.19 / 2.26 cores | 471.2 MiB | n/a | n/a |
| count | ClickHouse | 0.06 / 0.38 cores | 384.4 MiB | n/a | n/a |
| aggregation | prism-store | 1.20 / 2.12 cores | 483.4 MiB | n/a | n/a |
| aggregation | ClickHouse | 0.10 / 0.76 cores | 384.0 MiB | 37.3 MiB | 0 |
| logs LIKE | prism-store | 0.99 / 3.21 cores | 1490.0 MiB | n/a | n/a |
| logs LIKE | ClickHouse | 0.34 / 1.84 cores | 422.7 MiB | n/a | n/a |

**Charts** (same run): [`bench/charts-api/cpu-cores.svg`](bench/charts-api/cpu-cores.svg), [`bench/charts-api/memory-rss.svg`](bench/charts-api/memory-rss.svg), [`bench/charts-api/disk-io.svg`](bench/charts-api/disk-io.svg)

**Interpretation.** **Ingest** still leads ClickHouse even with JWT auth on every
request. **Count/aggregation over the API cost ~280–300 ms** vs the embedded
baseline's sub-15 ms — the gap is the per-request sandbox: on the bundled **DuckDB
v1.1.3**, `allowed_directories` does not exist, so the tenant's data is
**materialized into an in-memory table** (then external access is disabled +
configuration locked) before every query. That copy dominates at 1M rows. When
`go-duckdb` bundles DuckDB ≥1.2, the sandbox can use `allowed_directories` + a lazy
view (no per-query materialization), which should close most of this gap; the
embedded baseline shows the underlying engine is already competitive. **logs LIKE**
stays engine-level (no logs API) and is unchanged in shape. ClickHouse is queried
over its **native protocol** here (not HTTP), so the count/aggregation columns are
not a like-for-like transport comparison — they show the store's full RBAC/API
overhead against ClickHouse's fast path.

Full tables + caveats: [`bench/RESULTS-api.md`](bench/RESULTS-api.md). Reproduce: `make bench-api`.

### Arrow transport profile — attached (2026-07-23, `make bench-api-arrow`)

This profile measures store **count** and **aggregation** over HTTP `/sql` with **Arrow IPC**
transport (lazy-view sandbox + streaming), plus a **JSON-vs-Arrow scan** on the same SQL to
isolate transport memory and latency — all with RBAC/JWT on every request. It attaches
beside the JSON `-api` profile above; neither replaces the embedded baseline.

**Latency** (p50 / p95 / min ms; ingest: wall + rows/s; Apple M1 Pro, 2 vCPU / 1024 MiB cap, DuckDB v1.4.1):

| Workload | prism-store (Arrow) | ClickHouse (native) |
|----------|---------------------|---------------------|
| ingest | 1.16s · 1,719,589 rows/s | 1.90s · 1,051,625 rows/s |
| count | 124.6 / 126.0 / 122.5 | 1.3 / 2.3 / 1.0 |
| aggregation | 137.7 / 139.0 / 135.0 | 9.8 / 29.3 / 8.9 |
| logs LIKE | 43.9 / 46.7 / 42.9 | 34.3 / 74.0 / 34.0 |

**Scan transport** (store only, same `SELECT … LIMIT 100000` — not vs ClickHouse):

| Transport | p50 / p95 / min (ms) | wire size | rows |
|-----------|----------------------|-----------|------|
| JSON | 350.1 / 359.7 / 347.0 | ~8.8 MiB | 100,000 |
| Arrow | 172.9 / 173.9 / 161.0 | ~5.3 MiB | 100,000 |

**Resource usage** (peak RSS; scan phases isolate JSON-vs-Arrow):

| Workload | System | CPU mean / peak (cores) | Peak RSS (MiB) |
|----------|--------|-------------------------|----------------|
| count (Arrow) | prism-store | 1.14 / 2.06 | 413.7 |
| aggregation (Arrow) | prism-store | 0.98 / 1.86 | 426.1 |
| scan (JSON) | prism-store | 0.83 / 2.04 | 541.3 |
| scan (Arrow) | prism-store | 1.03 / 1.90 | 553.4 |

**Impact.** Versus the JSON `-api` profile above (~280–300 ms count/agg, ~471–483 MiB peak
RSS), the combined lazy-view sandbox (#54) + Arrow transport (#55) roughly **halves count/agg
latency** (280→125 ms count, ~300→138 ms aggregation). On the 100k-row scan, **Arrow is ~2×
faster than JSON (173 vs 350 ms) on a ~40% smaller wire** (5.3 vs 8.8 MiB), and never buffers
the full result in Go (bounded per-batch streaming). Peak RSS for the scan is comparable across
transports because the DuckDB scan over the parquet-backed view — not result serialization —
dominates memory at this scale; the transport's memory win is in bounded client/serialization
buffers rather than engine RSS.

**Charts** (same run): [`bench/charts-api-arrow/cpu-cores.svg`](bench/charts-api-arrow/cpu-cores.svg), [`bench/charts-api-arrow/memory-rss.svg`](bench/charts-api-arrow/memory-rss.svg), [`bench/charts-api-arrow/disk-io.svg`](bench/charts-api-arrow/disk-io.svg)

Full tables + caveats: [`bench/RESULTS-api-arrow.md`](bench/RESULTS-api-arrow.md). Reproduce: `make bench-api-arrow`.

#### Hot-cache-only transport variant — attached (2026-07-23, `make bench-api-arrow-hot`)

Same Arrow-transport profile with `QUERY_HOT_ONLY=true`, so the `/sql` sandbox reads **only the
hot snapshot** and skips the parquet tiers. In this dataset the full ~1M rows are resident in the
hot buffer, so counts match and latencies track the full profile (count 130.9 ms, aggregation
143.5 ms; scan Arrow 172.0 ms vs JSON 366.1 ms) — this run isolates the hot-only serving path
(no tier fan-out) for the transport. Results:
[`bench/RESULTS-api-arrow-hot.md`](bench/RESULTS-api-arrow-hot.md) and
[`bench/charts-api-arrow-hot/`](bench/charts-api-arrow-hot/). Reproduce: `make bench-api-arrow-hot`.

## Requirements

- Go 1.25+ (build/test only; the shipped agent artifact is a static binary).
- Docker + docker-compose (for `make full-tests` agent integration layer only).
- CGO toolchain for `prism-store` builds (`go build ./cmd/prism-store`).
