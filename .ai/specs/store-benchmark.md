# Spec: prism-store — reproducible benchmark vs ClickHouse

Status: ALL_OK

- **Slug / branch:** `feat/store-benchmark`
- **Owner phase:** orchestrator → developer
- **Relates to:** Epic #21 (final deliverable) — depends on #22–#29 (merged).

## 1. Task

Build a **reproducible benchmark harness** comparing prism-store's approach
(embedded DuckDB over tiered, zstd-compressed Parquet) against **ClickHouse**
on four workloads — **ingest, count, aggregation, and a logs `LIKE` scan** —
and publish the measured results in `README.md` with clear, one-command
reproduction steps. Both systems must be given a *fair* configuration
(ClickHouse is tuned per its best practices, not a strawman).

## 2. Scope

- **In scope** (`bench/` — a self-contained, well-structured harness):
  - **Datasets (deterministic, seeded generator):**
    - **metrics** — columns matching the store schema (`__name__` LowCardinality-style label, `labels`, `value` float64, `timestamp_ms`, `ts`), a configurable row count (default a "small" profile ~2M rows for CI/laptops; a `--scale` flag for larger local runs), realistic metric-name cardinality (e.g. ~200 series) over a fixed time span.
    - **logs** — synthetic logs with a **`message` text column** plus `ts`, `level` (LowCardinality-suitable), `service`; messages drawn from a template pool so a known substring (e.g. `"deadline exceeded"`) appears at a **known, fixed frequency** → the `LIKE '%deadline exceeded%'` result count is deterministic and asserted equal across both systems.
    Generator emits Parquet (for the store path) and is streamable to ClickHouse; same underlying rows feed both systems (apples-to-apples).
  - **ClickHouse side (fair, per clickhouse-best-practices):**
    - MergeTree tables; `ORDER BY` low→high cardinality including the primary filter (`ts`); native types (`Float64`, `DateTime64`), `LowCardinality(String)` for `__name__`/`level`/`service`; **no `Nullable`**; `PARTITION BY` by day (bounded partition count).
    - Ingest via **batched INSERT (10K–100K rows/batch)**, Native or Parquet format.
    - Logs table carries a **`tokenbf_v1` (or `ngrambf_v1`) skip index** on `message` so the `LIKE` is index-assisted — ClickHouse gets its best realistic shot.
    - Pinned image (e.g. `clickhouse/clickhouse-server:<pinned>`), run via compose; queries issued over HTTP/native client.
  - **prism-store side (the real engine):**
    - **metrics** ingest → the actual `prism-store` HTTP ingest path (Parquet windows), then flush + compaction so queries hit compacted tiers; **count** and **aggregation** via the store's query engine (fixed-schema union over hot + tiers + rollups, per #26).
    - **logs `LIKE`** → the store has no logs artifact today, so the harness represents the store's *approach* by running the identical DuckDB-over-tiered-Parquet query the engine uses, against a logs-shaped Parquet tier written in the store's layout. This is **documented explicitly** as "the store's engine over a logs-shaped tier," not a claim that the shipping API ingests logs.
  - **Workloads & measurement:**
    1. **ingest** — load the full dataset into each system; report wall-clock + rows/s.
    2. **count** — `count(*)` over a bounded `ts` range.
    3. **aggregation** — per-series `avg`/`min`/`max`/`count` `GROUP BY __name__` over a range.
    4. **logs LIKE** — `count(*) WHERE message LIKE '%deadline exceeded%'` over a range; assert both systems return the **same** count (correctness gate before timing).
    - Each query: warm once, then run K times (default 5); report **p50 / p95 / min** (ms). Fixed random seed; identical row sets; caches warmed identically.
  - **Orchestrator CLI** `bench/cmd/prism-bench` (or a `make bench` target) that: generates data → brings ClickHouse up (compose) → builds+runs prism-store → runs all workloads → writes a machine-readable `bench/results.json` and a rendered `bench/RESULTS.md` table (including host CPU/RAM, OS/arch, ClickHouse + DuckDB versions, dataset scale, commit).
  - **Reproducibility:** `bench/README.md` with prerequisites (docker, go) and the exact commands; `make bench` runs the default (CI-sized) profile end to end; a `--scale` knob for larger local runs. Everything cleans up (`compose down -v`).
  - **README results section:** a "Benchmark: prism-store vs ClickHouse" section with the results table, the reproduction command, a note on the environment the numbers were measured on, and an honest interpretation (where the store wins/loses and why — columnar Parquet+zstd, rollups for wide ranges; ClickHouse's strengths). No cherry-picking; if ClickHouse wins a workload, say so.
  - **Fairness & honesty guardrails (documented in `bench/README.md`):** identical data, identical time ranges, both warmed, ClickHouse tuned (skip index + batched inserts + typed schema), same host; the LIKE correctness assertion; state the store's logs path is engine-level.
- **Out of scope:** distributed/replicated ClickHouse; multi-node; the store's logs *artifact/API* (engine-level logs only, clearly labeled); micro-optimizing either system beyond documented, fair tuning; publishing to any external dashboard.

## 3. Open questions  (resolved before READY)

- [x] How to represent the store for the **logs LIKE** workload given it's metrics-only today? → Run the store's **DuckDB-over-tiered-Parquet** query over a logs-shaped Parquet tier in the store's layout, and label it as engine-level (not an API claim). Keeps the comparison honest and avoids scope-creeping a logs artifact.
- [x] Dataset scale? → Default **small (~2M rows)** so `make bench` runs on a laptop/CI in minutes; `--scale N` for bigger local runs. README states the scale used for the published numbers.
- [x] Is a raw `LIKE` fair to ClickHouse? → No — add a `tokenbf_v1`/`ngrambf_v1` skip index on `message` so ClickHouse is index-assisted; document it.
- [x] Where do published numbers come from? → Measured on the machine that runs the loop; the README records the exact environment and the one-command reproduction so anyone re-running gets comparable (host-dependent) numbers.
- [x] Drive store ingest via HTTP or the engine directly? → **HTTP ingest path** (the real one) for metrics, then flush+compact before querying; the logs tier is written directly in the store layout (engine-level, as above).

## 4. Decision log  (Decision Protocol)

- **ClickHouse tuned per best practices (fair baseline).**
  - ref: clickhouse-best-practices skill — `schema-pk-cardinality-order`, `schema-types-lowcardinality`, `schema-types-avoid-nullable`, `insert-batch-size`, `query-index-skipping-indices`; https://clickhouse.com/docs/best-practices
  - perf: typed schema + `ts`-prefixed ORDER BY + `tokenbf_v1` on `message` + 10K–100K batches is ClickHouse's realistic strong configuration.
  - product: a credible comparison — beating an untuned ClickHouse would be meaningless.
- **DuckDB-over-Parquet with warmed, compacted tiers for the store.**
  - ref: DuckDB Parquet performance — https://duckdb.org/docs/data/parquet/overview ; the store's fixed-schema union (#26).
  - perf: querying few large compacted zstd Parquet files with pushed-down `ts` filters is the store's designed hot path.
  - product: measures the shipping architecture, not a toy.
- **Deterministic seeded data + correctness assertion.**
  - ref: reproducible-benchmark practice; a fixed-frequency substring makes `LIKE` counts exact.
  - perf: n/a; product: results are trustworthy and re-runnable; the LIKE count equality gate prevents comparing wrong answers.

## 5. Acceptance checklist  (developer checks these off)

- [x] `make bench` (default profile) runs end-to-end on this host: generates data, brings ClickHouse up, builds+runs prism-store, runs all four workloads, tears down, and writes `bench/results.json` + `bench/RESULTS.md`.
- [x] Deterministic generator (seeded); the same rows feed both systems; the `LIKE` substring frequency is fixed and the **count matches** between ClickHouse and the store (asserted; benchmark fails if they differ).
- [x] ClickHouse schema is tuned: MergeTree, `ts`-prefixed `ORDER BY`, `LowCardinality`/native types, no `Nullable`, day partitioning, `tokenbf_v1`/`ngrambf_v1` on `message`; inserts batched 10K–100K.
- [x] Store metrics path uses the real HTTP ingest + compaction + query engine; logs path uses the engine's DuckDB-over-Parquet query (labeled engine-level).
- [x] Each query reports p50/p95/min over K warmed runs; ingest reports wall-clock + rows/s.
- [x] `bench/README.md`: prerequisites + exact one-command reproduction + cleanup; environment captured in `RESULTS.md` (CPU/RAM/OS/arch, CH + DuckDB versions, scale, commit).
- [x] `README.md` gains a "Benchmark: prism-store vs ClickHouse" section with the measured table, reproduction command, environment note, and an **honest** interpretation (including any workload ClickHouse wins).
- [x] Harness code is clean and structured (generator / clickhouse driver / store driver / orchestrator separated); no "macaronic" one-off scripts; errors handled; no secrets.
- [x] Bench Go code has unit tests for the deterministic generator (fixed seed → fixed substring count) and the results renderer; `make lint test` green.
- [x] `CGO_ENABLED=0 go build ./cmd/prism` and `go build ./cmd/prism-store` still pass; bench code (if it imports DuckDB) is CGO-gated and does not pull DuckDB into the agent build.
- [x] Nothing heavyweight added to the default CI `test`/`full` path that needs ClickHouse unless explicitly gated (bench is opt-in via `make bench`); no committed large datasets or binaries.

## 6. Mandatory review gates  (reviewer owns)

- [x] **Gate 1 — Guidelines:** harness idiomatic Go, separated concerns, wrapped errors, no globals; ClickHouse client and store drivers behind small interfaces; `make bench` is the single entrypoint; comments self-contained.
- [x] **Gate 2 — Edge cases:** benchmark fails loudly if the `LIKE` counts differ (correctness gate); compose teardown runs even on failure; deterministic seed reproduces identical counts across two runs; `--scale` works; missing docker → clear error; ClickHouse readiness waited on (no race).
- [x] **Gate 3 — Docs/comments match code:** README table/commands match what `make bench` actually produces and the harness flags; the store-logs "engine-level" caveat is stated and accurate; ClickHouse tuning claims match the actual DDL.
- [x] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [x] **Fairness audit:** reviewer confirms ClickHouse is genuinely tuned (not handicapped) AND the store isn't given an unfair shortcut (e.g. pre-cached results, skipping compaction, smaller data). Numbers in the README must be the ones `make bench` produced on the stated host — reviewer re-runs `make bench` and confirms the README table is consistent with a real run (allowing host variance).
  - Fixed (2026-07-23): metrics count/aggregation now full-table scan on both systems; metrics count correctness gate added.
- [x] Full docs/REVIEW.md checklist; TESTING.md layering (generator/renderer unit tests; the full bench is opt-in, not in CI gate).

## 7. Reviewer notes

**Verdict: ALL_OK** (re-review 2026-07-23). Prior CHANGES_REQUESTED items verified fixed in `50439ca` / `806b205`.

### ts-semantics fairness (PASS)

Metrics **count/aggregation** now full-table on both sides — no `ts` predicate. ClickHouse: `SELECT count()` / `GROUP BY __name__` from `bench_metrics`. Store: `buildFullMetricsUnionInner` unions `hot_current`, `hot_prev`, and tier `read_parquet` globs only (rollups excluded). Orchestrator asserts `storeMetricsCount == chMetricsCount == cfg.MetricsRows` before timing. Reviewer run: **1,000,000 = 1,000,000**. Logs LIKE unchanged — shared `ds.LogsQueryRange()` on both.

### Gate 4 (PASS)

`driver_cgo.go` nolint now reads “inner SQL unions hot and tier parquet sources built by this driver” — no cross-component name.

### Gate 3 / honesty (PASS)

`interpret()`, `bench/README.md`, and `README.md` describe full-table metrics scan, dataset-`ts` logs window, engine-level store logs path, ClickHouse logs LIKE win, and dual correctness gates. No misleading “identical time range” for metrics.

### Verification commands (re-review)

| Command | Result |
|---------|--------|
| `make lint` | 0 issues |
| `make test` | pass (-race) |
| `CGO_ENABLED=0 go build ./cmd/prism` | pass (no DuckDB/ClickHouse in agent) |
| `go build ./cmd/prism-store` | pass |
| `make bench` | pass (~12s); metrics gate 1,000,000=1,000,000; LIKE gate 10,000=10,000; teardown OK |

Reviewer `make bench` p50 (M1 Pro 16 GiB): ingest store 1.17s / CH 1.84s; count 0.6 ms / 1.6 ms; aggregation 4.8 ms / 5.9 ms; logs LIKE 19.5 ms / 16.5 ms. Winner shape matches committed README/RESULTS (host variance only).

---

**Prior review (2026-07-23, CHANGES_REQUESTED):** metrics count/agg used ingest-time `ts` (store) vs dataset `ts` (ClickHouse); fixed by full-table methodology above.
