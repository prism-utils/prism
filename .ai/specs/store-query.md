# Spec: prism-store — query API + Grafana DuckDB view SQL

Status: READY

- **Slug / branch:** `feat/store-query`
- **Owner phase:** orchestrator → developer
- **Issue:** elk-utilities/prism#26 (Epic #21) — depends on #24, #25 (merged).

## 1. Task

Port the read path from `homelab-apps` `services/prism-proxy` `internal/query`:
the HTTP query API and the **fixed-schema, tenant-scoped union builder**
(hot + hot_prev + tiers + optional rollup), plus a **library + CLI helper**
that emits the Grafana DuckDB datasource view SQL so any consumer can wire
`initSQL` without re-deriving it. Reads run under the engine's shared read lock
(`WithRead`) so a query never observes the hot table mid-flush.

## 2. Scope

- **In scope** (`internal/store/query` + `cmd/prism-store` wiring):
  - **Union builder `Builder.BuildSQL(Request)`**: `Request{Tenant,Start,End,Step}`. Union, each part filtered `ts >= ? AND ts < ?`, wrapped `SELECT * FROM (…) ORDER BY ts`:
    1. `SELECT * FROM hot_current WHERE ts >= ? AND ts < ?`
    2. `SELECT * FROM hot_prev WHERE ts >= ? AND ts < ?`
    3. for each tier `L0..L7` **only if its `*.parquet` glob matches on disk**: `read_parquet('<tenantRoot>/tiers/L{n}/*.parquet') WHERE ts >= ? AND ts < ?`
    4. if a rollup step is selected: `read_parquet('<tenantRoot>/rollups/<step>/*.parquet')` projected to the row shape (`"__name__"`, `'{}' AS labels`, `avg AS value`, `0 AS timestamp_ms`, `bucket AS ts`) filtered on `bucket`.
    Args are `Start,End` repeated per part (bound parameters). **Hard constraints (tested):** NO `union_by_name`, NO `filename=true` — fixed schema only; paths are **tenant-scoped** (a tenant query must never reference another tenant's path). `pickRollupStep`: explicit `step` wins; else range ≥7d→`1h`, ≥24h→`5m`, ≥1h→`1m`, shorter→raw only.
  - **`Execute(db, sql, args) → []Row`** and **`ToJSON(rows, exposeSQL, sql)`**: JSON `{ "rows":[{metric,labels?,value,timestamp_ms,ts}], "sql"? }` (`sql` only when `E2E_EXPOSE_QUERY_SQL=1`). `AssertNoUnionByName` test guard.
  - **HTTP handler in `cmd/prism-store`**: `GET <ROUTE_PREFIX>/{ns}/query?start=&end=&step=`. `start`/`end` RFC3339 required → missing/invalid `400`; unknown tenant `404`; missing tenant root `400 bad query`; exec failure `500 query failed`; success `200 application/json`. The query executes through **`engine.WithRead(ns, func(db){…})`** (shared read lock) so it is serialized against the flush rename.
  - **Grafana view SQL helper** — `query.ViewSQL(dataDir, tenant) (string, error)`: emits `CREATE OR REPLACE VIEW <name> AS …` unioning the tenant's tier parquet globs + the hot snapshot (`hot/current.parquet`), path-scoped, **no `union_by_name`/`filename`**, projected to the same row shape the API returns. Plus a **CLI subcommand** `prism-store print-view-sql --tenant <ns> [--data-dir <dir>]` that prints it (for wiring a DuckDB datasource `initSQL`).
  - **Perf + safety guards:** a `Benchmark`/opt-in test asserting an aggregate query over a compacted tenant is fast (target < 300 ms; measured, not a hard `-race` gate to avoid CI flakiness); a **hard unit test** asserting the built SQL contains neither `union_by_name` nor `filename`.
  - Reuse `internal/store/{engine,layout,rollup,tenant}`. Update `docs/STORE.md` (query API, union shape, rollup thresholds, view-SQL helper) + `docs/CONFIG.md` (`E2E_EXPOSE_QUERY_SQL`).
- **Store-level behavior tests (the "5 e2e concerns" as Go integration tests, aligned to TESTING.md — not a verbatim shell port):**
  1. **freshness** — a just-ingested row is visible via query within the hot snapshot window;
  2. **cross-tier gap-free** — a range spanning hot + L0 + a merged L1 returns every row exactly once, ordered by ts;
  3. **perf** — aggregate over a compacted tenant within the target (benchmark);
  4. **retention** — a query over an expired range returns nothing after retention deleted the segment;
  5. **tenant isolation** — tenant A's query never returns tenant B's rows and never references B's paths.
- **Out of scope:** `/admin/ensure`, `/stats`, seeds (#27); Helm (#28); release (#29); the actual Grafana deployment (consumers attach Grafana — the store stays pure per the ADR).

## 3. Open questions  (resolved before READY)

- [x] Embed Grafana in `prism-store`? → **No** — keep the store pure; ship the view-SQL helper + CLI so consumers wire their own Grafana (ADR §15, issue recommendation).
- [x] Read concurrency vs the flush rename? → query executes under `engine.WithRead` (shared lock); with the single-connection engine this serializes reads against writes without corruption.
- [x] Perf guard as a hard test? → **No**, a benchmark + a hard SQL-shape assertion; the ClickHouse benchmark task validates real numbers reproducibly.
- [x] Port the shell e2e harness verbatim? → **No** — cover the same five concerns as Go integration tests per TESTING.md.

## 4. Decision log  (Decision Protocol)

- **Fixed-schema union, no `union_by_name`/`filename`.**
  - ref: https://duckdb.org/docs/data/parquet/overview — `union_by_name` and `filename` force per-file schema reconciliation / extra columns.
  - perf: the upstream tiered-lifecycle benchmark measured ~6× (28.8s→4.6s) from dropping them; fixed schema lets DuckDB push the `ts` filter into each parquet scan.
  - product: predictable, fast Grafana panels across all tiers.
- **Rollup step auto-selection by range width.**
  - ref: Grafana/Prometheus step semantics (coarsen long ranges).
  - perf: ≥24h/≥7d ranges read `5m`/`1h` rollups instead of raw — bounded scan; product: long dashboards stay responsive.
- **Ship a view-SQL helper, keep the store pure.**
  - ref: DuckDB datasource `initSQL` pattern.
  - perf: n/a; product: the main project-agnostic lever — any consumer wires Grafana without re-deriving the path-scoped union.

## 5. Acceptance checklist  (developer checks these off)

- [ ] `BuildSQL` unions hot + hot_prev + present tiers (+ selected rollup); every part filters `ts`/`bucket` with bound `?` args; wrapped `ORDER BY ts`.
- [ ] SQL contains **no** `union_by_name` and **no** `filename` (hard test `AssertNoUnionByName`); paths are tenant-scoped (tenant-isolation test: A's SQL never contains B's tenant path).
- [ ] `pickRollupStep`: `step` hint wins; ≥7d→1h, ≥24h→5m, ≥1h→1m, else raw — table test.
- [ ] Query handler: missing/invalid start/end `400`; unknown tenant `404`; missing tenant root `400`; exec error `500`; success `200` JSON with `rows`; `sql` present only when `E2E_EXPOSE_QUERY_SQL=1`.
- [ ] Query runs under `engine.WithRead`; a test ingests + queries concurrently with a flush and returns a consistent, gap-free result under `-race`.
- [ ] Cross-tier gap-free: a range over hot+L0+L1 returns each row once, ordered by ts.
- [ ] Freshness, retention, tenant-isolation integration tests pass.
- [ ] `query.ViewSQL` + `prism-store print-view-sql --tenant` emit path-scoped view SQL with no `union_by_name`/`filename`, same row shape as the API; covered by a test.
- [ ] Aggregate perf benchmark present (target < 300 ms) — reported, not a flaky gate.
- [ ] `docs/STORE.md` + `docs/CONFIG.md` updated.
- [ ] Tests written first (`test:` commit precedes implementation) — CONTRIBUTING.md §1.
- [ ] `make lint test` green; `CGO_ENABLED=0 go build ./cmd/prism` passes; `go build ./cmd/prism-store` passes.

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Guidelines:** builder is leaf/pure (glob + string build); handler thin, slog at edge; query executes via `WithRead` (no raw unsynchronized `DB()` read in the concurrent path); errors wrapped; bound params for ts; no globals.
- [ ] **Gate 2 — Edge cases:** empty tenant (no tiers) → hot-only; absent rollup dir; range with no data → empty rows (not error); invalid RFC3339; step override vs auto; tenant-path traversal rejected; concurrent query+flush under `-race`.
- [ ] **Gate 3 — Docs/comments match code:** STORE.md query section + CONFIG.md flag match; view-SQL helper documented; no forward references.
- [ ] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol; the path-formatting comment is self-contained.
- [ ] Full docs/REVIEW.md checklist; TESTING.md layering (unit builder tests + DuckDB integration for cross-tier/freshness/isolation).

## 7. Reviewer notes

_(empty until first review)_
