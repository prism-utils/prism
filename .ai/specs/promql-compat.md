# Spec: PromQL-compatible read API for prism-store (Prometheus compatibility)

Status: READY
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `feat/promql-compat`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Store / query surface (docs/DESIGN.md §15 ADR; extends `internal/store/query`)

## 1. Task

Make prism Prometheus-compatible on the **read** path so any Prometheus exporter
(scraped by the `prism` agent) can be queried with **PromQL** through the store,
with results consumable by Grafana's Prometheus datasource and any PromQL client.
We embed the canonical Prometheus PromQL engine
(`github.com/prometheus/prometheus/promql`) over a **DuckDB-backed
`storage.Queryable`** that reads a tenant's existing metrics parquet, and expose
the standard Prometheus HTTP API (`/{ns}/api/v1/{query,query_range,series,labels,
label/<name>/values}`). The feature is **read-only and additive**: no change to
ingest, the agent, the frozen output contract, or on-disk layout. It reuses the
store's existing hardened per-request DuckDB **sandbox** (tenant isolation,
`hot-only` mode), **RBAC** (`query` action), **cluster** routing, and the `/sql`
**in-flight queue** — so all cross-cutting features work unchanged.

This is delivered as **one comprehensive PR** at the user's explicit direction
(overriding CONTRIBUTING.md's small-PR norm and the loop's human-review gates:
the user waived interactive review and design approval for this task).

## 2. Scope

- **In scope:**
  - New files under `internal/store/query/`:
    - `promql_adapter.go` — `storage.Queryable`/`Querier`/`SeriesSet`/`Series` +
      sample iterator backed by a sandbox `*sql.Conn` (streams a DuckDB cursor
      sorted by `labels, ts`; parses the `labels` VARCHAR into `labels.Labels`;
      applies matchers; bounded).
    - `promql_engine.go` — engine construction from `PromQLConfig`
      (`MaxSamples`, `Timeout`, `LookbackDelta`), instant/range/series/labels
      execution helpers.
    - `promql_handler.go` — the five Prometheus HTTP API handlers + the exact
      Prometheus JSON envelope (success/error, vector/matrix/scalar/string) +
      route patterns + `PromQLConfig`/`PromQLConfigFromEnv`.
  - Wiring in `cmd/prism-store/main.go`: register `/{ns}/api/v1/*` on the query
    plane when `PROMQL_API_ENABLED` (default `true`), wrapped with RBAC `query`,
    `OwnedTenantGuard`, and the `/sql` queue limiter (reused).
  - Cluster: add the new route patterns to `internal/store/cluster` coordinator
    mux so PromQL is forwarded to the owning client (same route-to-owner model).
  - E2E: `deploy/docker-compose.promql-e2e.yml` (real exporter → `prism` agent →
    `prism-store`) + `test/e2e` PromQL test (build tag `e2e`) + `make promql-e2e`.
  - Benchmark: `BenchmarkPromQL*` hot-path microbench in `internal/store/query`
    (+ `make bench-promql` wrapper) quantifying the new query path and proving no
    `allocs/op` regression on existing hot paths.
  - Docs: `docs/DESIGN.md` §15 ADR entry, `docs/STORE.md` (feature + section),
    `docs/CONFIG.md` §14 (env), `docs/TESTING.md` (new target). Dependency added
    via `go get` at latest release (`github.com/prometheus/prometheus`), `make tidy` clean.

- **Out of scope (explicit):**
  - No data-model / schema / output-contract change (labels parsed at query
    time; `ts` is the sample time). No `labels_map` column.
  - No ingest/agent change; no Prometheus **remote_write** receiver.
  - No PromQL **write**/rules/alerting; no `/api/v1/query_exemplars`,
    `/metadata`, `/targets`, `/rules`, admin/TSDB endpoints.
  - No PromQL over the **rollup** projection's aggregated columns (rollups drop
    labels); PromQL unions hot + tiers only for label fidelity (documented).
  - No native histograms beyond what the engine yields from float samples.
  - No logs surface (PromQL is metrics-only — the "engine encoding": logs
    tenants never expose it).

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Embed the Prometheus engine or translate PromQL→SQL? — A: **Embed** the
  canonical `promql.Engine` (real compatibility). Feasibility spiked: engine +
  `storage` + `parser` compile; agent `CGO_ENABLED=0` build unaffected; agent
  import graph has 0 `prometheus/prometheus` refs.
- [x] Q: Structured labels migration now, or parse at query time? — A: **Parse at
  query time.** Zero migration, works on all existing parquet, contract
  untouched, and memory-optimal (DuckDB does selective filter+sort; we stream).
- [x] Q: Sample timestamp source? — A: Use stored **`ts`** (same time axis as
  `/query` and `/sql`). Honoring `timestamp_ms` when non-zero is a future refinement.
- [x] Q: Where do the handlers live? — A: In `internal/store/query` (a query
  responsibility per the ADR) to reuse the unexported hardened sandbox helpers
  without exporting internals.
- [x] Q: How is the feature gated ("engine encoding")? — A: `PROMQL_API_ENABLED`
  (default `true`) registers the metrics-only Prometheus API; it queries the
  `metrics` view only. Logs tenants inherently never expose PromQL.
- [x] Q: Memory strategy for hot-only clients? — A: Reuse the sandbox `hot-only`
  union (hot snapshot only); bound total samples with `PROMQL_MAX_SAMPLES`
  (default 50,000,000, Prometheus default); reuse `DUCKDB_MEMORY_LIMIT`/threads
  and the `/sql` queue; stream a sorted cursor (no full materialization).

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Embed canonical PromQL engine over a custom `storage.Queryable`: **chosen**
  - ref: https://github.com/prometheus/prometheus/blob/main/promql/engine.go —
    `QueryEngine` interface + `EngineOpts` are designed to run over any
    `storage.Queryable`; Thanos embeds/replaces it the same way
    (https://thanos.io/tip/components/query.md/).
  - perf: engine bounds memory with `EngineOpts.MaxSamples`; we push the
    selective `__name__`+time filter and `ORDER BY labels,ts` into DuckDB
    (spillable, `DUCKDB_MEMORY_LIMIT`-capped) and stream the cursor, so the
    adapter holds ≈one series at a time.
  - product: only the real engine gives true PromQL semantics (range vectors,
    `rate`, staleness, `@`/offset), which is the whole point ("any query").
- Memory bound via `MaxSamples`: **default 50,000,000**
  - ref: https://prometheus.io/docs/prometheus/latest/querying/api/ and
    `--query.max-samples` (Prometheus default 50000000) — a single query cannot
    load more samples than this into memory.
  - perf: hard per-query ceiling; queries exceeding it fail fast rather than OOM.
  - product: matches operators' mental model from upstream Prometheus.
- Labels parsed at query time (no `labels_map` migration): **chosen**
  - ref: DuckDB pushes the selective predicate/sort
    (https://duckdb.org/docs/stable/sql/query_syntax/orderby); Prometheus label
    matching semantics (https://prometheus.io/docs/prometheus/latest/querying/basics/).
  - perf: `__name__`+time is highly selective; parsing cost is O(rows returned),
    label sets interned per distinct string within a query. Avoids rewriting the
    frozen contract, parser, rollups, and seeds, and works on already-written tiers.
  - product: PromQL works on all historical data immediately; smaller, safer change.
- HTTP envelope = exact Prometheus API JSON: **chosen** (hand-marshaled)
  - ref: https://prometheus.io/docs/prometheus/latest/querying/api/ —
    `{"status","data":{"resultType","result"}}`, vector `value:[ts,"v"]`, matrix
    `values:[[ts,"v"]]`, error `{"status":"error","errorType","error"}`.
  - perf: tiny controlled marshaling; avoids importing `web/api/v1` (heavy).
  - product: Grafana's Prometheus datasource + any PromQL client work unchanged.
- Reuse existing sandbox/RBAC/cluster/queue rather than new machinery: **chosen**
  - ref: prism DESIGN.md §15 (query is a store responsibility; sandbox hardening).
  - perf/product: inherits tenant isolation, hot-only memory, backpressure, and
    per-tenant routing for free; no new security surface.

## 5. Acceptance checklist  (developer checks these off)

- [ ] `go get github.com/prometheus/prometheus@latest`; `make tidy` clean; agent
      `CGO_ENABLED=0 go build ./cmd/prism` still green and import graph free of
      `prometheus/prometheus`.
- [ ] `promql_adapter.go`: streaming, sorted `storage.Queryable` over a sandbox
      conn; matcher application; `__name__`+time+sort pushdown; empty-tenant and
      hot-only correctness.
- [ ] `promql_engine.go`: engine built from `PromQLConfig`; instant + range +
      series + labels/label-values execution; `MaxSamples` enforced (exceed → error).
- [ ] `promql_handler.go`: five `/api/v1/*` handlers; exact Prometheus JSON
      envelope for vector/matrix/scalar/string + error mapping (400 bad expr/param,
      404 unknown tenant, 422 execution error, 503/limit); GET+POST where upstream allows.
- [ ] `PromQLConfig` + `PromQLConfigFromEnv` with `Validate()` naming bad paths;
      defaults from a single source.
- [ ] `cmd/prism-store` wiring: routes registered on the query plane when
      `PROMQL_API_ENABLED`; RBAC `query`, `OwnedTenantGuard`, queue limiter applied.
- [ ] Cluster coordinator forwards the new `/api/v1/*` patterns to the owning client.
- [ ] Tests written first (`test:` commit precedes implementation) — unit
      (adapter/engine/handler), edge cases (bad expr, unknown/symlink tenant,
      empty tenant, MaxSamples exceeded, cancellation/timeout, hot-only), cluster
      routing, Prometheus-envelope golden.
- [ ] `BenchmarkPromQL*` added; existing parser/encoder microbench shows no
      `allocs/op` regression vs the captured baseline.
- [ ] `deploy/docker-compose.promql-e2e.yml` + `test/e2e` PromQL test + `make
      promql-e2e`; **run locally and green** (real exporter → agent → store → PromQL).
- [ ] Docs updated: DESIGN §15, STORE.md, CONFIG.md §14, TESTING.md.
- [ ] `make lint test` green locally; `make full-tests` green (I/O + wiring touched).

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
