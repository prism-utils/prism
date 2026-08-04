# Spec: Loki-compatible logs API + remote-reader parity (logging-migration)

Status: IN_REVIEW

- **Slug / branch:** `feat/loki-compat-logs`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Store / query surface (extends `internal/store/query`; closes prism#73 + #75)
- **Issues:** [prism#73](https://github.com/elk-utilities/prism/issues/73), [prism#75](https://github.com/elk-utilities/prism/issues/75) (`logging-migration`)

## 1. Task

Give prism-store a **Grafana-native logs read surface** parallel to PromQL for
metrics: a **Loki-compatible HTTP API** over the existing file-backed `logs`
relation, and prove it works on the **remote reader** topology (`QUERY_HOT_ONLY`,
RO data dir / `prism-cache`) the same way `/sql` and PromQL do for metrics.
Homelab can then provision a stock Loki datasource instead of ClickHouse.
Update docs; ship well-tested (unit + Docker e2e); release follows merge.

## 2. Scope

- **In scope:**
  - `internal/store/query/`: Loki handlers + LogQL **subset** parser + SQL/adapter
    over the existing sandbox `logs` view (reuse row/time/memory caps,
    `union_by_name` path already used by `/sql`).
  - Routes (with `ROUTE_PREFIX`):  
    `GET|POST {prefix}/{ns}/loki/api/v1/query_range`  
    `GET|POST {prefix}/{ns}/loki/api/v1/labels`  
    `GET|POST {prefix}/{ns}/loki/api/v1/label/{name}/values`
  - Wire in `cmd/prism-store/main.go`: `LOKI_API_ENABLED` (default `true`),
    RBAC `query`, `OwnedTenantGuard`, share `/sql` in-flight queue; cluster
    coordinator forwards the new patterns.
  - Timestamp: logs Parquet has no event-time column (OUTPUT_CONTRACT); stamp
    each row with the **landing file mtime** (ingest time), nanoseconds in Loki
    `values` pairs.
  - Line + labels: `message` (or `template` when `message` is NULL) as the log
    line; string columns `format`, `template`, and other non-numeric columns as
    stream labels (skip `count`; put `count` as label string when present).
  - LogQL subset (v1): stream selector `{label="value", label2=~"re"}` plus
    optional line filter `|= "substr"` / `|~ "regex"` / `!=` / `!~`. No metric
    LogQL (`rate`, `count_over_time`, …) in this cut — return `400` with a clear
    error. Empty/missing selector treated as match-all for Explore friendliness
    (document); Grafana often sends `{job="…"}` — also accept synthetic
    `job="prism"` on every stream so a default `{job="prism"}` works.
  - **#75:** Docker e2e with **writer** (ingest + jobs) + **reader**
    (`RUN_JOBS=false`, `QUERY_HOT_ONLY=true`, RO mount of writer data): after
    ingest on writer, reader serves `/sql` `FROM logs` **and** Loki
    `query_range` / `labels`. Document in `STORE.md` that logs are file-backed
    and **unaffected by `QUERY_HOT_ONLY`** (no metrics hot catalog).
  - Docs: `docs/STORE.md`, `docs/CONFIG.md`, `docs/TESTING.md` (+ DESIGN ADR
    note if PromQL section has a sibling).
  - Make target e.g. `make loki-e2e` (docker-compose), analogous to `promql-e2e`.
  - Unit tests first (TDD); edge cases per TESTING.md.

- **Out of scope:**
  - Full LogQL / metric queries / tail websocket / push API / series/volume.
  - Custom Grafana plugin; ClickHouse; homelab reconciler wiring.
  - Metrics-style `hot_current` for logs; rollups/retention changes for logs.
  - Changing OUTPUT_CONTRACT or agent `--quick logs` (already ships summary).

## 3. Open questions — resolved

- [x] Q: Loki API vs Grafana plugin? — A: **Loki API** (user + product decision;
  mirrors PromQL; no unsigned plugin).
- [x] Q: Full LogQL or subset? — A: **Subset** (selectors + line filters) enough
  for Grafana Explore Logs; metric LogQL deferred.
- [x] Q: Timestamp source? — A: **File mtime** at query time (ingest/land time);
  contract forbids event timestamps in Parquet.
- [x] Q: Summary-only windows (`--quick logs`)? — A: Emit streams with
  `template` (+ `count` label); line = template text (or `message` when set).
- [x] Q: Same PR for #73+#75? — A: **Yes** — one store surface; reader e2e is
  acceptance for both.

## 4. Decision log

- **Loki-compatible HTTP API (not a Grafana plugin):**
  - ref: https://grafana.com/docs/loki/latest/reference/loki-http-api/ — stock
    Grafana Loki datasource speaks `query_range` / `labels` / `label/…/values`.
  - perf: one SQL scan per request over existing sandbox; no second engine.
  - product: same friction model as PromQL metrics; no plugin sign/ship matrix.

- **LogQL subset only (selectors + line filters):**
  - ref: Grafana Explore sends LogQL stream selectors; metric LogQL is optional
    for first “see my logs” UX (same docs page).
  - perf: push label predicates into DuckDB WHERE; line filters in SQL
    `contains`/`regexp_matches` — avoid pulling unbounded rows (honor `limit`).
  - product: clear 400 on unsupported syntax beats a half-broken full parser.

- **Timestamps = landing file mtime:**
  - ref: `docs/OUTPUT_CONTRACT.md` §3.2 — “Timestamp fields are never ingested
    (storage stamps ingest time).”
  - perf: one `Stat` per parquet file; broadcast ts to rows in that file.
  - product: honest ingest-time axis; matches store semantics.

- **Synthetic `job="prism"` label on every stream:**
  - ref: Grafana Loki datasource often requires a non-empty stream selector;
    Prometheus/Loki ops convention uses `job`.
  - perf: negligible label cardinality (+1).
  - product: default Explore query `{job="prism"}` works out of the box.

- **Reader e2e shares host volume writer→reader (Docker):**
  - ref: existing `promql-e2e` compose pattern + STORE reader/writer split;
    https://docs.docker.com/engine/storage/volumes/
  - perf: N/A (test only).
  - product: proves the homelab `prism-cache` topology before consumers cut over.

## 5. Acceptance checklist  (developer checks these off)

- [x] Loki handlers registered; JSON envelopes match Loki success/error shapes
      (`status`, `data.resultType=streams`, `result[].stream`, `values: [[ts_ns, line], …]`).
- [x] `query_range` / `labels` / `label/{name}/values` implemented with LogQL subset;
      unsupported LogQL → 400; empty tenant → empty success (not 500).
- [x] RBAC `query` + tenant isolation + cross-tenant denied (mirror PromQL tests).
- [x] `LOKI_API_ENABLED` (default true) + CONFIG.md / STORE.md / TESTING.md updated;
      reader/writer + `QUERY_HOT_ONLY` behavior for logs documented.
- [x] Unit tests (handlers, LogQL parse, mtime ts, limit/direction, empty tenant).
- [x] Docker e2e: `--quick logs` (or land fixtures) → writer ingest → **reader**
      Loki `query_range` + `/sql` FROM logs; `make loki-e2e` green.
- [x] Tests written first (`test:` commit precedes implementation) — CONTRIBUTING.md §1.
- [x] `make lint test` green locally; loki-e2e green.
- [x] Cluster route patterns include Loki paths (unit or wiring test).

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_

## 8. Developer notes (delivery)

- **Surface:** `internal/store/query/{loki.go,loki_handler.go,loki_sql.go,logql.go}`;
  routes `GET|POST <prefix>/{ns}/loki/api/v1/{query_range,labels,label/{name}/values}`
  (GET+POST on all three, per §2). Wired in `cmd/prism-store/main.go` behind
  `LOKI_API_ENABLED` (default true), RBAC `query`, the shared `/sql` in-flight
  queue, and `OwnedTenantGuard`; `cluster.NewServeMux` forwards every pattern.
- **Sandbox:** logs are file-backed, so the handler needs no engine and no hot
  snapshot. It opens the same hardened `:memory:` sandbox `/sql` uses
  (`allowed_directories`, extension hardening, `lock_configuration`) and creates a
  logs relation whose rows carry `__prism_ts_ns` = the landing file's mtime,
  unified across windows with `UNION ALL BY NAME`.
- **Caps:** no new limit envs — `SQL_API_MAX_ROWS` caps entries per query
  (`limit` defaults to Loki's 100), `SQL_API_TIMEOUT_SECONDS` bounds execution,
  `DUCKDB_MEMORY_LIMIT` / `DUCKDB_THREADS` govern the sandbox.
- **Labels:** text columns + synthetic `job="prism"`; `count` as a label string;
  `message` is the line (never a label) and falls back to `template`; NULL/empty
  values and illegal label names are omitted.
- **Tests:** `logql_test.go` (subset parse, unsupported vs malformed, matcher
  semantics), `loki_internal_test.go` (config `Validate`, defaults, env, route
  patterns, time parsing), `loki_api_test.go` (streams/mtime/limit/direction/time
  range/line filters/summary window/mixed schemas/empty tenant/404/isolation/
  labels/values/POST), `loki_rbac_test.go` (reader/writer/cross-tenant/JWT +
  owned-tenant guard), `cluster/router_loki_test.go`,
  `cmd/prism-store/loki_routing_test.go`, `test/e2e/loki_e2e_test.go`
  (`make loki-e2e`: agent → writer → read-only reader).
- **Verification:** `make lint test` green; `make loki-e2e` green (reader served
  11 rows via `/sql FROM logs` and 4 streams / 11 entries via the Loki API from
  the writer's read-only mount).
