# Spec: /sql log prune + artifact views

Status: READY

- **Slug / branch:** `cursor/sql-logs-prune-baa6`
- **Owner phase:** orchestrator
- **PLAN phase(s):** store query SQL sandbox

## 1. Task

Grafana must query logs and events only through prism (`POST /{ns}/sql`), never by globbing tenant parquet from a Grafana DuckDB plugin. For that path to stay fast and isolated, `/sql` must time-prune log files the same way Loki does, and must expose artifact-scoped views (`logs_raw`, `logs_template`, `logs_summary`) so a summary panel does not open raw L5 segments. Also return object `records` so Grafana Infinity can plot JSON without unzipping `rows`/`columns`.

## 2. Scope

- **In scope:** `internal/store/query` `/sql` sandbox (`prepareSandboxConn`, JSON body), tests, STORE.md SQL relation table.
- **Out of scope:** Loki/PromQL planners (already prune). Grafana plugin install and dashboard JSON (homelab-apps).

## 3. Open questions

- [x] Q: Keep `logs` as a full union? — A: Yes. Artifact views are extra. Existing `FROM logs` tests stay valid. Grafana panels use `logs_summary` / `logs_raw`.
- [x] Q: Grafana time macros vs `__prism_ts_ns`? — A: Views add `proxy_ts` from ingest ns (microsecond `make_timestamp`) so panel SQL can keep `proxy_ts BETWEEN …`. File prune uses request `start`/`end`.

## 4. Decision log

- Time-prune `/sql` logs with the same `filterLogFiles` window-id heuristic Loki uses.
  - ref: https://duckdb.org/docs/stable/data/parquet/overview.html — `read_parquet` opens every listed file before `WHERE`.
  - perf: a 6h Grafana window must not open an 800MiB L5 whose window id is weeks old.
  - product: docs already claimed `/sql` prunes when `start`/`end` are set; metrics did, logs did not.
- Artifact-scoped views, not a filtered union of all artifacts.
  - ref: same DuckDB open-then-filter behavior — `WHERE count IS NOT NULL` still opens raw files in a union.
  - perf: volume/template panels only open that artifact's segments.
  - product: matches the Grafana view names already used (`logs_raw` / `logs_template` / `logs_summary`).
- Additive `records` array of objects on the JSON success body.
  - ref: https://grafana.com/docs/plugins/yesoreyeram-infinity-datasource/latest/query/ — Infinity root selector wants an array of objects.
  - perf: one extra map per row vs Grafana-side JSONata zip; bounded by `max_rows`.
  - product: Loki/Prom stay as-is; SQL panels can use Infinity without a custom plugin.

## 5. Acceptance checklist

- [ ] `POST /sql` with `start`/`end` covering only a recent log file returns that file's rows from `logs` with no SQL `WHERE` (old filename window is not opened).
- [ ] `logs_raw` / `logs_template` / `logs_summary` are queryable; a raw-only row does not appear in `logs_summary`.
- [ ] `logs` remains the union of all artifacts (existing count tests).
- [ ] Success JSON includes `records` objects keyed by column name.
- [ ] `proxy_ts` is present on log views.
- [ ] Tests written first (`test:` commit precedes implementation)
- [ ] `make lint test` green locally

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
