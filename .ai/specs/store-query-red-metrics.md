# Spec: query-plane RED metrics (prism#113)

Status: ALL_OK
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `feat/store-query-red-metrics`
- **Issue:** https://github.com/elk-utilities/prism/issues/113
- **Ships as:** next patch after tip (`v1.9.14` expected; tag after merge)
- **Owner phase:** orchestrator → developer → reviewer → merge → release
- **PLAN phase(s):** store observability (extends `feat/prism-store-metrics` / #106)

## 1. Task

Platform o11y (Grafana sidecar + prism-alert → prism-store over ClusterIP) has no
query-plane RED series with the names dashboards expect. `#106` already shipped
a USE exporter (`http_*`, `queries_total`, queue, lifecycle), but the platform
contract needs the exact query RED family:

- `prism_store_query_requests_total{api,code,tenant}`
- `prism_store_query_duration_seconds{api,tenant}`
- `prism_store_query_inflight{api}`

with `api ∈ {promql, loki, sql}`, including authz/guard/queue rejects, and tests
that assert scrape text moves as expected on success and 4xx/5xx.

## 2. Scope

- **In scope:**
  - Add the three query RED metric families above to `internal/store/metrics`
    (same private registry as `#106`; respect `METRICS_ENABLED` /
    `METRICS_PER_TENANT` / tenant-label cap + overflow).
  - Wire `InstrumentQuery` (or equivalent) **outermost** on PromQL, Loki, and
    SQL handler chains in `cmd/prism-store` (and cluster-mode equivalents if
    those routes are served) so authz, owned-tenant guard, and queue 429s are
    counted.
  - `api` is a closed enum: `promql` | `loki` | `sql` only (never path-derived).
  - `query_inflight` Inc/Dec around the whole handler (counts queued waits).
  - When `METRICS_PER_TENANT=false`, omit the `tenant` label from
    `query_requests_total` / `query_duration_seconds` (same pattern as existing
    per-tenant series — drop the label, do not emit empty).
  - Docs: list the three metric names in `docs/STORE.md` (and `docs/CONFIG.md`
    only if a new knob is added — prefer none).
  - **Tests must assert reported series:** scrape `/metrics` (or registry
    gather) after exercising handlers and assert exact counter/histogram/gauge
    samples for success (`code="200"`), 4xx, and 5xx (and inflight under load).
- **Out of scope:**
  - Ingest HTTP request/status/latency counters (Traefik covers public ingest).
  - Removing or renaming existing USE series from `#106`.
  - Optional job metrics (`job_runs_total` / …) — already covered by
    `lifecycle_*` from `#106`; do not dual-publish.
  - Homelab scrape / dashboard wiring (sibling repo).
  - Duplicating `/stats` billing JSON as Prometheus.

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Add `#113` names alongside `#106`, or reshape/rename? — **A:** Additive.
  `#106` series stay; new query RED family matches the platform o11y contract.
- [x] Q: Include structured `GET /{ns}/query` under `api`? — **A:** No. Issue +
  platform plan list only `promql|loki|sql`. Structured query remains on
  existing `http_*` / `queries_total` with `route="query"`.
- [x] Q: Ship optional `job_*` aliases? — **A:** No. `lifecycle_*` already
  covers writer health; dual names add scrape cost without product value.
- [x] Q: New env knobs? — **A:** None. Reuse `METRICS_*` from `#106`.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Additive query RED family with exact `#113` names (do not rename USE series).**
  - ref: https://prometheus.io/docs/practices/naming/ — stable names are a
    consumer contract; platform o11y PromQL already targets these names
    (homelab `.ai/specs/platform-o11y/services/prism-proxy.md`).
  - perf: three more families on the existing private registry; labels bounded
    (`api`×3, `code` low-card, `tenant` capped at 256 + overflow).
  - product: dashboards/alerts can ship against the audited contract without
    breaking anyone already scraping `#106` USE series.

- **Middleware outermost; inflight includes queue wait.**
  - ref: https://github.com/prometheus/client_golang/blob/main/prometheus/promhttp/instrument_server.go
    (`InstrumentHandlerInFlight` / duration / counter pattern) + RED method
    (rate/errors/duration as request SLIs).
  - perf: one status-capturing wrapper per request (same cost class as existing
    `Instrument`); gauge Inc/Dec is atomic and allocation-free on the hot path
    beyond the existing recorder.
  - product: authz 401/403/404, guard rejects, and SQL queue 429s show up in
    the same counter as handler outcomes — brownouts are visible.

- **Skip optional `job_*` series; keep `lifecycle_*`.**
  - ref: issue #113 “Optional — only if cheap”; existing
    `prism_store_lifecycle_ticks_total` /
    `lifecycle_tick_duration_seconds` /
    `lifecycle_last_success_timestamp_seconds`.
  - perf: zero extra series.
  - product: writer health already scrapable; platform gap called out is query
    RED, not job aliases.

## 5. Acceptance checklist  (developer checks these off)

- [x] `prism_store_query_requests_total{api,code,tenant?}` increments on
      promql/loki/sql success and on 4xx/5xx (incl. authz/guard/queue rejects)
- [x] `prism_store_query_duration_seconds{api,tenant?}` observes end-to-end
      handler time for those APIs
- [x] `prism_store_query_inflight{api}` rises while a request is in flight
      (including while queued) and returns to baseline after
- [x] `GET /metrics` scrape (already mounted) includes the new series when
      metrics enabled; series absent/inert when disabled
- [x] Docs in `docs/STORE.md` list the three metric names + labels (`api`
      enum, tenant cardinality note)
- [x] **Tests assert reported metrics:** unit/integration coverage scrapes the
      registry/handler and asserts expected sample lines/values for success,
      4xx, and 5xx (not merely “handler returns status”)
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [x] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [x] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

<!-- Reviewer appends one actionable line under any gate it unchecks. Set
     Status: ALL_OK only when every box above is checked; otherwise
     Status: CHANGES_REQUESTED. -->

**2026-08-10 — ALL_OK.** Test-first history confirmed (`275ca69` test → `224bb9a` feat).
`make lint test` and `make full-tests` green (cmd wiring touched). Query RED
middleware outermost on promql/loki/sql; scrape assertions cover success/4xx/5xx
(incl. authz), per-tenant off, inflight, disabled/nil. STORE.md + CONFIG.md +
package doc match delivered series; comments describe local intent only.
