# Spec: prism-store Prometheus exporter (USE KPIs)

Status: READY

- **Slug / branch:** `feat/prism-store-metrics`
- **Ships as:** next patch after current tip (expect `v1.9.9` or `v1.10.0` — tag after merge)
- **Owner phase:** orchestrator → developer → reviewer → merge → release → homelab scrape + `/admin/resources`

## 1. Task

Operators need proper USE-style visibility into the shared prism-store writer: Utilization (CPU/mem/queue slots), Saturation (queue depth, open-tenant LRU, landing-file caps), Errors (HTTP 5xx/429, lifecycle failures), and query load per tenant.

Today the store exposes `/admin/queue` JSON and frozen `/stats` billing JSON, but **no Prometheus `/metrics`**. Homelab kube-prometheus cannot scrape KPIs, so `/admin/resources` cannot chart prism the way it charts namespace CPU/memory.

This task adds a first-class Prometheus exporter on prism-store and instruments the high-value surfaces (HTTP, queue, engine LRU, lifecycle/files, errors). Homelab scrape wiring + admin charts are a sibling task.

## 2. Scope

**In scope**
- `internal/store/metrics` (or equivalent): private `prometheus.Registry` (not global default — tests must not leak)
- Register Go + process collectors (`go_*`, `process_*` including RSS and open FDs)
- `GET /metrics` on the same mux as `/healthz`/`/readyz` (both planes when split — unauthenticated scrape; NetworkPolicy is the gate in k8s)
- Env: `METRICS_ENABLED` default **true**; optional `METRICS_PATH` default `/metrics`
- Instrument:
  - Queue: in-flight, depth, limits, rejected_total (reasons), wait histogram — reuse `Limiter.Snapshot` / existing rejected counter; prefer gauges updated from Snapshot or atomic sources
  - HTTP: request counter + duration histogram with **low-cardinality route labels** (`sql`, `promql`, `loki`, `ingest`, `stats`, `admin_queue`, `admin_ensure`, `query`, `healthz`, …) — **never** raw tenant path
  - Per-tenant query counters: `prism_store_queries_total{tenant,route}` and error counters `{tenant,route,code_class}` where tenant is known from the request — cardinality opt-in via `METRICS_PER_TENANT` default **true** for this product (shared multi-tenant writer needs it); document the series budget
  - Engine: open tenants gauge, max open, eviction counter
  - Lifecycle (from existing tick paths, not sync disk walks on scrape): segment/log landing file gauges refreshed on tick; flush/merge/retention counters; compaction CPU; last successful tick timestamp; tick errors
- Helm chart: container port name `metrics`, optional ServiceMonitor template behind values flag
- Docs: CONFIG, STORE, MEMORY/MIGRATION as needed; DESIGN decision entry

**Out of scope**
- Changing frozen `/stats` JSON
- Homelab ServiceMonitor enablement / NetworkPolicy / admin UI (sibling `homelab-apps` spec)
- Alertmanager rules in prism repo

## 3. Open questions — resolved

- [x] Metrics on public port vs dedicated port — **A:** same listen port as healthz (8080 in prism-proxy), unprefixed `/metrics`. Dedicated port deferred; NetworkPolicy must allow kube-prometheus to 8080 (homelab owns that carefully).
- [x] Per-tenant labels — **A:** enabled by default (`METRICS_PER_TENANT=true`) because the product ask is per-tenant + ALL aggregation via `sum without (tenant)`. Keep HTTP route labels low-cardinality; only add `tenant` where the handler already knows the namespace.
- [x] Disk walks on scrape — **A:** **No.** File gauges updated from lifecycle ticks / existing stats helpers already invoked on ticks.
- [x] Auth on `/metrics` — **A:** unauthenticated (standard scrape); rely on NetworkPolicy.

## 4. Decision log

- **Private registry + standard Go/process collectors**
  - ref: https://prometheus.io/docs/guides/go-application/ — client_golang registry + collectors
  - perf: process RSS/CPU/FDs free; no custom cgroup parsing
  - product: matches what `/admin/resources` already charts via cAdvisor; store-native series fill the gap (queue, queries, files)

- **USE-oriented metric set (Utilization / Saturation / Errors)**
  - ref: http://www.brendangregg.com/usemethod.html — USE method
  - perf: gauges for saturation (queue depth, open tenants, landing files vs limit); counters for errors/rejects; histograms for latency
  - product: operators can answer “is prism saturated?” without reading logs

- **Low-cardinality HTTP route label; tenant only on explicit series**
  - ref: https://prometheus.io/docs/practices/naming/ + cardinality guidance
  - perf: avoids unbounded series from path templates
  - product: ALL = `sum(...)`; per-tenant = filter `tenant="…"`

## 5. Acceptance checklist

- [ ] `GET /metrics` returns Prometheus text; includes `go_*` / `process_*` when enabled
- [ ] Queue gauges/counters present and move under load (tests)
- [ ] HTTP duration + request totals with bounded route labels
- [ ] Per-tenant query/error series when `METRICS_PER_TENANT=true`; absent/empty when false
- [ ] Engine open-tenant gauges + eviction counter
- [ ] Lifecycle file/job metrics updated from ticks (unit/integration coverage for at least one path)
- [ ] Helm values + optional ServiceMonitor template + golden updated
- [ ] Docs updated (CONFIG defaults, STORE observability section)
- [ ] Tests first (`test:` commit); `make lint test` green (+ full-tests if wiring warrants)

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases**
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty)_
