# Spec: prism-store opt-in memory observation

<!--
  This file IS the loop state (see .ai/workflows/feature-loop.md).
-->

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/memory-observe-baa6`
- **Owner phase:** reviewer
- **PLAN phase(s):** store observability (metrics)
- **Ships as:** annotated tag `v1.0.7` after squash-merge to `main` (image `ghcr.io/prism-utils/prism-store:1.0.7`)

## 1. Task

Add **opt-in memory observation** on `prism-store` so operators can see, during heavy production usage, whether RSS/cgroup growth is Go heap, DuckDB off-heap, which DuckDB role is open, and which lifecycle job is running. Default **off**. When on, extra Prometheus series on the existing `/metrics` listener plus one structured log line per lifecycle job. Must include **e2e**. After merge, tag `v1.0.7` so homelab can pin the image. Homelab chart/gitops pin is out of scope (parent agent after the image exists).

## 2. Scope

- **In scope:**
  - Env `MEMORY_OBSERVE` (bool, default `false`) on `cmd/prism-store`, independent of `METRICS_ENABLED`.
  - New observe collector/families in `internal/store/metrics` (private registry, closed labels).
  - DuckDB open-instance Inc/Dec by closed `role` at the listed production open/close sites.
  - Lifecycle job start/end RSS / heap / cgroup snapshots via `observed()` / `ObserveTick`.
  - One slog line at job **end** when observe is on.
  - Parse `GOMEMLIMIT` and `DUCKDB_MEMORY_LIMIT` with the same byte-size rules as `internal/config.parseByteSize` (`1638MB` decimal vs `1433MiB` binary).
  - Docs: `docs/CONFIG.md` (new env), `docs/STORE.md` observability table.
  - Chart: expose `MEMORY_OBSERVE` under existing `metrics.*` values→env pattern (`metrics.memoryObserve: "false"`), matching `METRICS_ENABLED` / `METRICS_PATH` / `METRICS_PER_TENANT`. Update golden.
  - Unit tests (test-first commit) + `test/e2e/` (`//go:build e2e`) included in `make e2e`.
- **Out of scope:**
  - Homelab chart/gitops image pin or enabling observe on cluster.
  - Public `/debug/pprof` (or any pprof listener on `:8080`).
  - Extra SQL/`PRAGMA` against live DuckDB to read its memory tables.
  - Duplicating `go_*` / `process_*` (including `go_memstats_*` and `process_resident_memory_bytes`).
  - Tenant labels on the new families.
  - A sampling goroutine.
  - Instrumenting test-only DuckDB opens (`internal/store/testparquet`, `*_test.go`, seed helpers) or engine log-coalesce’s extra in-memory connector (not in the role set below).

## 3. Open questions  (must be empty/answered before `Status: READY`)

All resolved in Phase 0. Do not re-ask.

- [x] Q: Enable flag? — A: env `MEMORY_OBSERVE` bool, default **false**. Independent of `METRICS_ENABLED`. If `METRICS_ENABLED=false`, observe is inert even if `MEMORY_OBSERVE=true` (no scrape). If metrics on and observe off: **zero extra series, zero extra slog, zero cgroup reads, no DuckDB Inc/Dec export**. Hot-path Inc/Dec of instance counters may be skipped entirely when observe is off (check a bool; no lock).
- [x] Q: Duplicate `go_*` / `process_*`? — A: **Do not duplicate.** `NewGoCollector` + `NewProcessCollector` already export `go_memstats_*` and `process_resident_memory_bytes`. Observe adds only what they lack: cgroup, DuckDB instance count by role, per-job RSS/cgroup/heap snapshots, observe-enabled gauge, optional job slog.
- [x] Q: How to measure DuckDB RAM? — A: **Do not** run extra SQL/`PRAGMA` against live DuckDB. Track **open instance count by closed `role` label** in Go (`engine`, `merge`, `rollup`, `materialize`, `sql`, `promql`, `loki`, `bounds`, `stat`). Combine with cgroup/RSS at job boundaries to infer which role is expensive.
- [x] Q: pprof? — A: **Out of scope this PR.** Metrics + slog are enough to scrape from kube-prometheus. No public `/debug/pprof` on `:8080`.
- [x] Q: Cardinality? — A: Closed label sets only. `role` as above. `job` already closed (`hot_snapshot`, `flush`, `merge`, `retention`). No tenant label on the new families. No sampling goroutine; scrape-time reads + atomic gauges + ObserveTick hook.
- [x] Q: E2E shape? — A: `test/e2e/` with `//go:build e2e`. Prefer spawning the **prism-store binary** (or `go test` helper that starts `runStore`) with temp `DATA_DIR` — docker not required if HTTP in→out is real. Must run under `make e2e`. Cases: observe on → series present after ingest; observe off → new families absent (or `prism_store_memory_observe 0` and no cgroup/duckdb_open/job_memory families); after ingest, `role="engine"` ≥ 1. Also unit tests for collector/config (test-first commit).
- [x] Q: Version? — A: After squash-merge to main, **create annotated tag `v1.0.7`** and push it to origin so `.github/workflows/release.yml` builds GHCR. Orchestrator owns tag+watch after `ALL_OK`.
- [x] Q: Homelab pin / enable on cluster? — A: **Out of scope.** Parent Homelab orchestrator after 1.0.7 exists.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Extra series only when `MEMORY_OBSERVE=true`, not always-on:
  - ref: https://prometheus.io/docs/practices/instrumentation/#do-not-over-instrument — keep default scrape cheap; debug cardinality on demand.
  - perf: cgroup file reads and MemStats on scrape/job-end only when enabled; ingest path stays allocation-neutral.
  - product: operators turn it on for a soak, off afterward; matches “debug when needed”.
- Do not duplicate Go/process collectors:
  - ref: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus/collectors#NewGoCollector
  - perf: one MemStats scrape, not two.
  - product: existing Grafana `go_*` dashboards keep working.
- Cgroup v2 `memory.current` / `memory.peak` / `memory.max`:
  - ref: https://docs.kernel.org/admin-guide/cgroup-v2.html#memory-interface-files
  - perf: three small file reads per scrape when enabled; if files missing (non-container), omit series or export NaN/absent — tests cover missing files.
  - product: this is what kube OOM actually sees (includes page cache + CGO), which `process_resident_memory_bytes` understates.
- DuckDB role gauges instead of querying DuckDB memory tables:
  - ref: https://duckdb.org/docs/stable/configuration/pragmas.html#memory_limit (soft cap per instance, not process-wide).
  - perf: atomics only; no extra DuckDB connections.
  - product: answers “how many merge/engine/sql instances are live” which is the oversubscribe hypothesis.
- Job-boundary RSS/heap/cgroup samples hooked into existing lifecycle `ObserveTick` / `observed()`:
  - ref: https://www.brendangregg.com/usemethod.html (USE: utilization during the job, not a random scrape).
  - perf: two snapshots per tick (start/end); ticks are already 15s–60s.
  - product: correlates merge duration histogram we already have with memory.
- GOMEMLIMIT stays process env (Go runtime); expose configured bytes as a gauge when observe on so dashboards can ratio RSS vs cap:
  - ref: https://go.dev/doc/gc-guide#Memory_limit
  - perf: parse env once at start.
  - product: compare heap vs GOMEMLIMIT vs cgroup max on one graph.

## 5. Metrics contract (implement exactly)

When `MEMORY_OBSERVE=true` **and** metrics enabled, `/metrics` MUST include:

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `prism_store_memory_observe` | gauge | — | 1 |
| `prism_store_cgroup_memory_bytes` | gauge | `kind` ∈ `current`,`peak`,`max` | cgroup v2 (skip kind if file missing) |
| `prism_store_gomemlimit_bytes` | gauge | — | parsed `GOMEMLIMIT` (0 if unset/unparseable) |
| `prism_store_duckdb_memory_limit_bytes` | gauge | — | parsed `DUCKDB_MEMORY_LIMIT` (0 if unset/unparseable) |
| `prism_store_duckdb_open` | gauge | `role` closed set | live instances |
| `prism_store_job_rss_bytes` | gauge | `job`,`phase` ∈ `start`,`end` | last sample |
| `prism_store_job_cgroup_current_bytes` | gauge | `job`,`phase` | last sample (omit series if no cgroup) |
| `prism_store_job_heap_alloc_bytes` | gauge | `job`,`phase` | `MemStats.HeapAlloc` last sample |

Also one slog at job **end** when observe on: `msg="memory observe job"`, attrs `job`, `duration_ms`, `rss_bytes`, `heap_alloc_bytes`, `cgroup_current_bytes` (omit cgroup attr if unavailable), `err` if tick failed.

When observe off: `prism_store_memory_observe` **must not appear** (or the whole observe collector unregistered). Other new names must not appear. Existing series unchanged.

`role` closed set (exact strings): `engine`, `merge`, `rollup`, `materialize`, `sql`, `promql`, `loki`, `bounds`, `stat`. Unknown roles must not create a new label value (ignore or map nowhere).

`job` closed set (already defined in `internal/store/lifecycle`): `hot_snapshot`, `flush`, `merge`, `retention`.

### Enablement matrix

| `METRICS_ENABLED` | `MEMORY_OBSERVE` | Behavior |
|---|---|---|
| false | * | `/metrics` 404; observe inert (no scrape, no slog from observe, no cgroup reads). |
| true | false (default) | Existing series only. Zero extra series, zero extra slog, zero cgroup reads, Inc/Dec skipped (bool check, no lock). |
| true | true | Contract series + job-end slog. |

### Wiring

- **Config:** add `Observe bool` (or equivalent) on `metrics.Config`. `cmd/prism-store` sets it from `envBool("MEMORY_OBSERVE", false)` and passes parsed `GOMEMLIMIT` / `DUCKDB_MEMORY_LIMIT` bytes into the registry (parse once at start). Export `parseByteSize` from `internal/config` (or a thin public wrapper) rather than copying the unit table.
- **Collector:** register observe collectors only when `Enabled && Observe`. Scrape-time cgroup reads live in that collector. No background goroutine.
- **DuckDB Inc/Dec:** package-level `metrics.DuckDBOpen(role)` / `metrics.DuckDBClose(role)` (names flexible) gated by an atomic/bool “observe armed” flag set when the observe collector is registered. When the flag is off, return immediately — no lock. Wire at successful open and matching close (including error-path close after a successful open) for:
  - tenant engine (`openTenant` / tenant handle close on LRU eviction and `Engine.Close`) → `engine`
  - merge in-memory connector (`newInMemoryConnector` used by the merge executor, not StatSegment) → `merge`
  - rollup builder (`rollup.NewBuilder` / `Close`) → `rollup`
  - materialize DuckDB open/close → `materialize`
  - `/sql` sandbox (`openSandboxConn`) → `sql`
  - PromQL sandbox (same sandbox helper if shared: pass role; if PromQL opens its own connector, tag `promql`) → `promql`
  - Loki sandbox → `loki`
  - FileBounds bounds DB (`openBoundsDB`) → `bounds`
  - merge `StatSegment` connector → `stat`
- **Lifecycle:** `observed()` takes a **start** sample (RSS, HeapAlloc, cgroup current) before the pass and an **end** sample after. Hook via a new Recorder method (e.g. `ObserveTickStart(job)`) plus existing `ObserveTick`, or an equivalent that still reports start+end gauges. When observe is off, `observed()` must not read `/proc`, cgroup, or `ReadMemStats`. Job-end slog is emitted by the metrics/lifecycle observe path only when observe is on.
- **Cgroup paths:** read cgroup v2 files `memory.current`, `memory.peak`, `memory.max` from the process cgroup (typical `/sys/fs/cgroup/...`). Missing file → omit that `kind` (and omit `job_cgroup_current_bytes` if current is missing). Tests inject a fake dir (do not require a real container).
- **RSS for job samples:** process RSS at the sample instant (e.g. `/proc/self/statm` or equivalent). Do not invent a second `process_resident_memory_bytes` family.
- **Chart:** `metrics.memoryObserve` default `"false"` → env `MEMORY_OBSERVE`. Follow `statefulset.yaml` + golden + values comments. Helm CI golden must be regenerated.

### Tests (TDD)

First commit is **tests only** (`test:`). Hooks run `make lint test`, so that commit must **compile**: stub the types/API the tests need; assertions fail for the right reason (missing series / gauge stuck at 0), not missing symbols.

Unit (package `internal/store/metrics` and/or config):

- Observe off: scrape body must not contain `prism_store_memory_observe`, `prism_store_cgroup_memory_bytes`, `prism_store_gomemlimit_bytes`, `prism_store_duckdb_memory_limit_bytes`, `prism_store_duckdb_open`, `prism_store_job_rss_bytes`, `prism_store_job_cgroup_current_bytes`, `prism_store_job_heap_alloc_bytes`.
- Observe on: all contract names present after a scrape; `prism_store_memory_observe` == 1.
- Fake/open-close increments `prism_store_duckdb_open{role="engine"}` (and at least one other role); close decrements. Off flag does not export the family.
- Cgroup: test double with files present exports `kind=current|peak|max`; missing-files path omits those kinds.
- Byte-size parse: `1638MB` vs `1433MiB` (and unset → 0) for the two limit gauges.
- Job start/end: after `ObserveTickStart` + `ObserveTick`, `job_rss_bytes` and `job_heap_alloc_bytes` exist for that job at both phases.
- slog: capture handler; job-end line only when observe on; omit cgroup attr when unavailable.

E2E (`test/e2e/`, `//go:build e2e`, must run under `make e2e`, docker not required):

- Spawn prism-store (binary or in-process `runStore`) with temp `DATA_DIR`, HTTP ingest of a real parquet window, then `GET /metrics`.
- Observe on → contract series present after ingest; `prism_store_duckdb_open{role="engine"}` ≥ 1.
- Observe off → new families absent.

Existing docker e2e tests keep their skip-without-docker behavior; do not make `make e2e` require docker for the new cases.

### Docs

- `docs/CONFIG.md`: `MEMORY_OBSERVE` bool, default false, independent of `METRICS_ENABLED`, note inert when metrics off.
- `docs/STORE.md`: observability table rows for the new families; mention opt-in; do not claim they always exist.
- Comments atomic (`CONTRIBUTING.md` §3.8): no “see `lifecycle.go`” / “mirrors X” pointers.

## 6. Acceptance checklist  (developer checks these off)

- [x] `MEMORY_OBSERVE` default false; no extra series when false
- [x] Observe on: all series in the contract exist after a scrape
- [x] DuckDB open gauge moves with engine/merge (or sql) open/close
- [x] Lifecycle job start/end RSS (and heap) samples update after a tick
- [x] Cgroup series present in cgroup v2 test double / absent-files path covered
- [x] slog job line only when observe on
- [x] CONFIG.md + STORE.md updated
- [x] Tests written first (`test:` commit before implementation)
- [x] `make lint test` green
- [x] **`make e2e` green** (new e2e included; developer and reviewer both run it)
- [ ] `make full-tests` if wiring/I/O touched (this change does)

## 7. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 8. Reviewer notes

<!-- Reviewer appends one actionable line under any gate it unchecks. Set
     Status: ALL_OK only when every box above is checked; otherwise
     Status: CHANGES_REQUESTED. -->

_(empty until first review)_
