# Spec: benchmark — idle baseline + dense resource time-series charts + minimal resource caps

Status: IN_REVIEW

- **Slug / branch:** `feat/bench-resource-charts`
- **Owner phase:** orchestrator → developer
- **Relates to:** the ClickHouse benchmark (#42) and the resource-usage sampling (PR #45). Follow-up requested by the user.

## 1. Task

The current resource measurement is too coarse and unfair to reason about. Improve it:

1. **Idle baseline.** Measure and report each system's **idle** resource usage (CPU, memory, and I/O) over a quiet window *before* any workload runs, so per-workload numbers are read against a baseline.
2. **Dense time-series + charts.** Replace the handful of per-workload aggregate samples with a **continuous, high-frequency** sample stream across the whole benchmark, and render **charts** (many points) of CPU and memory (and I/O where available) over the run timeline, with phase boundaries annotated. Charts are committed and embedded in the results.
3. **Minimal, equal resource caps.** Configure **both** systems with a **small, equal resource envelope** (CPU + memory) so neither allocates the full host. This makes the comparison fair and reflects the store's minimal-footprint target.

Everything stays reproducible by anyone cloning the repo.

## 2. Scope

### A. Minimal resource caps (both systems, equal budget)
- **Budget:** default **2 vCPU / 1 GiB RAM per system** (constants in the harness, overridable by `prism-bench` flags `--cpus`, `--mem-mib`). Record the applied caps in `results.json`/env and the docs.
- **prism-store (DuckDB):** add optional DuckDB settings to `engine.Config` — `Threads int` and `MemoryLimit string` — applied at connection open via the `go-duckdb` `NewConnector` connInitFn (`SET threads=…`, `SET memory_limit=…`); `0`/`""` preserve current DuckDB defaults (no behavior change when unset). Wire matching env in `cmd/prism-store` (`DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`). The bench uses these for **both** the store **binary** (env) and the **embedded engine** (config), plus `GOMAXPROCS` + `GOMEMLIMIT` on the store binary process.
- **ClickHouse:** add `cpus` + `mem_limit` to the compose service, and mount a `config.d`/`users.d` snippet capping `max_threads` and `max_server_memory_usage`/`max_memory_usage` to the same budget. Values derive from the same constants.
- The store's DuckDB thread/memory cap is a genuine, generally-useful product config (bounded footprint) — keep it minimal, documented, and tested; it must be a **no-op when unset**.

### B. Idle baseline
- After both systems are up + warmed and **before** ingest, run a quiet **idle window** (default ~5s, flag `--idle-seconds`) sampling both targets. Record an `idle` baseline `Usage` per system (CPU mean/peak cores, mean/peak RSS, I/O) into results and mark it on the charts.

### C. Continuous dense sampling + charts
- **Continuous monitor** per target for the entire benchmark: process targets (store binary during ingest; the benchmark process for the embedded-engine queries) sampled at **~20–50 ms**; the ClickHouse container sampled at **~50–100 ms** by **diffing the cumulative `cpu_stats.cpu_usage.total_usage` (and blkio) counters** from the Docker Engine API (real sub-second resolution — do NOT rely on Docker's pre-computed 1s percentage). Each sample is timestamped and **phase-tagged** (`idle`, `ingest`, `count`, `aggregation`, `logs_like`).
- **Raw series** persisted to `bench/results-timeseries.json` (or CSV) for reproducibility (may be downsampled to a sane cap, e.g. ≤ a few thousand points, with full fidelity retained for per-phase aggregates).
- **Charts** rendered with a **pure-Go, no-CGO, SVG-capable** plotting library (bench-only dep; e.g. `gonum.org/v1/plot`). Produce committed SVGs under `bench/charts/`:
  - CPU cores over the timeline (both systems, phase bands + idle region),
  - memory RSS over the timeline (both systems),
  - I/O where available (may be Linux-only / container-only — omit cleanly if empty).
  Embed the charts in `README.md` (benchmark section) and `bench/RESULTS.md`. SVG chosen for git-diffable, GitHub-renderable output (no binary blob churn).
- Per-phase aggregate tables (from #45) stay, now computed from the dense series, and now include the **idle baseline row**.

### D. Docs
- Update `bench/README.md` + `README.md`: the caps (and why — minimal equal envelope, no full-host allocation), the idle-baseline method, the sampling resolutions and why container CPU is derived by counter-diffing, the chart artifacts, and the honest caveats retained from #45 (embedded-engine sampling; per-process IOPS Linux-only; Docker Desktop blkio may read 0 → Linux host for real container I/O).

### Out of scope
- Containerizing prism-store (queries are embedded by design); Prometheus/node_exporter; changing workloads or fairness methodology from #42/#45 beyond adding the caps + baseline.

## 3. Open questions  (resolved before READY)

- [x] How to cap the store fairly without containerizing it? → **DuckDB `threads` + `memory_limit`** via `engine.Config`/`NewConnector` connInitFn (dominant CPU/mem knob), applied to binary + embedded engine; `GOMAXPROCS`/`GOMEMLIMIT` on the binary. No-op when unset.
- [x] How to cap ClickHouse? → compose `cpus`+`mem_limit` + `config.d` `max_threads`/memory. Same budget.
- [x] Budget value? → **2 vCPU / 1 GiB** default, flag-overridable, recorded in results.
- [x] Dense container CPU without the 1s Docker percentage limit? → poll the Docker API and **diff the cumulative `total_usage` nanosecond counter** ourselves at our interval.
- [x] Chart format? → **SVG** via a pure-Go lib (git-diffable, GitHub-renders, no CGO). Raw series also emitted as JSON/CSV.
- [x] New deps? → the plot lib + (already present) `gopsutil`, all **bench-only**; must not enter the `cmd/prism` static build. Engine change adds no new dep (uses existing `go-duckdb`).

## 4. Decision log  (Decision Protocol)

- **Cap DuckDB via `threads`/`memory_limit` rather than containerizing the store.**
  - ref: go-duckdb `NewConnector(dsn, connInitFn)` (`duckdb@v1.8.5`); DuckDB config `threads`/`memory_limit` — https://duckdb.org/docs/configuration/overview .
  - perf: `threads` directly bounds DuckDB's scan parallelism (the store's dominant CPU user); product: a bounded-footprint knob operators want anyway; no-op when unset so shipping behavior is unchanged.
- **Cap ClickHouse via compose limits + `max_threads`/memory to the same budget.**
  - ref: Compose `cpus`/`mem_limit`; ClickHouse `max_threads`/`max_server_memory_usage` — https://clickhouse.com/docs/en/operations/settings/settings .
  - perf/product: equal, minimal envelope → fair comparison, no full-host allocation.
- **Dense CPU by diffing the cumulative Docker `total_usage` counter.**
  - ref: Docker Engine API container `stats` (`cpu_stats.cpu_usage.total_usage` cumulative ns) — https://docs.docker.com/reference/api/engine/ .
  - perf: gives true sub-second resolution vs the streamed 1s percentage; product: charts with many real points.
- **SVG charts via a pure-Go plotter.**
  - ref: `gonum.org/v1/plot` (SVG via `vg/vgsvg`, pure Go).
  - perf: n/a; product: git-diffable, GitHub-renderable, reproducible, no CGO / no external tools.

## 5. Acceptance checklist  (developer checks these off)

- [x] `engine.Config` gains `Threads`/`MemoryLimit`, applied at DuckDB open; **no-op when unset** (existing engine tests unchanged); a new test asserts the settings take effect (e.g. `SELECT current_setting('threads')` == configured) and that unset preserves defaults. `cmd/prism-store` reads `DUCKDB_THREADS`/`DUCKDB_MEMORY_LIMIT`.
- [x] Compose caps ClickHouse (`cpus`+`mem_limit`) and a mounted `config.d` caps `max_threads`/memory to the same budget; the store binary + embedded engine are capped to the same budget; caps recorded in `results.json` env.
- [x] Idle baseline sampled before workloads for both systems and rendered as a baseline row + chart region.
- [x] Continuous phase-tagged sampling across the whole run: process targets ~20–50 ms; container by counter-diffing the Docker API ~50–100 ms; raw series written to `bench/results-timeseries.*`.
- [x] Charts (`bench/charts/*.svg`) for CPU + memory (+ I/O where non-empty) with phase bands + idle region, embedded in `README.md` and `bench/RESULTS.md`; per-phase aggregate tables retained and recomputed from the dense series.
- [x] `bench/README.md` + `README.md` document caps, baseline, sampling resolutions + counter-diff rationale, chart artifacts, and the retained caveats.
- [x] `make bench` re-run on this host; all artifacts (json, timeseries, RESULTS.md, charts, README) regenerated from the SAME run; correctness gates (metrics count-equality, LIKE-equality) still pass.
- [x] Bench-only deps: `CGO_ENABLED=0 go build ./cmd/prism` passes and imports NEITHER the plot lib NOR gopsutil (verify import graph); `go build ./cmd/prism-store` passes; `make tidy` clean.
- [x] `make lint test` (`-race`) green; sampler + chart-writer unit-tested (dense-series aggregation, idle window, chart file written & non-empty/well-formed SVG, counter-diff CPU math); goroutines stop cleanly (no leaks); no committed datasets (charts + small results artifacts are the intended committed outputs).

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Guidelines:** engine config change minimal + no-op when unset + wrapped errors; samplers/chart writer behind small interfaces, no globals, ctx-bound goroutine lifecycle; Docker counter-diff handles first-sample/rollover; comments self-contained.
- [ ] **Gate 2 — Edge cases:** unset DuckDB settings = no behavior change; caps actually applied (assert, don't assume); idle window with near-zero activity; a phase shorter than the sample interval still yields ≥1 sample; container counter first-sample (no prior) handled; empty I/O series → chart omitted, not broken; Docker socket absent → documented fallback (may disable dense container series with a clear note); sampling overhead does not materially move latencies vs #42/#45.
- [ ] **Gate 3 — Docs/comments match code:** caps/budget, baseline, resolutions, chart list and file paths match what the code emits; embedded charts render; committed numbers/charts came from one real `make bench` run; caveats accurate.
- [ ] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [ ] **Honesty/attribution audit:** correct target per phase; idle baseline genuinely idle; container CPU derived from counter deltas (not the misleading streamed %); caps disclosed; no fabricated metric when unavailable (`n/a` preserved).
- [ ] Full docs/REVIEW.md checklist; TESTING.md layering (unit tests for engine settings, aggregation, counter-diff, chart writer; full bench opt-in). Verify the agent static build excludes bench-only deps.

## 7. Reviewer notes

_(empty until first review)_
