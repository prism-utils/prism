# Spec: prism-store — "run jobs" flag (toggle background maintenance)

Status: READY

- **Slug / branch:** `feat/store-run-jobs`
- **Owner phase:** orchestrator → developer
- **Feature 2 of 3** in the prism-store roles epic (independent; ship as its own PR).

## 1. Task

Add a bootstrap flag/config **`RUN_JOBS`** (default **true**). When set to false,
the store runs **no background maintenance at all** — the entire lifecycle loop
(hot snapshot, flush, merge, rollups, retention) does not run. Ingest and the
query API keep working. Default (jobs on) is the current behavior, unchanged.

## 2. Scope

- **In scope (`cmd/prism-store/main.go`):**
  - Read `RUN_JOBS` (bool env, default **true**) into `serverConfig` (reuse the existing `envBool` helper).
  - When true: start the background loop exactly as today.
  - When false: **do not start** `RunBackgroundLoop` (no tickers created) — none of snapshot/flush/merge/rollup/retention run. Log clearly at startup that background jobs are disabled.
  - Log the effective value on the startup info line.
- **Out of scope:**
  - Per-job granularity (a single all-or-nothing toggle per the resolved design; merge-only is explicitly NOT what we're building).
  - The `mode` feature (separate spec/PR). This flag is orthogonal and composes with it later.
  - Any change to the lifecycle runner internals, engine, ingest, or query.

## 3. Open questions  (resolved before READY)

- [x] Gate only merge, or all background jobs? → **All background maintenance** (snapshot + flush + merge + rollup + retention) — resolved with the requester. A pure no-maintenance mode.
- [x] Default? → **true** (current behavior; opt-out).
- [x] Env name? → **`RUN_JOBS`** (`UPPER_SNAKE`, matching convention).
- [x] Behavioral caveat when false? → hot data will not flush/compact and retention will not delete; that is the operator's explicit choice for a query-only / externally-orchestrated node. Documented, not guarded.

## 4. Decision log  (Decision Protocol)

- **Single all-or-nothing background-jobs toggle, default on.**
  - ref: compute/serving separation — a serving pool that does no background merges, with maintenance handled by a separate role/pool (ObsessionDB on decoupled ClickHouse, "compute-compute separation": https://obsessiondb.com/blog/building-on-decoupled-clickhouse ).
  - perf: a jobs-off node spends zero CPU/IO on compaction/rollup/retention → predictable, low-overhead serving; the write/maintenance role runs elsewhere.
  - product: composes with `QUERY_HOT_ONLY` (feature 1) and the upcoming `mode` (feature 3) to express read vs. maintenance roles; default-on keeps every existing deployment identical.

## 5. Acceptance checklist  (developer checks these off)

- [ ] `serverConfig` gains a `runJobs` bool from `envBool("RUN_JOBS", true)`; startup log includes `run_jobs`.
- [ ] When `RUN_JOBS=false`, `RunBackgroundLoop` is **not** started (no goroutine, no tickers); a startup log line states background jobs are disabled.
- [ ] When unset/true, behavior is byte-for-byte the current behavior (loop runs).
- [ ] Test: with jobs disabled, the server serves `/healthz`/`/readyz` and shuts down cleanly, and NO lifecycle tick runs (e.g. inject a fake/counting runner or assert no snapshot/flush side effects over a short window). Prefer a table/behavior test using the existing `runServe`/loop seams; use a fake clock/runner, not `time.Sleep`-based timing.
- [ ] Test: default (unset) starts the loop (guard the wiring so it can't silently regress).
- [ ] Graceful shutdown still works in both states (no goroutine leak when jobs off; loop stops on ctx cancel when jobs on) — covered under `-race`.
- [ ] Docs updated: `docs/STORE.md` + `main.go` usage comment document `RUN_JOBS` (default true, that false disables ALL background maintenance, and the no-flush/no-retention caveat).
- [ ] `make lint test` (`-race`) green; `go build ./cmd/prism-store` ok; `CGO_ENABLED=0 go build ./cmd/prism` ok; `make tidy` clean; no committed blobs.

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Guidelines:** minimal, env parsed in existing style, no globals, wrapped errors, comments self-contained; default-on verified.
- [ ] **Gate 2 — Edge cases:** jobs off → clean shutdown, no leaked goroutine (goleak or `-race`), server still serves; jobs on → loop still stops on ctx cancel; toggling does not affect ingest/query wiring.
- [ ] **Gate 3 — Docs/comments match code:** documented default + the "disables all maintenance" semantics + the no-flush/no-retention caveat match the code.
- [ ] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [ ] Full `docs/REVIEW.md` checklist; TDD verified via `git log` (test-first).

## 7. Reviewer notes

_(empty until first review)_
