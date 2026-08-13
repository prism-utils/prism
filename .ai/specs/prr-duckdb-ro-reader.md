# Spec: fix(store) — RO reader skip writable ensure (DuckDB engine spam)

Status: ALL_OK
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/prr-duckdb-ro-reader-8a90`
- **Owner phase:** orchestrator
- **Issue:** prism-utils/prism#125 (PRR / Homelab prod)
- **PLAN phase(s):** store reader/writer split (post Phase store-run-jobs + sql_readonly_replica)

## 1. Task

On Homelab prod, the shared `prism-proxy` query plane mounts tenant data
read-only (`dataReadOnly: true`, `RUN_JOBS=false`). Site-main's reconciler still
POSTs `/admin/tenants/{ns}/ensure` on a short interval. Ensure today always calls
`eng.DB(ns)`, which `MkdirAll`s and opens a writable `engine.duckdb` under
`/data/<ns>/`. That fails with `Read-only file system` and floods ERROR logs
(`ensure tenant engine`) every ~5 minutes per tenant. Fix the store so a
jobs-off / RO replica treats ensure as a no-op success and never opens or seeds
writable DuckDB on the tenant data mount, while writers (`RUN_JOBS=true`) keep
today's ensure behavior.

## 2. Scope

- **In scope:**
  - `admin.Config`: add `RunJobs bool` (mirrors process-wide `RUN_JOBS`).
  - `admin.EnsureHandler`: when `!cfg.RunJobs`, after tenant-id validation only,
    return **204** without calling `eng.DB`, seed, or tiered-layout writers.
    Optional single debug/info log once-per-call is OK; **no ERROR**.
  - Wire `RunJobs: cfg.runJobs` in `serverConfig.adminConfig()` /
    `newServeMux` (same source as query/PromQL).
  - Tests: RO/jobs-off ensure returns 204 on a chmod-555 (or otherwise
    non-writable) data dir and never creates `engine.duckdb`; jobs-on still
    ensures and still 500s when the dir is not writable.
  - Docs: `docs/STORE.md` (ensure + reader/writer split) note that
    `RUN_JOBS=false` makes `/admin/tenants/{ns}/ensure` a no-op 204.
- **Out of scope:**
  - Homelab chart flips to RW; separate emptyDir engine mount; softening ERROR
    to WARN while still returning 500; changing site-main reconciler cadence;
  - Opening DuckDB with `access_mode=read_only` for ensure (ensure must not
    touch the engine file at all on the replica);
  - Changing `/readyz` MkdirAll behavior (separate concern).

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Gate on `RUN_JOBS=false` vs a new `DATA_READ_ONLY` env? — A: **Reuse
  `RUN_JOBS`**. Homelab already sets `runJobs: false` on the shared RO plane;
  query/PromQL already key RO replica semantics off `RunJobs`. A second flag
  would drift from the existing reader/writer contract.
- [x] Q: No-op 204 vs 200 body vs keep 500+WARN? — A: **204 no-op**. Reconciler
  treats non-2xx as failure (best-effort warn today); returning 204 clears the
  ERROR spam and matches writer ensure's success status without inventing a
  new response shape.
- [x] Q: Should jobs-off ensure still create seeds if the mount happens to be
  writable? — A: **No.** Writers own layout; the replica must not write tenant
  paths even if the FS is accidentally RW.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Gate ensure writes behind `RUN_JOBS=false` (no-op 204); do not open engine.**
  - ref: DuckDB concurrency — a process that must not write uses
    `access_mode=read_only` / does not take the writer lock; RO replicas never
    create the DB file
    (https://duckdb.org/docs/stable/connect/concurrency.html,
    https://github.com/duckdb/duckdb-go#readme `access_mode=read_only`).
    Also matches PostgreSQL hot-standby posture: replicas serve reads and do
    not run primary bootstrap/DDL
    (https://www.postgresql.org/docs/current/hot-standby.html).
  - perf: eliminates repeated failed `MkdirAll` + DuckDB open attempts and
    ERROR log volume on the shared plane (CPU + I/O + log pipeline noise);
    zero allocations for engine open on the ensure hot path when jobs are off.
  - product: preserves the Homelab reader/writer safety model
    (`dataReadOnly: true` + tenant `prism-cache` writers). Site-main already
    best-efforts ensure; 204 makes the contract honest instead of papering
    over 500s.

## 5. Acceptance checklist  (developer checks these off)

- [x] `admin.Config` includes `RunJobs bool`; `adminConfig()` / mux wiring sets
      it from `serverConfig.runJobs`.
- [x] `EnsureHandler` with `RunJobs=false`: valid tenant → **204**, no
      `eng.DB`, no seed/tier writes, no ERROR log for ensure engine.
- [x] `EnsureHandler` with `RunJobs=true` (default): unchanged — opens engine,
      seeds layout; non-writable data dir still **500** + ERROR.
- [x] Tests written first (a `test:` commit precedes implementation) —
      CONTRIBUTING.md §1
- [x] Unit tests cover: jobs-off + non-writable dir → 204 and no
      `engine.duckdb`; jobs-on + non-writable → 500; jobs-on happy path still
      204 + seed present; unknown tenant still 404 in both modes.
- [x] `docs/STORE.md` documents ensure no-op under `RUN_JOBS=false`.
- [x] `make lint test` green locally (+ `make full-tests` if I/O wiring needs it;
      this change is admin HTTP + engine open gate → at least `make lint test`).

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [x] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [x] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

**2026-08-13 — ALL_OK.** TDD order confirmed (`0305af7` test → `662307d` fix).
`make lint test` re-run green. Ensure no-op gated on `RunJobs` only; writer path
unchanged (500 on non-writable). STORE.md ensure table + reader/writer feature
row match code. Comments self-contained (no cross-file refs). Wiring covered by
`TestAdminConfigWiresRunJobs`.