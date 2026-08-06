# Spec: Retention fail-safe + quiet ingest logs (#95)

Status: IN_REVIEW

- **Slug / branch:** `fix/retention-failsafe`
- **Owner phase:** orchestrator
- **PLAN phase(s):** Store lifecycle / ops hardening
- **Issue:** https://github.com/elk-utilities/prism/issues/95
- **Target release:** next patch after merge (with quiet-ingest change in the same version)

## 1. Task

Fix prod retention abort (#95): empty/corrupt rollups make `StatRollupMaxBucket`
scan `NULL` into `*time.Time`, `TickRetention` returns early, and `MAX_LOG_FILES`
never runs — so one bad tenant/file blocks pruning for everyone. Make lifecycle
**fail-safe** (per-tenant / per-file continue on error). Keep retention
configurable by days (`RETENTION_DAYS`) and merge knobs (`MAX_SEGMENT_BYTES`,
`SEGMENTS_PER_TIER`) with force-merge **ignoring already-sealed** segments
(metrics already does; align logs landing). Demote per-window ingest success
logs (HTTP + Flight) from Info→Debug so default collection of store stdout
cannot feedback-loop.

## 2. Scope

- **In scope:**
  - `internal/store/rollup` — NULL/empty `max(bucket)` handling
  - `internal/store/lifecycle` — fail-safe ticks; empty-rollup delete/skip
  - `internal/store/engine` — fail-safe flush + hot-snapshot tenant loops
  - `internal/store/merge` — logs landing force-merge ignores sealed
    (`Bytes >= MAX_SEGMENT_BYTES`); shrink when sum exceeds max (like metrics)
  - `internal/store/ingest` — demote success Info→Debug (HTTP + Flight)
  - `cmd/prism-store` — inject logger into lifecycle runner for per-tenant errors
  - Docs: `STORE.md` / `CONFIG.md` — fail-safe behavior; confirm knobs;
    note ingest success is Debug
  - Regression tests for #95 + fail-safe + sealed-ignored log merge + log level
- **Out of scope:**
  - Publishing a new CGO agent image
  - Changing default slog filter to Warn (keep default Info; Debug off)
  - Per-tenant CPU quotas / fair scheduling beyond continue-on-error
  - New retention knobs beyond existing `RETENTION_DAYS` / `MAX_LOG_FILES` /
    `SEGMENTS_PER_TIER` / `MAX_SEGMENT_BYTES` (document + make correct)

## 3. Open questions

- [x] Q: Fail-safe granularity? — A: Per-tenant for flush/merge/retention/hot
  snapshot; per-file for rollup retention. Log Error with `tenant` (and path
  when file-scoped) and continue. Catastrophic `listTenants` / data-dir errors
  may still fail the tick.
- [x] Q: Empty rollup on NULL max(bucket)? — A: Treat as no usable bucket —
  **delete** the empty/corrupt rollup file (issue preference); do not abort.
- [x] Q: Retention days / segment knobs new env? — A: No new env names.
  `RETENTION_DAYS` already applies to metrics tiers, rollups, and log age;
  `MAX_LOG_FILES` remains the log file-cap. `SEGMENTS_PER_TIER` =
  force-merge count; `MAX_SEGMENT_BYTES` = seal size. Fix logs landing so
  sealed segments are excluded from the force-merge count (parity with metrics).
- [x] Q: Default log level? — A: Keep process default Info. Demote high-volume
  success ingest lines to Debug (disabled unless operators raise level). Keep
  startup/shutdown/config Info.

## 4. Decision log

- Fail-safe continue-on-tenant-error for lifecycle ticks:
  - ref: https://learn.microsoft.com/en-us/azure/architecture/antipatterns/noisy-neighbor/noisy-neighbor — isolate tenant failures so one tenant cannot starve shared background work
  - perf: one bad tenant costs a log line + skip; other tenants still prune/merge
  - product: matches #95 impact (26k files because one NULL rollup aborted the tick)

- NULL/empty rollup → delete, do not fail the tick:
  - ref: https://github.com/elk-utilities/prism/issues/95 — prefer delete empty rollups
  - perf: avoids opening poison files every hour; reclaim junk bytes
  - product: retention must make forward progress under corrupt/empty artifacts

- Logs landing force-merge ignores sealed; shrink like metrics planner:
  - ref: Lucene TieredMergePolicy sealed-segment exclusion (mirrored in
    `docs/STORE.md` seal rules + existing metrics `FindMerges`)
  - perf: prevents sealed giants from blocking tiny-file compaction
  - product: operators already configure `MAX_SEGMENT_BYTES` /
    `SEGMENTS_PER_TIER`; behavior must match documented seal semantics for logs

- Ingest success Info → Debug:
  - ref: https://aws-observability.github.io/observability-best-practices/signals/logs/ — filter high-volume low-value logs at source; Info for meaningful state
  - perf: stops log→collect→ingest amplification when agents tail store stdout
  - product: useful Info stays (listen/start/stop/config); routine success is Debug

## 5. Acceptance checklist  (developer checks these off)

- [x] `StatRollupMaxBucket` (or retention caller) treats NULL/empty max(bucket)
      without Scan error; empty/unusable rollups are deleted (or skipped without
      failing the tick)
- [x] `TickRetention` continues after per-tenant / per-file errors so
      `retainLogsTenant` / `MAX_LOG_FILES` still run for healthy work
- [x] `TickMerge`, `FlushDue`, `ExportHotSnapshots` continue per-tenant on error
      (log + continue; logger wired into runner/engine as needed)
- [x] Regression: empty rollup parquet must **not** block `MAX_LOG_FILES`
      enforcement for another tenant (or same tenant logs after rollup step)
- [x] Logs landing force-merge: segments with `Bytes >= MAX_SEGMENT_BYTES` are
      excluded from the count/inputs; candidate sets shrink when sum would exceed
      max (parity with metrics)
- [x] Docs confirm `RETENTION_DAYS`, `MAX_SEGMENT_BYTES`, `SEGMENTS_PER_TIER`,
      `MAX_LOG_FILES` behavior including fail-safe ticks
- [x] HTTP + Flight ingest success logs are `Debug` (not `Info`)
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
