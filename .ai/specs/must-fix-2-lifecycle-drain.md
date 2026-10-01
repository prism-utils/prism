# Spec: must-fix-2 lifecycle drain

Status: READY

- **Slug / branch:** `cursor/must-fix-2-lifecycle-b991`
- **Owner phase:** developer
- **PLAN phase(s):** store lifecycle
- **Issues:** #184 #185 #186 #187 #188 (epic #183)

## 1. Task

Prod tenant `user-fqsejat4-apps` (~51 GiB) never drains: orphan `hot/*.tmp` and DuckDB spill, RAM-only flush schedule, retention that ignores hot/engine, fail-closed `ScanTier`, and snapshot export holding `te.mu` for the whole COPY. Implement the five child issues in order. Designs were settled with a design sub-agent; do not reopen them.

## 2. Scope

Work only in `/home/masoas/workdir/cursor-must-fix-2-lifecycle-b991/prism`.

- **In scope:** `internal/store/gc` (new), `layout` scratch predicates, `lifecycle` scratch tick + boot + `RetainHot` call, `engine` flush reconstruct + snapshot second connection + `RetainHot` + `HasOpen`, `merge.ScanTier` fail-open + `UnreadableExpired`, `cmd/prism-store` boot scratch GC when `RUN_JOBS=true`, `docs/STORE.md` + `docs/CONFIG.md` `RETENTION_DAYS` one-liner.
- **Out of scope:** new env vars; unlink live `engine.duckdb` / `.wal`; `SET temp_directory`; logs scan fail-open; Grafana/billing; #162 promote; changing `HOT_WINDOW_*` defaults; `lsof`/fd heuristics; comments that name other files/functions.

Implement **one slice at a time**. Each slice: `test:` commit first, then implementation commit(s). Order: #184 → #187 → #185 → #188 → #186.

## 3. Open questions

- [x] Q: GC package? — A: new leaf `internal/store/gc`; do not extend `promote.GCTenant`.
- [x] Q: Flush persistence? — A: reconstruct `due = MIN(hot_current.ts)+HotWindow`; no sidecar file.
- [x] Q: Snapshot vs flush lock? — A: second `sql.DB` on the same go-duckdb Connector; do not hold `te.mu` for COPY/ATTACH.
- [x] Q: Unreadable files? — A: omit in `ScanTier`; mtime-delete only on retention tick via `UnreadableExpired`.

## 4. Decision log

- **Leaf `internal/store/gc` + layout predicates, not promote/engine-owned pin GC**
  - ref: https://duckdb.org/docs/stable/operations_manual/footprint_of_duckdb/files_created_by_duckdb — spill is `⟨database⟩.tmp/`; WAL is `⟨database⟩.wal`.
  - perf: one `ReadDir(hot/)` + one lstat of `engine.duckdb.tmp` per tenant per hour and once at boot.
  - product: crash reclaim without expanding promote or teaching engine about pin names.

- **Deadline = `min(hot_current.ts) + HotWindow`, not a sidecar**
  - ref: https://duckdb.org/docs/current/connect/overview — path-backed DuckDB reopens with the same tables.
  - perf: one `MIN(ts)` on first open / FlushDue miss; `flushAt` caches it.
  - product: existing 3.1 GiB catalogs drain without a backfill file.

- **Fail-open inside `ScanTier`, mtime-delete only on retention**
  - ref: https://github.com/prometheus/prometheus/issues/17833 — one corrupt block stops compaction and retention never drains.
  - perf: no extra DuckDB opens on omit; extra `ReadDir` only on the hourly retention tick.
  - product: matches STORE.md per-file skip already true for rollups.

- **Safety-net DELETE in TickRetention; do not flush from that tick**
  - ref: https://prometheus.io/docs/prometheus/latest/storage/ — delete only fully expired data; mixed L0 would keep expired rows.
  - perf: DROP/DELETE in place vs 2× disk to flush then delete.
  - product: zero-ingest tenants drain even if flush has not run.

- **Second connection for snapshot; writer pool stays 1**
  - ref: https://github.com/marcboeker/go-duckdb#connector — two `sql.DB` on one Connector.
  - ref: https://duckdb.org/docs/current/connect/concurrency.html — in-process MVCC; do not open a second process on the file.
  - perf: +1 connection per LRU tenant; flush no longer queued behind a 10-minute COPY.
  - product: 15s snapshot and 30s flush tickers become independent.

## 5. Acceptance checklist

### Slice #184 GC

- [x] `layout.IsHotScratch(name)` / `layout.IsEngineScratch(name)` allowlist only (see issue #184 / design). Never glob all `*.tmp`.
- [x] `gc.HotDir`, `gc.EngineSpill`, `gc.Tenant` with injected `now` and grace. Missing/empty dirs → nil.
- [x] Delete iff `ModTime().Before(now.Add(-grace))`. Equal-to-grace: keep.
- [x] Grace: use `DeleteGrace` but **floor at 120s** when DeleteGrace is 0.
- [x] `EngineSpill` skip when `HasOpen(tenant)` (do not smash live DuckDB spill).
- [x] `TickRetention` calls scratch GC first; per-tenant errors logged and skipped; tick returns nil.
- [x] Boot: when `RUN_JOBS=true`, call scratch GC once after `NewRunner` before the background loop. `RUN_JOBS=false` does not.
- [x] `promote.GCTenant` unchanged.
- [x] Tests (fail on current main): `TestHotDirGCRemovesOrphanSnapshotTmpAndWal`, `TestHotDirGCLeavesInFlightTmp`, `TestHotDirGCRemovesStaleReadPins`, `TestEngineSpillGCRemovesOrphanDuckDBTmp`, `TestPromoteGCStillRemovesPromoteTmp` (existing promote GC), `TestGCContinuesAfterTenantError`. `t.TempDir()`, frozen clock, `os.Chtimes`, no Sleep.

### Slice #187 ScanTier

- [x] `ScanTier`: on `StatSegment` error, `slog.Error("stat segment", "path", path, "err", err)` and continue. `ReadDir` errors other than IsNotExist still return.
- [x] `UnreadableExpired` listing complement used only from `tickRetention` after a successful `ScanAllTiersRoots`. Delete `isSegmentFile` names not in live scan whose mtime is strictly before cutoff. Skip CompactedSet. Do not StatSegment again.
- [x] Tests: `TestScanTierOmitsUnreadableSegment`, `TestScanTierEmptyDir`, keep `TestScanTierSkipsRetiredSegments`; `TestTickRetentionContinuesAfterUnreadableSegment` (fresh-mtime garbage remains, expired good L0 gone); `TestTickRetentionDeletesUnreadableOlderThanRetention`; `TestTickMergeContinuesAfterUnreadableSegment`.

### Slice #185 durable flush

- [ ] Reconstruct `flushAt` from `MIN(hot_current.ts)+HotWindow` when missing. Empty/missing table does not arm.
- [ ] `open` arms after `ensureHotCurrent`.
- [ ] `FlushDue` lists on-disk tenants (`listDataTenants`), arms if missing, flushes due tenants. Skip dirs with no `engine.duckdb`.
- [ ] Ingest / IngestDuckDB: **open first**, then `maybeFlushDue`, then insert, then `scheduleFlush`.
- [ ] `scheduleFlush` still once-if-missing. Light ingest must not reset an overdue reconstructed deadline.
- [ ] Tests: `TestFlushDueWithoutIngestAfterHotWindow`, `TestFlushScheduleSurvivesEngineRestart` (shared DataDir, Close, New, FlushDue), `TestFlushScheduleArmedFromExistingHotRowsOnOpen`, `TestEmptyHotDoesNotArmFlush`, keep `TestFlushAfterHotWindowCreatesOneL0SegmentSortedByTs`. No schedule filename asserts.

### Slice #188 snapshot vs flush

- [ ] Keep `te.db` `MaxOpenConns(1)`. Store Connector on `tenantEntry`; `te.snap = sql.OpenDB(connector)` MaxOpenConns(1). Close snap with the writer.
- [ ] `exportHotSnapshot` runs COPY/ATTACH on `te.snap` **without** holding `te.mu` for that I/O. Keep `exportGroup.Do`.
- [ ] Unexported `exportBarrier func()` (nil in prod) invoked after any lock release, before COPY/ATTACH. Tests only.
- [ ] Tests: `TestHotSnapshotDoesNotBlockFlushDue` (channel barrier, FlushDue returns before release, L0 exists, goleak, -race); `TestOverlappingSnapshotsStillSingleExport`; keep existing flush/snapshot failsafe tests. No Sleep.

### Slice #186 hot retention

- [ ] `Engine.RetainHot(tenant, cutoff)`: exclusive lock; DROP or DELETE expired rows in `hot_current` and `hot_prev` (`ts` strictly before cutoff); CHECKPOINT; never unlink `engine.duckdb`. If no remaining rows, unlink published `hot/current.{parquet,duckdb}` (+ wal). If mixed delete happened, `ExportHotSnapshot`.
- [ ] `tickRetention` always calls `RetainHot` per tenant (even if tier scan failed); log and continue.
- [ ] Tests: `TestTickRetentionDeletesExpiredHotSnapshotAndEngineRows`, `TestTickRetentionKeepsHotSnapshotInsideWindow` (inside + equal-cutoff keep), `TestTickRetentionZeroIngestStillDeletes` (INSERT via engine DB, never Ingest after runner construction), keep logs file-cap tests and L0 age deletion.

### Cross-cutting

- [ ] Tests written first (a `test:` commit precedes implementation) per slice — CONTRIBUTING.md §1
- [ ] Atomic comments only — CONTRIBUTING.md §3.8
- [ ] `docs/STORE.md` Hot window, Hot snapshot, Retention, Lifecycle table match the three epic invariants
- [ ] `docs/CONFIG.md` `RETENTION_DAYS` names hot snapshot + engine rows
- [ ] `make lint test` green; `make full-tests` green (I/O + engine wiring)

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
