# Spec: lifecycle holes — oversized L0 convert, sidecar unlink, materialize tmp GC

Status: IN_REVIEW

- **Slug / branch:** `cursor/lifecycle-holes-b991`
- **Owner phase:** reviewer
- **PLAN phase(s):** store lifecycle / cold promote
- **Worktree:** `/home/masoas/workdir/cursor-lifecycle-holes-b991/prism`

## 1. Task

Three leftover lifecycle holes after v1.0.25 drain:

1. **Item 1 (most critical):** a sealed L0 DuckDB at or above `MAX_SEGMENT_BYTES` is skipped by merge and skipped by promote (`if duckdb && tier < 1 { continue }`), so it sits on SSD forever (prod ~3.3 GiB leftover). Convert it to cold parquet when `Eligible` — do **not** merge it, do **not** raise the cap, do **not** rename L0→L1.
4. **Item 4:** `.merge-skip` / `.merge-attempts` sidecars stay after the segment is unlinked.
5. **Item 5:** crash leftover `materializations/<name>/<dest>.parquet.tmp` is not in scratch GC.

Do not break existing functionality, especially item 1: undersized Eligible L0 DuckDB must still stay on hot and merge to L1; only oversized sealed L0 DuckDB converts.

## 2. Scope

Work only in this worktree.

- **In scope:**
  - Item 1: `internal/store/promote` convert-on-promote for **oversized** Eligible L0 `.duckdb` (metrics + logs); `promote.Config.MaxSegmentBytes`; `lifecycle.promoteConfig` passes `r.cfg.MaxSegmentBytes`; `docs/STORE.md` line that currently says L0 duckdb never leaves.
  - Item 4: unlink `.merge-skip` + `.merge-attempts` with the segment from segment-remove paths (`lifecycle.removePath`, `merge.retireSources` immediate delete, `merge.removeIfPresent`, promote source unlink). Helper in `layout`.
  - Item 5: `layout.IsMaterializeScratch` + `gc.Materializations` walk of `materializations/<name>/` with the same 2m grace/mtime floor as `HotDir`; `gc.Tenant` calls it.
- **Out of scope:** merging files with `Bytes >= MaxSegmentBytes`; raising `MAX_SEGMENT_BYTES`; splitting `FlushDue`; renaming L0→L1; converting undersized L0 DuckDB; converting L0 DuckDB that is not `Eligible`; globbing all `*.tmp` anywhere; orphan-sidecar GC without the segment; new env vars; Helm/charts; changing `COLD_AFTER` / `HOT_WINDOW_*` / `RETENTION_DAYS`; comments that name other files/functions.

Implement **one slice at a time**. Each slice: `test:` commit first, then implementation. Order: item 1 → item 4 → item 5.

## 3. Open questions

- [x] Q: Convert all Eligible L0 DuckDB, or only oversized? — A: **only** `Bytes >= MaxSegmentBytes`. Undersized L0 still merges to L1 (existing `TestTenantNeverPromotesL0DuckDB` must keep passing).
- [x] Q: Merge oversized instead of convert? — A: **no**. Planner already skips `s.Bytes >= MaxSegmentBytes`; do not change that.
- [x] Q: `MaxSegmentBytes <= 0` on promote.Config? — A: skip L0 duckdb convert (today's behavior) so tests that omit the field stay green. Lifecycle production config always sets it.
- [x] Q: Oversized fixture size? — A: tests set a tiny `MaxSegmentBytes` (e.g. 1) against a normal one-row DuckDB; do not write multi-GiB files.
- [x] Q: Sidecar unlink when `HoldSource` holds the file? — A: leave sidecars until the segment is actually unlinked (grace expire / immediate delete).
- [x] Q: Glob all `*.tmp`? — A: **no**. Materialize scratch is allowlisted names under `materializations/<name>/` only.

## 4. Decision log

- **Convert-on-promote for sealed oversized L0 DuckDB (choice B), not merge, not cap raise, not L0→L1 rename**
  - ref: https://duckdb.org/docs/lts/sql/statements/copy.html — `COPY … TO` parquet is the same convert already used for L1+.
  - perf: one convert per sealed leftover, same as L1+; no 2× merge rewrite of a 3 GiB file that cannot shrink under the cap.
  - product: leftover L0 still ATTACHes on SSD until `COLD_AFTER`; then it becomes queryable cold parquet. Undersized L0 still compact. Merge of oversized stays forbidden.

- **Sidecars leave with the segment, not a separate glob walk**
  - ref: https://prometheus.io/docs/prometheus/latest/storage/ — deleting a block removes its directory (chunks + meta together).
  - perf: two extra `unlink` of missing-ok paths on an already-deleting segment; no extra `ReadDir`.
  - product: skip/attempt markers cannot outlive the file they name.

- **Materialize tmp GC is an allowlist walk of `materializations/<name>/`, same grace as hot scratch**
  - ref: https://duckdb.org/docs/stable/sql/statements/copy.html — `COPY TO` writes a dest file that is renamed; a crash leaves the dest.tmp.
  - perf: one `ReadDir(materializations)` plus one `ReadDir` per named item per retention/boot GC; no tenant-wide glob.
  - product: in-flight COPY is protected by the 2m mtime floor; live parquet is never scratch.

## 5. Acceptance checklist  (developer checks these off)

### Slice item 1 — oversized L0 convert (do this first; do not break undersized L0)

- [x] `promote.Config` grows `MaxSegmentBytes int64`. `<= 0` means "do not convert L0 duckdb" (existing skip).
- [x] `listHotCompacted` / `fileRef` already `Stat`s; carry `Bytes` so the skip can compare size.
- [x] Keep skipping L0 `.duckdb` when `Bytes < MaxSegmentBytes` **or** `MaxSegmentBytes <= 0`. `TestTenantNeverPromotesL0DuckDB` must still pass unchanged (small file, no / large cap).
- [x] When Eligible **and** `.duckdb` **and** tier 0 **and** `Bytes >= MaxSegmentBytes > 0`: convert via the existing L1+ convert path (`recoverOrConvert` → cold `.parquet`). Do not byte-copy `.duckdb` onto cold. Convert failure leaves the hot source; dest unpublished.
- [x] Undersized Eligible L0 duckdb still stays on hot (merge path unchanged). Ineligible oversized L0 duckdb (max_ts too new) stays on hot.
- [x] `FindMerges` / logs pack still skip `Bytes >= MaxSegmentBytes`. Do not merge oversized in this change.
- [x] `lifecycle.promoteConfig` sets `MaxSegmentBytes: r.cfg.MaxSegmentBytes`.
- [x] Logs L0 oversized duckdb uses the same rule (already listed by `listHotCompacted`).
- [x] `docs/STORE.md`: replace "L0 `.duckdb` never leaves" with: leftover L0 parquet stays eligible; **undersized** L0 duckdb stays on `DATA_DIR` for merge; **oversized** (`>= MAX_SEGMENT_BYTES`) L0 duckdb converts to cold parquet when Eligible.
- [x] Tests (fail on current main for the new ones): `TestTenantConvertsEligibleOversizedL0DuckDB` (tiny cap, Eligible, dest parquet, hot unlinked, no cold duckdb); `TestTenantDoesNotConvertIneligibleOversizedL0DuckDB`; keep `TestTenantNeverPromotesL0DuckDB`; keep L1 convert tests. Frozen clock, `t.TempDir()`, no Sleep, no multi-GiB files.

### Slice item 4 — sidecar unlink

- [x] `layout.RemoveSidecars(segmentPath)` unlinks `MergeSkipMarker` and `MergeAttemptsMarker`; `IsNotExist` is success.
- [x] Call it when a **segment file** is actually unlinked: `lifecycle.removePath`, `merge.retireSources` (grace <= 0), `merge.removeIfPresent` (grace expire / compacted purge), promote source `os.Remove(f.Path)`. Do not call it when `HoldSource` keeps the file.
- [x] Missing sidecars are not an error. Do not glob. Do not add orphan-sidecar GC.
- [x] Tests: retention/removePath deletes segment **and** both sidecars; grace-expire / `removeIfPresent` deletes sidecars; a segment without sidecars still deletes; held source keeps sidecars until unlink.

### Slice item 5 — materialize scratch GC

- [x] `layout.IsMaterializeScratch(name)` true only for `*.parquet.tmp` and `*.duckdb.tmp` (the `final+".tmp"` dest from materialize COPY). Live `*.parquet` / `*.duckdb`, hot snapshot tmps, `orphan.tmp`, promote temps are false.
- [x] `gc.Materializations(dataDir, tenant, now, grace)` walks `materializations/<name>/` only. Missing/empty root → nil. Same `stale` / 2m floor as `HotDir`.
- [x] `gc.Tenant` also runs `Materializations`. Fresh tmp (mtime within / equal grace) stays. Live parquet stays. Foreign `orphan.tmp` under that dir stays (not allowlisted).
- [x] Never glob all `*.tmp` under the tenant.
- [x] Tests: `TestMaterializeGCRemovesStaleDestTmp`, `TestMaterializeGCLeavesInFlightTmp`, `TestMaterializeGCLeavesLiveParquet`, `TestIsMaterializeScratchAllowlist`. Frozen clock, `os.Chtimes`, no Sleep.

### Shared

- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1 — **per slice**.
- [x] `make lint test` green locally (+ `make full-tests` because this touches I/O / lifecycle wiring).
- [x] No unused tests, no stale comments, no extra features.

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
