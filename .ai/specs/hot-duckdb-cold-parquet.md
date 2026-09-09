# Spec: Promote converts hot DuckDB L1+ to cold Parquet

Status: ALL_OK
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/hot-duckdb-cold-parquet-2fb0`
- **Owner phase:** orchestrator
- **PLAN phase(s):** store lifecycle / cold promote

## 1. Task

Metrics (and compacted log tiers) stay **DuckDB on the hot SSD root**. When a
compacted **L1+** `.duckdb` segment is promoted to `COLD_DATA_DIR`, convert it
to **Parquet** on the cold dest — do not byte-copy a `.duckdb` file onto HDD.
L0 `.duckdb` never promotes. `.parquet` sources keep today's byte-copy + PAR1
check (mixed trees during rollout).

This unblocks homelab `HOT_SEGMENT_FORMAT=duckdb` + `MERGE_SEGMENT_FORMAT=duckdb`
on the per-tenant writer. Today `promote.CopyAtomic` byte-copies then
`verifyParquetMagic`; a `.duckdb` source would fail or land DuckDB on cold.
Grafana last-1h PromQL is ~25s with hot parquet; DuckDB ATTACH of a hot
snapshot is the intended hot format. Cold HDD stays parquet for portable scans.

Default env in the prism-store binary stays parquet for back-compat; homelab
charts set duckdb. Document the recommended writer env.

## 2. Scope

- **In scope:**
  - `internal/store/promote`: dest path `.duckdb` → `.parquet`; convert L1+
    duckdb at promote time; parquet L1+ (and leftover aged L0 parquet) still
    byte-copy; skip L0 `.duckdb`; `recoverOrCopy` must not SHA-compare duckdb
    source vs parquet dest; leftover cold `.duckdb` from a bad copy is removed
    and converted; convert writes via unique `*.promote.tmp` on the **cold**
    filesystem then rename; convert failure leaves hot source; dest unpublished.
  - Reuse `segformat.ConvertDuckDBToParquet` (or a thin helper that COPYs onto
    a caller-supplied dest file) with table name from `tableForPath` /
    `MetricsTable` vs `LogsTable` (export `TableForPath` if needed).
  - Crash GC of `*.promote.tmp` still deletes convert temps; a completed
    parquet dest is not deleted.
  - `AfterPromote` catalog rebuild must see the cold parquet path.
  - Docs: `docs/CONFIG.md` + `docs/STORE.md` (+ `cmd/prism-store` cold-tier
    comment if it still says byte-copy). Recommended writer env. No Grafana
    glob change.
- **Out of scope:** Helm/charts (homelab-apps); changing default
  `HOT_SEGMENT_FORMAT` / `MERGE_SEGMENT_FORMAT` (stay parquet); Grafana glob;
  converting in-place on SSD then copying; object storage; changing L0
  **parquet** eligibility (current main still promotes leftover aged L0
  parquet); DuckDB storage-version convert job.

## 3. Open questions  (must be empty/answered before `Status: READY`)

Resolved in the intake request. Do not re-ask.

- [x] Q1: Convert at promote time, not a separate job? — A: yes. Reuse
      `segformat.ConvertDuckDBToParquet`. Table via `tableForPath` /
      MetricsTable vs LogsTable.
- [x] Q2: Dest path? — A: same relative path with `.duckdb` → `.parquet`.
      Catalog rebuild (`AfterPromote`) sees the cold parquet path. Hot source
      `.duckdb` is held/unlinked per existing delete-grace.
- [x] Q3: `.parquet` sources? — A: keep today's byte-copy + magic check.
- [x] Q4: `recoverOrCopy` SHA? — A: must not SHA-compare duckdb source vs
      parquet dest. If dest parquet already exists and is valid (PAR1), skip
      convert. If dest is leftover `.duckdb` from a bad copy, remove and convert.
- [x] Q5: Where does convert write? — A: temp on the **cold** filesystem then
      rename (same crash-safety as CopyAtomic). Do not convert in-place on SSD
      then copy.
- [x] Q6: Docs / Grafana? — A: CONFIG.md + STORE.md. No Grafana glob change
      (query already ATTACHes duckdb + read_parquet mixed).
- [x] Q7: Default env? — A: binary default may stay parquet. Document
      recommended writer env `HOT_SEGMENT_FORMAT=duckdb` +
      `MERGE_SEGMENT_FORMAT=duckdb` (cold still parquet via promote convert).
- [x] Q8: L0? — A: current main promotes leftover **aged L0 parquet**. This
      task does **not** revert that. L0 **`.duckdb`** is never promoted
      (stays on hot for ATTACH). Convert applies to compacted **L1+** duckdb.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Convert at promote time (not a background convert job):
  - ref: https://duckdb.org/docs/lts/sql/statements/copy.html — `COPY … TO`
    FORMAT parquet is the supported projection; DuckDB `USE_TMP_FILE` writes
    a sibling temp then renames so a cancelled write does not replace a good
    dest. Promote already owns dest-FS temp + rename; reuse that seam.
  - perf: convert CPU runs once per aged L1+ file on the writer (`RUN_JOBS`);
    query replicas only read. Avoids a second walk of the tree and a second
    on-SSD parquet that would then be copied to HDD.
  - product: one job, one crash-safety story (`*.promote.tmp` + GC). Homelab
    can set duckdb on hot without landing DuckDB files on spinning disk.

- Cold dest is Parquet (PAR1), not a byte-copied `.duckdb`:
  - ref: https://parquet.apache.org/docs/file-format/ — 4-byte `PAR1` magic at
    head and tail is the portable identity check already used by
    `verifyParquetMagic`.
  - ref: https://chistadata.com/clickhouse-storage-tiering/ — hot SSD / cold
    HDD (or object) tiering keeps recent data native-fast and aged data on
    cheaper, portable storage.
  - perf: DuckDB ATTACH of a hot snapshot is the intended last-1h path; cold
    scans stay `read_parquet` on HDD. Convert is sequential per eligible file,
    not on the ingest hot path.
  - product: mixed trees during rollout; query already unions ATTACH +
    read_parquet. No Grafana glob change.

- Unique `*.promote.tmp` on the cold FS, COPY directly onto that temp, then
  rename (do not convert on SSD then copy; do not use `dst + ".tmp"` which
  crash-GC does not collect):
  - ref: https://duckdb.org/docs/lts/sql/statements/copy.html (`USE_TMP_FILE`)
    plus promote's existing CopyAtomic (unique temp, fsync, magic, same-FS
    rename). DuckDB's own extra `.tmp` suffix would not match
    `layout.IsPromoteTemp`.
  - perf: one write to HDD; no extra SSD parquet leftover.
  - product: kill mid-convert leaves unpublished dest; GC deletes
    `*.promote.tmp`; completed parquet dest is kept.

- `recoverOrCopy` skips SHA when source is duckdb and dest is parquet:
  - ref: https://parquet.apache.org/docs/file-format/ — different encodings
    cannot share a content hash; identity is PAR1 + readable rows, not SHA.
  - perf: skip a useless full-file hash of mismatched formats.
  - product: a valid existing cold parquet is not deleted; a leftover cold
    `.duckdb` from today's byte-copy bug is removed and replaced.

## 5. Acceptance checklist  (developer checks these off)

- [x] Test first (test-only commit before implementation): promote of an L1
      `.duckdb` writes `.parquet` on cold, hot source removed/held, dest has
      PAR1 magic, rows match.
- [x] Parquet L1 still byte-copies (no convert).
- [x] L0 duckdb is never promoted.
- [x] Convert failure leaves hot source in place; dest unpublished (no
      truncated canonical file).
- [x] Crash GC still deletes `*.promote.tmp`; a completed parquet dest is not
      deleted.
- [x] `recoverOrCopy`: valid dest parquet + duckdb source skips convert (no
      SHA); leftover dest `.duckdb` is removed then converted.
- [x] Convert writes unique `*.promote.tmp` on the cold filesystem then
      rename; no in-place convert on the hot root.
- [x] Docs match (`CONFIG.md`, `STORE.md`; recommended writer env).
- [x] Tests written first (a `test:` commit precedes implementation) —
      CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring
      touched) — `make lint test` 0 issues; `make integration` + `make e2e` OK

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [x] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [x] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

History: `test(store/promote)` (415acb3) precedes `feat(store/promote)` (8dd78f0).
Re-ran `make lint test` (0 issues, race green) plus `make integration` and
`make e2e` (I/O). Convert failure, L0 skip, leftover duckdb, valid-parquet
recover, GC of `*.promote.tmp`, and AfterPromote dest path are covered.
No DESIGN.md drift (cold promote lives in STORE.md). Comments describe local
crash-safety; they do not point at other symbols.
