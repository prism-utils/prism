# Spec: Log merge dest format wins (restore 1.0.13 COPY contract)

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/log-merge-dest-format-b37a`
- **Owner phase:** reviewer
- **PLAN phase(s):** store lifecycle / merge executor (regression of #155)

## 1. Task

Prod admin Grafana (`user-fknjdouh-apps`) 400s on `logs_raw` and last-1h Loki is
empty after 1.0.13/1.0.14. Root cause is a **1.0.14 regression** (prism#155,
`059a9c0` concat/k-way). Landing windows are `.duckdb`. `MERGE_SEGMENT_FORMAT`
is parquet. `ExecuteLogMerge` (and metrics `ExecuteMerge`) does:

```
useDuck := SegmentFormat == DuckDB || sourcesHaveDuckDB(sources)
```

When any source is DuckDB, `mergeLogsDuckDB` / `AtomicExportDuckDB` writes a
**DuckDB file** to a path whose extension is **`.parquet`** (`SegmentFormat.Ext()`).
Grafana/`read_parquet` then fails: `No magic bytes found at end of file`. Those
outputs are ~780 KiB (not 0-byte), so 1.0.13 `TooSmall` does not skip them.
Merge ticks then error on those L0 files; last-1h never becomes searchable even
though ingest is live.

**1.0.13** always COPY/export to the **destination** format (parquet dest →
parquet bytes even when sources are duckdb). That is the correct contract.
Restore it. Skip existing poison L0 on query and merge scan so current tenant
data is visible without a data-dir purge.

## 2. Scope

- **In scope:**
  1. Destination format wins. If dest is parquet, write parquet (k-way or COPY).
     DuckDB sources are inputs only — never export DuckDB bytes to a `.parquet`
     path. Same for metrics `ExecuteMerge` (`useDuck || sourcesHaveDuckDB`).
     When dest is parquet and any source is DuckDB, skip k-way/concat and COPY
     to parquet (k-way/concat cannot read `.duckdb`). When dest is duckdb,
     `AtomicExportDuckDB` is still correct.
  2. Query + merge scan skip files that are not valid parquet when opened as
     parquet (header+footer `PAR1` magic), not only size &lt; 8 bytes — so
     existing poison L0 on disk stops failing the whole `logs_raw` relation and
     merge tick. Skip only; do not rename (RO query plane).
  3. Tests first (`test:` commit then `fix:`). `make lint test`.
  4. STORE.md one-liner if operator-visible (dest-format contract + skip
     invalid parquet, not only TooSmall).
- **Out of scope:**
  - Metrics OOM / concat algorithm changes
  - Homelab gitops image pin (follow-up after tag)
  - Landing searchable without refresh
  - Quarantine/rename of poison files
  - Tagging a release (parent pins gitops after merge)

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Dest format vs source format when mixing duckdb landing + parquet dest?
      — A: **Dest wins** (1.0.13). DuckDB sources are ATTACH/COPY inputs only.
- [x] Q: Skip vs rename poison L0? — A: **Skip only.** Same as skip-unusable:
      shared Grafana proxy mounts data read-only; rename would fail.
- [x] Q: Metrics executor? — A: **Same `useDuck || sourcesHaveDuckDB` bug.**
      Fix in the same change. Metrics query skip is not required unless it
      shares the helper; logs query + logs merge scan are mandatory.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Dest format is source of truth (restore 1.0.13 COPY/export contract).**
  - ref: https://duckdb.org/docs/current/data/parquet/overview.html —
    `read_parquet` (and a `.parquet` path) is a Parquet reader. DuckDB writes
    parquet with `COPY … (FORMAT parquet)`, not by exporting a `.duckdb` file
    to a `.parquet` name.
  - ref: https://github.com/duckdb/duckdb/blob/main/extension/parquet/parquet_reader.cpp
    (`ParseParquetFooter`) — the reader checks the last four bytes for `PAR1`
    / `PARE`; otherwise it throws `No magic bytes found at end of file '%s'`.
    One bad path in `read_parquet([…])` fails the whole list (same class as
    https://github.com/duckdb/duckdb/issues/19871).
  - perf: all-parquet packs still k-way/concat (no DuckDB). Mixed landing→L0
    uses COPY to parquet, which is the 1.0.13 path and ATTACHes duckdb sources
    as inputs. Avoids writing a second incompatible object at the dest path.
  - product: Grafana Loki last-1h and `/sql` `logs_raw` can open L0. A config
    flip (`MERGE_SEGMENT_FORMAT=parquet` with duckdb landing) must not poison
    the searchable tier.

- **Skip invalid parquet on query/merge scan (footer+header magic), not only TooSmall.**
  - ref: https://parquet.apache.org/docs/file-format/ — a Parquet file starts
    and ends with 4-byte magic `PAR1` (plus a 4-byte little-endian footer
    length before the trailing magic). Size ≥ 8 is necessary but not
    sufficient; a ~780 KiB DuckDB file at a `.parquet` path passes TooSmall
    and still 400s `logs_raw`.
  - ref: Spark `ignoreCorruptFiles` (https://spark.apache.org/docs/3.5.9/sql-data-sources-generic-options.html)
    — production readers skip corrupt files rather than failing the job.
  - perf: one 8-byte header + 4-byte footer read per `.parquet` path (same
    order as an extra `Stat`). Cheaper than a failed sandbox / merge tick.
  - product: current tenant L0 poison is skipped so valid siblings stay
    visible without a manual data-dir purge. Skip not rename: RO query plane
    (skip-unusable-log-segments spec).

## 5. Acceptance checklist  (developer checks these off)

- [x] `ExecuteLogMerge` with duckdb landing sources and parquet dest writes a
      file whose header and footer are `PAR1` (DuckDB `read_parquet` succeeds).
      Dest path still uses `.parquet`.
- [x] `ExecuteMerge` (metrics) with duckdb sources and parquet dest writes
      parquet bytes to a `.parquet` path, not a DuckDB file.
- [x] Dest `SegmentFormat=duckdb` still emits `.duckdb` via `AtomicExportDuckDB`.
- [x] All-parquet sources still use k-way (logs) / concat (metrics); COPY only
      as fallback or when a duckdb source forces a rewrite to parquet dest.
- [x] Query `logs_raw` open set omits a `.parquet` path that is a DuckDB file
      (no footer `PAR1`) and still lists a valid sibling parquet.
- [x] `ScanLogTier` / log merge scan omits the same poison `.parquet` and still
      returns valid neighbors (tick does not fail on k-way of duckdb-bytes).
- [x] Skip does not rename or write a sidecar for the poison file.
- [x] Valid `.duckdb` landing/tier files are not skipped by the parquet-magic
      check (they stay ATTACH inputs).
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)
- [x] STORE.md one-liner: dest format governs merge output; query/scan skip
      `.parquet` files that lack parquet magic (not only size &lt; 8).

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

<!-- Reviewer appends one actionable line under any gate it unchecks. Set
     Status: ALL_OK only when every box above is checked; otherwise
     Status: CHANGES_REQUESTED. -->

_(empty until first review)_
