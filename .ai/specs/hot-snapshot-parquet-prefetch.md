# Spec: Pin hot snapshot inode for sandbox reads

Status: IN_REVIEW

- **Slug / branch:** `cursor/fix-hot-snapshot-parquet-prefetch-0de6`
- **Owner phase:** developer
- **PLAN phase(s):** Store query / engine hardening
- **Worktree:** `~/workdir/cursor-fix-hot-snapshot-parquet-prefetch-0de6/prism`

## 1. Task

Grafana PromQL against prism-store fails with:

```
execution: expanding series: Invalid Error: Prefetch registered for bytes outside file:
/data/user-fqsejat4-apps/hot/current.parquet, attempted range: [63795048, 63795576), file size: 168
```

`execution: expanding series` is Prometheus wrapping a DuckDB error from the
metrics sandbox. DuckDB bound parquet footer offsets from a ~64 MiB
`hot/current.parquet`, then a later `ExportHotSnapshot` atomically replaced that
path with a 168-byte zero-row snapshot (empty `hot_current` after flush). Scan
prefetch then used the old offsets against the new file size.

A local repro with go-duckdb v2.4.3 (DuckDB 1.4.1) confirms both facts:

- `COPY (… WHERE false) TO current.parquet` is **exactly 168 bytes**.
- Concurrent `os.Rename` of that empty file over a live `SELECT labels, value
  FROM metrics` yields the identical `Prefetch registered for bytes outside
  file: … file size: 168` error.

The sandbox treats `hot/current.parquet` as an immutable segment, but the writer
reuses that path on every export (per-request + `HOT_SNAPSHOT_SECONDS` ticker).
Pin the hot snapshot inode for the sandbox lifetime so a replace cannot mix
footer and bytes.

## 2. Scope

- **In scope:**
  - `internal/store/query` sandbox prep (`prepareMetricsSandboxConn`,
    `prepareSandboxConn`): after `collectMetricsSources`, pin
    `hot/current.parquet` and `hot/current.duckdb` to a unique sibling path
    (hardlink, copy on `EXDEV`), point the view/ATTACH at the pin, unlink pins
    in the sandbox cleanup.
  - Regression test that replaces `current.parquet` with a 168-byte empty
    snapshot during / after sandbox bind and still scans the original rows
    (the prefetch error must not surface).
  - Edge: empty snapshot pin (0 rows, no error); leftover pins unlinked on
    cleanup; `collectMetricsSources` still only lists `current.parquet` /
    `current.duckdb` (pins are not extra sources).
  - `docs/STORE.md` hot-snapshot section: sandbox pins the inode; Grafana
    DuckDB `print-view-sql` / `current.parquet` glob remains a residual race
    (metrics dashboards use PromQL).
- **Out of scope:**
  - Changing export naming (`current.parquet` stays the published name).
  - Grafana DuckDB `initSQL` / homelab-apps datasource overlays.
  - Skipping empty snapshot exports (empty overwrite is correct after flush;
    L0 holds the rows).
  - Pinning immutable tier segments.

## 3. Open questions

- [x] Q: Hardlink-per-query vs generation files vs skip-empty-overwrite? —
      A: **Hardlink pin on the read path.** Generation files help Grafana too
      but change the on-disk contract and need grace-delete. Skipping empty
      exports leaves stale hot rows that duplicate L0. The reported failure is
      PromQL, which we control: give the sandbox a path we will not rename over.
- [x] Q: Pin `current.duckdb` as well? — A: **Yes.** `AtomicExportDuckDB`
      replaces the same stable name; ATTACH/scan can reopen by path.
- [x] Q: What if `os.Link` returns `EXDEV`? — A: **Copy** to the pin path.
      Homelab PVCs are same-filesystem; copy is the rare fallback, not the
      dashboard path.

## 4. Decision log

- Pin the hot snapshot inode (hardlink to a unique sibling; sandbox reads the
  pin; unlink on cleanup):
  - ref: DuckDB parquet prefetch checks registered byte ranges against
    `GetFileSize()` and throws `Prefetch registered for bytes outside file`
    (https://github.com/duckdb/duckdb/blob/main/extension/parquet/include/thrift_tools.hpp).
    DuckDB's external file cache PR notes the same class of failure when a
    parquet file changes during execution
    (https://github.com/duckdb/duckdb/pull/16463). Apache Parquet files are
    immutable; readers bind footer offsets then later read those ranges
    (https://parquet.apache.org/docs/file-format/). This repo already chose
    “readers open by path, not by inode” for compacted sources
    (`.ai/specs/segment-delete-grace.md`); the hot snapshot is the one metrics
    file whose *path* is reused.
  - perf: `link(2)` + `unlink(2)` per query, no extra bytes while the live
    snapshot inode is still linked as `current.parquet`. Copy-on-`EXDEV` is
    one snapshot-sized write and only when the pin directory is a different
    filesystem than the PVC.
  - product: PromQL/`/sql` keep seeing the snapshot that was published when
    the sandbox opened, which is the freshness the handler already advertised
    after `ExportHotSnapshot`. Dashboards stop 500ing after a hot flush.

## 5. Acceptance checklist  (developer checks these off)

- [x] Regression: sandbox (or PromQL) column scan of `hot/current.parquet`
      still returns the original rows when that path is replaced mid-flight
      with a 168-byte zero-row parquet; error must not contain
      `Prefetch registered for bytes outside file`
- [x] Pin uses a unique sibling under the tenant `hot/` dir; view/ATTACH SQL
      uses the pin path, not `current.parquet` / `current.duckdb`
- [x] Sandbox cleanup unlinks pins (no leftover `hot/.read-*` after success
      or error)
- [x] Empty `current.parquet` (seed / post-flush) still queries as zero rows
- [x] `current.duckdb` hot format is pinned the same way
- [x] `docs/STORE.md` records the pin + the Grafana DuckDB residual
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (`make full-tests` — I/O/wiring)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
