# Spec: Store: metrics FileBounds from Parquet footer stats

<!--
  Loop state for prism#141 follow-up. Process: .ai/workflows/feature-loop.md
-->

Status: READY
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/metrics-bounds-footer-baa6`
- **Issue:** prod writer OOM after v1.0.5 metrics catalog (homelab tenant `user-fqsejat4-apps`)
- **Owner phase:** developer
- **PLAN phase(s):** store query / merge catalog (Phase 4-adjacent)

## 1. Task

v1.0.5 `FileBounds` runs `SELECT MIN(ts), MAX(ts) FROM read_parquet(...)` on every hot/L0/L* file while rebuilding the metrics catalog. That opens an **uncapped** in-memory DuckDB per file. On a tenant with ~65 L0 segments (one ~215 MiB), the per-tenant writer (`RUN_JOBS=true`) is **OOMKilled at 4 Gi** within ~80 s of start, so ingest stops and Grafana `count(up)` at `now` is empty. Shared RO already wrote a valid `_manifest.json`; range queries over the catalog window still return series. This change makes bounds **footer stats + a hard DuckDB memory cap**, so catalog rebuild cannot scan GiB of page data.

## 2. Scope

- **In scope:**
  - `internal/store/metricsmeta` `FileBounds` / `statMinMax`: Parquet `ts` min/max from `parquet_metadata` footer stats; DuckDB segments still `MIN/MAX` on the metrics table.
  - Cap the bounds connector (`memory_limit` + `threads=1`) so a stats miss cannot inflate RSS past a small budget.
  - Fallback `MIN(ts)/MAX(ts)` only when footer stats are absent; still under the same cap (fail closed / skip file if the capped scan cannot run).
  - Tests for footer bounds matching written `ts`, empty parquet still known-empty, garbage still skipped.
  - `docs/STORE.md` one-line note that the metrics catalog reads Parquet column stats, not a full `ts` scan.
- **Out of scope:**
  - Homelab chart memory formula (`duckdb% + gomem% > 100%`).
  - Changing fail-closed skip of files with unknown bounds (still skip).
  - Materializations (prism#140 / homelab-apps#693).
  - Merge executor’s own `MIN/MAX` on a single output segment.

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Footer stats vs keep MIN/MAX and only set `memory_limit`? — A: **Footer first.** A 128 MiB cap would still skip the 215 MiB L0 (catalog hole). Iceberg/Parquet planners read row-group min/max from the footer without opening data pages.
- [x] Q: Unknown stats: scan or skip? — A: **Capped scan fallback, then skip.** Same fail-closed as v1.0.5 when bounds cannot be obtained.
- [x] Q: DuckDB `.duckdb` hot/tier files? — A: **Still `MIN/MAX` on the attached metrics table** under the same memory cap (no Parquet footer).

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Read Parquet `ts` bounds from `parquet_metadata` footer stats, not `read_parquet` + `MIN/MAX`.**
  - ref: DuckDB Parquet metadata — `stats_min_value` / `stats_max_value` per row group (`https://duckdb.org/docs/current/data/parquet/metadata`). Parquet row-group min/max exist so readers skip data pages (`https://www.duckdb.org/2021/06/25/querying-parquet`).
  - perf: footer is kilobytes; catalog rebuild over 65 files stays O(files) metadata opens, not O(rows). Writer RSS stays under a 4 Gi cgroup.
  - product: Grafana shared RO and tenant writers keep a correct open-set catalog after 1.0.5 without crashlooping ingest.

- **Cap the bounds DuckDB (`memory_limit` ~128 MiB, `threads=1`).**
  - ref: DuckDB `SET memory_limit` is the supported way to bound a connection (`https://duckdb.org/docs/stable/configuration/pragmas.html#memory_limit`).
  - perf: a stats-miss fallback cannot allocate GiB; the cgroup is no longer the first backstop.
  - product: one huge L0 is skipped (logged) instead of killing the writer for every tenant.

## 5. Acceptance checklist  (developer checks these off)

- [ ] Parquet `FileBounds` uses footer `ts` stats when present; result matches the written ingest timestamps.
- [ ] Empty parquet remains known-empty `(0,0,true)`; unreadable files remain skipped `(false)`.
- [ ] Bounds DuckDB sets a small `memory_limit` and `threads=1` before any file read.
- [ ] DuckDB segment bounds still come from `MIN(ts)/MAX(ts)` on the metrics table (capped connector).
- [ ] `docs/STORE.md` states catalog bounds come from Parquet column stats.
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
