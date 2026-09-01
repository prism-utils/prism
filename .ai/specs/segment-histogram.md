# Spec: Tenant segment histogram diagnostic

Status: READY

- **Slug / branch:** `cursor/segment-histogram-1cdb`
- **Owner phase:** developer
- **PLAN phase(s):** operator diagnostics (store layout)

## 1. Task

Operators need a one-command snapshot of how a tenant store is laid out on disk:
which families and tiers hold data, how large the files are, and which calendar
days the parquet footers (or window ids) cover. This ships a `diagnostic/`
script plus `make diagnostic-segments TENANT=…` that prints JSON.

## 2. Scope

- **In scope:** `diagnostic/segment-histogram/` (Go CLI), `diagnostic/segment-histogram.sh`, Makefile target, docs/TESTING.md make-target list, unit tests
- **Out of scope:** mutating store files, DuckDB queries, HTTP admin API, changing merge/compact

## 3. Open questions

- [x] Q: Script language? — A: Go CLI under `diagnostic/` (arrow-go footer reads, CGO-free). Thin `.sh` wrapper so Make has a script entrypoint.
- [x] Q: Prod target identity? — A: `--tenant` is the store namespace (`user-fknjdouh-apps` admin canary, `user-fqsejat4-apps` almeidamarcos). `--data-dir` / `--cold-dir` default to the prod hostPaths.
- [x] Q: Histogram shape? — A: JSON with totals, by family/kind/tier/root, size buckets (`le_bytes`), UTC day buckets of min_ts, plus a compact per-file list.

## 4. Decision log

- Footer stats vs DuckDB `parquet_metadata`: footer-only via apache/arrow-go
  - ref: https://parquet.apache.org/docs/file-format/ — min/max live in column-chunk statistics; a diagnostic must not scan row groups.
  - perf: one footer read per file (~KB), no 128MB DuckDB session per segment.
  - product: runs on the operator workstation against hostPath without CGO or a store process.
- Size buckets as inclusive `le_bytes` powers of four KiB
  - ref: https://prometheus.io/docs/practices/histograms/ — cumulative `le` buckets are the operator-familiar histogram encoding.
  - perf: O(files) increment; no extra I/O.
  - product: matches how we already talk about lucene packs (tens of MiB vs 681MiB).
- Date histogram by UTC day of min_ts (footer, else window-id)
  - ref: same parquet footer stats; window-id is the store's on-disk clock (`layout.WindowIDNanos`).
  - perf: no extra reads.
  - product: answers "when is this tenant's data from?" without dumping every file in a spreadsheet.

## 5. Acceptance checklist

- [ ] `make diagnostic-segments TENANT=<ns>` prints JSON to stdout
- [ ] Snapshot covers metrics tiers, hot window, logs landing/tiers, rollups, materializations; hot+cold roots
- [ ] Compacted / `.tmp` / `_` prefixed files are skipped; unreadable parquet is counted not fatal
- [ ] JSON includes size histogram, UTC day histogram, per-segment path/bytes/min_ts/max_ts
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally
- [ ] Ran against prod admin (`user-fknjdouh-apps`) and almeidamarcos (`user-fqsejat4-apps`)

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
