# Spec: Admin logs must-fix (same-type merge, dual-format query, isolation, L0 cold, full billing)

Status: IN_REVIEW

- **Slug / branch:** `cursor/admin-logs-must-fix-1cdb`
- **Owner phase:** developer
- **Closes:** prism#158 (epic), #159, #160, #161, #162, #163

## 1. Task

Prod admin (`user-fknjdouh-apps`) holds ~57 GiB of log segments on hot SSD, Grafana/Loki/`/sql` fail, merge errors every tick, and `/stats` bills ~176 MiB. Fix the five must-fix bugs without rewriting working metrics parquet concat, ingest of either format, or Grafana dashboard JSON.

## 2. Scope

- **In scope:** `segformat` payload sniff; log/metrics merge dest follows source payload (no transcode); same-type packs; rename mismatched extensions; query ATTACH duckdb-magic files even under `.parquet` and skip unreadable files; `mergeLogsTenant` continues after pack/artifact errors; L0 older than `COLD_AFTER` is packable and promotable; `TenantOnDiskBytes` walks both tenant roots.
- **Out of scope:** `COLD_AFTER` default/value; Grafana JSON; site-main UI; object storage; charging prices.

## 3. Open questions

- [x] Q: Transcode duckdb→parquet because `MERGE_SEGMENT_FORMAT=parquet`? — A: **No.** Same-type only. Dest extension follows source payload magic.
- [x] Q: Promote leftover single/skip L0 after 12h? — A: **Yes.**
- [x] Q: Billing includes temp/sidecars/`metrics-raw`/hot logs? — A: **Yes, every regular file under both tenant roots.**
- [x] Q: Query fake `DUCK` header that is not a real database? — A: Include by magic at scan; **skip on ATTACH failure** so one poison file cannot 400 the relation.

## 4. Decision log

- Same-type merge, dest follows payload:
  - ref: https://parquet.apache.org/docs/file-format/ — magic `PAR1` at head and tail identifies parquet; DuckDB files use `DUCK` at offset 8 (`internal/duckdbfile`).
  - perf: avoids DuckDB COPY of 56k landing files into parquet (the 1.0.14 OOM class).
  - product: both formats are first-class; naming bytes `.parquet` made Loki `read_parquet` abort.
- L0 after `COLD_AFTER` may leave hot:
  - ref: existing `COLD_AFTER` / `max_ts` clock in `docs/CONFIG.md`.
  - perf: SSD stays for ingest + young L0; aged L0 moves after a same-type pack.
  - product: 12h means cold. The old “L0 never leaves” exception filled the OS disk.
- Billing walks the tenant tree:
  - ref: `/stats` `onDiskBytes` is what site-main already meters.
  - perf: one `WalkDir` per tenant per scrape; cheaper than wrong bills.
  - product: landing and temp files occupy disk.

## 5. Acceptance checklist

- [x] DuckDB sources merge to `.duckdb`; parquet sources to `.parquet`; mixed source list errors
- [x] Planner emits separate same-type packs
- [x] Mismatched live log extensions are renamed to match payload; format-mismatch skip sidecars cleared
- [x] Loki/`logs` relation opens parquet via `read_parquet` and duckdb-magic files via ATTACH; ATTACH/read failure skips that file
- [x] `mergeLogsTenant` continues to later artifacts/packs after an error
- [x] L0 with `max_ts` older than `COLD_AFTER` is force-packed (same type) and eligible for promote; younger L0 stays hot
- [x] `onDiskBytes` counts logs landing, `.tmp`, `.compacted`, `metrics-raw`, both roots
- [x] Tests written first (a `test:` commit precedes implementation)
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates

- [x] **Gate 1 — Follows the guidelines**
- [x] **Gate 2 — Tests cover edge cases**
- [ ] **Gate 3 — Docs & comments match**
  `docs/STORE.md` logs/metrics rewrite still says dest format wins and duckdb sources COPY to parquet; query still says duckdb-at-`.parquet` is skipped. Update those sections (and `MERGE_SEGMENT_FORMAT` in CONFIG.md / STORE.md ingest) to match dest-follows-payload, ATTACH-on-magic, and repair-rename.
- [ ] **Gate 4 — Comments are atomic**
  `internal/store/merge/logs.go` `findLogTierPacks` comment names `findMergeForTier`. State the local constraint (no time-adjacency; log L0 windows are often minutes apart) without naming another function.
- [ ] Full docs/REVIEW.md checklist passes
  Observability & docs items fail with Gate 3 (STORE.md/CONFIG.md drift) and Gate 4 (non-atomic comment).

## 7. Reviewer notes

First pass: CHANGES_REQUESTED on Gates 3–4. Docs/comment follow-up is in the next commit (STORE.md/CONFIG.md dest-follows-payload + ATTACH-on-magic; `findLogTierPacks` comment no longer names another function). Re-review those two gates.
