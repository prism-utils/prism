# Spec: prism-store — compaction (Lucene tiered merge) + rollups + retention + metering + tickers

Status: IN_REVIEW

- **Slug / branch:** `feat/store-lifecycle`
- **Owner phase:** orchestrator → developer
- **Issue:** elk-utilities/prism#25 (Epic #21) — depends on #24 (merged).

## 1. Task

Port the background lifecycle of `prism-store` from `homelab-apps`
`services/prism-proxy` (`internal/merge`, `internal/rollup`, `internal/lifecycle`,
`internal/stats/metering.go`): a **Lucene TieredMergePolicy-analogue** compactor,
**rollups** after promotions, **time-based retention**, **compaction metering**,
and the **four ticker loops** wired into `cmd/prism-store`. After this slice the
store self-manages its on-disk tiers end to end (ingest→hot→L0→L1…→retention).

## 2. Scope

- **In scope** (new packages under `internal/store/`): `merge`, `rollup`, `lifecycle`, and the metering half of `stats`.
  - **`merge` — planner (pure Go, no DuckDB):** `Segment{Tier,Path,Bytes,MinTs,MaxTs}`, `MergeAction{Sources,DestTier}`, `DeleteAction`. `Planner.FindMerges`: skip **sealed** segments (`Bytes ≥ MaxSegmentBytes`); group live segments by `Tier`; for the lowest tier with `≥ SegmentsPerTier` segments, group by **Lucene size level** (floor-rounded `log(bytes/floor)/log(segmentsPerTier)`), within a level pick the first **time-adjacent contiguous run** (gap ≤ one segment span) of up to `MaxMergeAtOnce`, then **shrink the set down to 1** until the summed bytes ≤ `MaxSegmentBytes`; return **at most one** action (no cascade), deterministic tier/level/path ordering. `DestTier = tier+1`.
  - **`merge` — executor + segment IO (DuckDB):** `StatSegment` (`MIN(ts),MAX(ts)` + file size), `ScanTier`/`ScanAllTiers` (skip dirs/dotfiles/non-parquet), `Executor.ExecuteMerge`: ephemeral in-memory DuckDB, `UNION ALL` sources → `COPY (… ORDER BY ts) TO 'L{dest}/<unixNano>.parquet.tmp'` → atomic rename → **delete source files only after** the output is durably renamed → return the new `Segment`.
  - **`merge` — retention:** `Retention(segments, now, cfg)` → `DeleteAction`s for segments with `MaxTs` **strictly before** `now − RetentionDays` (boundary **kept**: 15d kept, 16d deleted). Default 15 days.
  - **`rollup`:** `ParseSteps("1m,5m,1h")` → DuckDB interval literals; `Builder.BuildFromMerge(sourcePaths, now)` writes `<tenant>/rollups/<step>/<unixNano>.parquet` with schema `bucket, "__name__", avg, min, max, count, sum` = `time_bucket(INTERVAL '<step>', ts)` grouped by bucket + name, ordered. Plus test helpers `AggregateRaw`/`ReadRollup` for the "rollups match raw aggregates" assertion.
  - **`stats` metering** (in the existing `internal/store/stats`): `.metering.json` read/write (atomic tmp+rename, `0640`); `AddCompactionCPUSeconds` (increment), `CompactionCPUSeconds` (read); `TenantOnDiskBytes` (sum `tiers/`+`rollups/`+`hot/`+`engine.duckdb`(+`.wal`), **exclude** legacy `metrics-raw/` and dotfiles). (The `/stats` HTTP endpoint is #27; only the metering primitives land here.)
  - **`lifecycle` — Runner + the four ticks:** `TickHotSnapshot` (engine hot snapshot), `TickFlush` (engine `FlushDue`), `TickMerge` (per tenant: `ScanAllTiers` → `FindMerges` → one `ExecuteMerge`; **meter** the merge's elapsed seconds into `.metering.json`; if `DestTier ≥ 1` build rollups from the merged output), `TickRetention` (per tenant: delete expired tier segments **and** expired rollup files on the same clock). `FloorBytesFromHotWindow` heuristic. `MaxTier` default 8.
  - **`cmd/prism-store` wiring:** a single background goroutine with four independent tickers (snapshot `HOT_SNAPSHOT_SECONDS`=15, flush `FLUSH_TICK_SECONDS`=30, merge `MERGE_TICK_SECONDS`=60, retention `RETENTION_TICK_SECONDS`/`RETENTION_TICK_HOURS`=1h), started in `serve`, stopped on shutdown; errors logged (slog), never fatal. New env: `SEGMENTS_PER_TIER` (6), `MAX_SEGMENT_BYTES` (2147483648), `RETENTION_DAYS` (15), `ROLLUP_STEPS` (`1m,5m,1h`), `MAX_TIER` (8), plus the four tick intervals. Document all in `docs/CONFIG.md` + `docs/STORE.md`.
- **Clean-ups vs upstream:** each merge/rollup file re-declares `escapePath`/`tierDir`; introduce a tiny **`internal/store/layout`** package (`TierDir`, `RollupDir`, `ToSlash`) used by the new packages to avoid duplication (leave the already-merged `engine` untouched to avoid churn — note the optional future adoption). Use `strings.Join`. Fix the upstream `FloorBytesFromHotWindow` indentation glitch. Keep SQL file-paths formatted+`ToSlash` (driver limitation) with one atomic comment; no injected user input reaches SQL (paths are server-owned, ts values are engine-owned).
- **Out of scope:** the `/stats` and `/admin/ensure` HTTP endpoints + seeds (#27); query API (#26); Helm (#28); release (#29). Do not add those routes.

## 3. Open questions  (resolved before READY)

- [x] Metering unit — CPU vs wall-clock? → keep upstream **wall-clock elapsed of the merge** (single-threaded DuckDB COPY ≈ CPU for the burst) to preserve the **byte-for-byte `/stats` billing contract** the consumer depends on (#27/#30). Document the approximation in `docs/STORE.md`.
- [x] Retention on rollups? → yes, the issue requires it; delete rollup files whose **max `bucket`** is before the cutoff, same clock as tier retention (small `StatRollupMaxBucket` helper).
- [x] Shared path helpers? → new `internal/store/layout`; do not refactor `engine` in this slice (scope control).
- [x] Cascade merges? → **no** — at most one action per `TickMerge` pass, lowest tier first (Lucene-parity, bounded burst).

## 4. Decision log  (Decision Protocol)

- **Lucene TieredMergePolicy analogue over parquet tiers.**
  - ref: https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/index/TieredMergePolicy.html — size-level grouping, seal ceiling, bounded merge-at-once.
  - perf: one bounded merge per pass caps CPU/RAM bursts (~≤1 core for seconds, matching the measured ~3.5 CPU-s/L-merge); time-adjacent runs keep segments range-contiguous so queries prune by ts.
  - product: predictable compaction on a shared central pod; big segments seal and stop churning.
- **Rollups only after promotion to L1+.**
  - ref: https://duckdb.org/docs/sql/functions/timestamp (`time_bucket`) — vectorized downsample.
  - perf: rollups amortize long-range queries (≥24h/≥7d) to `5m`/`1h` grain; building them only on L1+ landings avoids rework on volatile L0.
  - product: Grafana long ranges stay fast without scanning raw.
- **Strict-before retention boundary (15d kept, 16d deleted).**
  - ref: prom-style TSDB retention semantics (delete blocks fully past the window).
  - perf: bounded disk; product: deterministic, tested boundary that the billing/on-disk model relies on.

## 5. Acceptance checklist  (developer checks these off)

- [x] **Planner:** 6 same-level L0 segments → one `MergeAction` to L1; sealed (`≥ MaxSegmentBytes`) segments never chosen; output cap shrinks the set; **no cascade** (one action/pass); deterministic — ported `planner_test.go` cases green.
- [x] **Executor:** merging 6 L0 → 1 L1 file, rows **ordered by ts**, **source files deleted** only after the output lands; `StatSegment`/`ScanTier` correct — ported `executor_test.go` green.
- [x] **Rollups:** after an L1+ merge, `rollups/{1m,5m,1h}/…parquet` exist and their `avg/min/max/count/sum` per (bucket,name) **equal the raw aggregates** (`AggregateRaw`) — ported `rollup_test.go` green.
- [x] **Retention:** a 16-day-old segment is deleted, a 15-day-old segment is **kept**; expired rollups removed on the same clock — ported `retention_test.go` green.
- [x] **Metering:** a merge increments `.metering.json` `compactionCpuSeconds`; `TenantOnDiskBytes` sums tiers/rollups/hot/duckdb and **excludes** legacy `metrics-raw/` + dotfiles — ported `metering_test.go` green.
- [x] **Tickers:** four independent tickers wired in `cmd/prism-store serve`, started and cleanly stopped on shutdown; a lifecycle test drives ingest→flush→(6×)→merge→rollup end to end and asserts a gap-free L1 + rollups.
- [x] New env parsed with documented defaults; `docs/CONFIG.md` + `docs/STORE.md` updated (incl. the metering wall-clock note).
- [x] `internal/store/layout` removes the `escapePath`/`tierDir` duplication in the new packages.
- [x] Tests written first (`test:` commit precedes implementation) — CONTRIBUTING.md §1.
- [x] `make lint test` green; `CGO_ENABLED=0 go build ./cmd/prism` passes; `go build ./cmd/prism-store` passes.

## 6. Mandatory review gates  (reviewer owns)

- [x] **Gate 1 — Guidelines:** planner is pure-Go/leaf; executor/rollup close their DuckDB handles (no leak); atomic tmp+rename everywhere; sources deleted only post-rename; tickers in one goroutine, stopped on ctx; slog at the edge, libs return wrapped errors; no globals; `internal/store/*` don't import `pipeline`.
- [ ] **Gate 2 — Edge cases:** empty/no segments; fewer than `SegmentsPerTier`; all sealed; overlapping/gapped time ranges (chain break); single oversized candidate (shrink to 1); retention exact boundary; rollup over multiple sources; metering when elapsed==0 (no write); merge when a source vanishes mid-pass; retention when a file is already gone.
- [x] **Gate 3 — Docs/comments match code:** `docs/STORE.md` lifecycle section (tiers, seal, rollups, retention boundary, metering approximation, tick defaults) + `docs/CONFIG.md` env match exactly; no forward references.
- [ ] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [ ] Full docs/REVIEW.md checklist; TESTING.md layering (pure-Go planner unit tests + DuckDB-backed golden/behavior tests for executor/rollup/lifecycle).

## 7. Reviewer notes

**REQUEST CHANGES** (2026-07-22). _(addressed — developer re-handoff 2026-07-22: Gate 4 nolint self-contained; edge-case tests for planner/metering/executor/retention/tickers; `RunBackgroundLoop` exported and tested with goleak.)_
