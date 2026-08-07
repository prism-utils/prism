# Spec: Preserve log ingest times through merge + ModeTail SeekEnd

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `fix/logs-ts-preserve-and-tail-seek`
- **Owner phase:** orchestrator
- **PLAN phase(s):** Phase — Store logs compaction + agent file input
- **Related issues:** #103 (merge collapses Loki times), #104 (ModeTail SeekStart re-ships)

## 1. Task

Two production bugs. **(A)** After log tier merges, Grafana/Loki show every row at merge wall-clock because Loki stamps `__prism_ts_ns` from the segment filename window id, and `ExecuteLogMerge` names the output with `layout.SegmentNameFormat(now, …)`. **(B)** Agent `ModeTail` opens with `io.SeekStart`, so every restart re-reads every log from byte 0 and re-ships history (insane duplicated volume). Fix merge to project per-source ingest time onto rows as `__prism_ts_ns`, prefer that column in Loki SQL, name merged segments from min source MinTs, confirm metrics keep per-row `ts`/`timestamp_ms`, and change ModeTail to `io.SeekEnd`. Update contract/docs comments so storage ingest-time stamping at land/merge is explicitly allowed.

## 2. Scope

- **In scope:**
  - `internal/store/merge/logs.go` (+ helpers): project `__prism_ts_ns` per source arm during log merge; name output with **min source MinTs** (not `now`); coalesce carefully if source already has the column.
  - `internal/store/query/logs_catalog.go` / Loki path (`buildLogsRelationSQL` / mixed): prefer per-row `__prism_ts_ns` column when present; filename→MinTsNs JOIN only as fallback for legacy files without the column.
  - Metrics merge path audit: confirm merge/PromQL keep per-row `ts` / `timestamp_ms`; fix only if something stamps from filename or overwrites `ts` on merge; document finding in Decision Log.
  - `internal/input/file/file.go`: ModeTail `Location` → `io.SeekEnd`; add tests.
  - Docs: one clear sentence each in `docs/OUTPUT_CONTRACT.md`, `docs/STORE.md`, and relevant Loki comments — OUTPUT_CONTRACT forbids **parser event** timestamps; **storage ingest-time** stamped at land/merge is allowed and required for honest charts after compaction.
  - Tests (TDD) covering merge time preservation, Loki preference of column vs legacy JOIN, ModeTail SeekEnd.
- **Out of scope:**
  - Durable byte-offset checkpoints / Filebeat-style registry (note as future work only).
  - Homelab Grafana SQL triple-counting raw+template+summary (separate site-main task).
  - Changing parser OUTPUT_CONTRACT to emit event timestamps.
  - Rewriting historical already-merged segments on disk (new merges + new Loki reads only).

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: How should merge preserve times? — A: Project each source's ingest window ns onto every row as `__prism_ts_ns` (BIGINT); coalesce/EXCLUDE if column already exists; use source MinTs/StatLogSegment window ns per SELECT arm.
- [x] Q: How should Loki pick the time axis? — A: Prefer `__prism_ts_ns` column when files may contain it; filename JOIN only as fallback for legacy files without the column.
- [x] Q: Merged segment filename? — A: Name with **min source MinTs**, not `now`, so legacy filename consumers stay closer to truth; per-row column remains source of truth.
- [x] Q: ModeTail on restart without durable offsets? — A: SeekEnd (`io.SeekEnd`) — ship only new lines after start; durable offsets are future work.
- [x] Q: Metrics affected? — A: Audit only; fix iff merge overwrites `ts` from filename/now. Expected: metrics already keep per-row `ts`/`timestamp_ms` (ORDER BY ts merge). Document in Decision Log.
- [x] Q: Contract wording? — A: Clarify: parsers must not emit event timestamps; storage may/must stamp ingest-time at land/merge for honest post-compaction charts.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Preserve per-row ingest time through log merge (column `__prism_ts_ns`), not merge wall-clock:
  - ref: https://shivamagarwal7.medium.com/elasticsearch-navigating-lucene-segment-merges-9ed775bd45cb — Lucene/ES merges re-index documents into a new segment; document field values (including timestamps) are preserved, not rewritten to merge time.
  - perf: One BIGINT column projection per merge SELECT arm; coalesce/EXCLUDE is O(columns) SQL, no extra I/O beyond existing COPY. Avoids per-query JOIN map when column is present.
  - product: Time-series UX requires honest charts after compaction; collapsing to merge-time is incorrect (same class of bug as rewriting doc `_timestamp` on Lucene merge).

- Loki prefers column `__prism_ts_ns`, filename JOIN as legacy fallback:
  - ref: same Lucene merge model + existing prism Loki design in `docs/STORE.md` (ingest-time axis). After merge packs many windows into one file, filename can only hold one window id — column is required for correctness.
  - perf: Column path skips VALUES JOIN; legacy path unchanged for old files.
  - product: Gradual rollout-safe: new merges write the column; old landing files still work via JOIN.

- Name merged log segment with min source MinTs (not `now`):
  - ref: https://prometheus.io/docs/prometheus/latest/storage/ — compacted blocks keep the time range of their samples; block identity reflects data time, not compaction wall-clock.
  - perf: Trivial (already have `mergedLogBounds`); no extra scan.
  - product: Legacy consumers that only read filename stay closer to truth; column remains SoT.

- Metrics: confirm per-row `ts`/`timestamp_ms` survive merge; do not invent a second timestamp column unless audit finds overwrite:
  - ref: https://ganeshvernekar.com/blog/prometheus-tsdb-compaction-and-retention/ — Prometheus TSDB compaction N-way merges samples while preserving original sample timestamps.
  - perf: No-op if already correct (`ExecuteMerge` already `ORDER BY ts` / `SELECT *`).
  - product: Same invariant as logs: compaction must not rewrite sample time.
  - **Audit (implementation):** `ExecuteMerge` is `SELECT * FROM (UNION ALL …) ORDER BY ts` — per-row `ts` and `timestamp_ms` are preserved; nothing stamps from the segment filename or merge wall-clock. Covered by `TestExecuteMergePreservesPerRowTimestamps`. No metrics code change.
  - **Audit result (2026-08-07):** `TestExecuteMergePreservesPerRowTimestamps` confirms metrics merge keeps per-row `ts` (not merge wall-clock / filename). No code change.

- ModeTail SeekEnd on start (not SeekStart); durable offsets deferred:
  - ref: https://www.elastic.co/docs/reference/beats/filebeat/filebeat-input-log — Filebeat `tail_files: true` starts at end for files never seen; production shipping avoids re-indexing entire history on first/restart without registry. Durable registry is best long-term, but SeekEnd is the critical fix when no checkpoint exists (current state).
  - perf: Avoids re-reading multi-GB histories on every agent restart; CPU/network/storage savings dominate.
  - product: Standard `tail -F` shipping semantics; SeekStart caused multiplicative duplicate volume (e.g. bluetooth template counts in the hundreds of thousands).

## 5. Acceptance checklist  (developer checks these off)

- [x] Log merge projects per-source ingest ns onto rows as `__prism_ts_ns` (coalesce if already present; careful EXCLUDE so UNION schemas stay clean)
- [x] Merged log segment filename uses **min source MinTs**, not wall-clock `now`
- [x] Loki/`buildLogsRelationSQL` (and mixed path): prefer `__prism_ts_ns` column when present; filename JOIN only for legacy files lacking the column
- [x] Metrics merge/PromQL audit documented in Decision Log (and fixed if any path stamps from filename or overwrites `ts`)
- [x] ModeTail uses `io.SeekEnd`; tests assert seek-end behavior (and that SeekStart is not used for ModeTail)
- [x] OUTPUT_CONTRACT.md + STORE.md + Loki-related comments: one clear sentence that storage ingest-time at land/merge is allowed/required; parsers still must not emit event timestamps
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` because I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
  - `internal/e2e/logging_test.go` uses `time.Sleep(200 * time.Millisecond)` to wait for the tailer before append — TESTING.md §2 / REVIEW red flag: replace with `require.Eventually` (or channel/ctx sync), no `time.Sleep`.
- [x] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
  - `internal/store/query/log_ingest_ts.go` comment on `logSegmentHasIngestTS` names `annotateDuckLogIngestTS` — drop the cross-function pointer; describe the local constraint only (e.g. duckdb needs DESCRIBE after ATTACH).
- [ ] Full docs/REVIEW.md checklist passes
  - Blocked by Gate 2 (`time.Sleep`) and Gate 4 (non-atomic comment); also checklist “no time.Sleep; deterministic”.

## 7. Reviewer notes

- Re-ran locally: `make lint test` green (0 lint issues; all packages ok, including `internal/input/file`, `internal/store/merge`, `internal/store/query`). `make full-tests` green (integration + e2e OK).
- Test-first history holds: `94201cf test:` → `cf5a61f test(store,input):` precede `e95bf5a fix(store,input):`.
- Implementation matches READY decisions: merge stamps `__prism_ts_ns` + min MinTs filename; Loki column-prefer + legacy JOIN; metrics audit only; ModeTail SeekEnd; contract/docs clarified.
- Fix only the unchecked items above; do not broaden scope.

