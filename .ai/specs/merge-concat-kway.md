# Spec: Merge concat (metrics) + k-way (logs) + OOM quarantine

Status: CHANGES_REQUESTED
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/merge-concat-kway-1cdb` (cloud branch prefix; prism type is `feat`)
- **Owner phase:** orchestrator
- **PLAN phase(s):** store lifecycle / merge executor (post store-lifecycle)

## 1. Task

Stop the lucene metrics merge COPY from OOMing DuckDB (`ORDER BY ts` over hundreds of MiB of parquet, 359/359 failures in 6h on `user-fqsejat4-apps`). Metrics merge becomes **row-group concatenation** of already-sorted, same-schema L0s (no DuckDB rewrite, no feature flag). Logs merge becomes a **k-way merge by ingest ts** (schemas may differ). Schema mismatches split the pack instead of mixing columns. COPY+sort remains only as a guarded fallback (5 attempts, then mark sources unmergeable). Existing query, catalog, delete-grace, rollup, and materialize contracts stay intact.

## 2. Scope

- **In scope:**
  - `internal/store/merge` executor (metrics + logs), planner skip of quarantined/unmergeable sources, ScanTier
  - Sidecar markers for merge-skip / attempt count (layout helper next to `.compacted`)
  - `internal/store/lifecycle` tick: honor quarantine so a failed pack is not retried every `MERGE_TICK_SECONDS`
  - Unit + table-driven merge tests (large-pack, schema split, k-way order, fallback budget, existing timestamp/grace tests)
  - `docs/STORE.md` merge executor section; `docs/CONFIG.md` only if a new env is required (prefer none)
- **Out of scope:**
  - Feature flags / env to pick concat vs COPY (operator request: concat is the metrics path)
  - Homelab image bump / gitops promote (follow-up after this prism release is on `main` and tests below are green)
  - Changing lucene `MaxSegmentBytes` (2Gi disk seal stays; concat makes large packs safe)
  - Rewriting merge-time `last_events` SQL (already skip-on-error; not this OOM)
  - Adding `github.com/parquet-go/parquet-go` (stay on `apache/arrow-go/v18` already in the dependency budget)

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Metrics algorithm? — A: concat (row-group append in `MinTs` order). No flag.
- [x] Q: Logs algorithm? — A: k-way merge by `__prism_ts_ns` (fallback `ts`). No flag.
- [x] Q: Schema mismatch? — A: split pack by schema fingerprint; concat only homogeneous subsets (≥2 files). Singleton / other schema stays live (new “type”). Never UNION mismatched metrics schemas.
- [x] Q: Fallback? — A: DuckDB COPY+sort only when concat/k-way cannot run (duckdb-format segments, corrupt footer, or operator-forced). Max **5** failures per source fingerprint, then mark unmergeable (`too-large` / skip sidecar). Planner treats those like sealed (`Bytes >= MaxSegmentBytes`).
- [x] Q: Compatibility? — A: dest is still parquet (or duckdb when `SegmentFormat` is duckdb); same columns; per-row `ts` preserved; sources still retire via delete-grace; catalog/rollup/materialize still run after a successful dest rename.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Metrics merge = parquet row-group concat, not DuckDB `COPY (… ORDER BY ts)`**
  - ref: https://parquet.apache.org/docs/file-format/ — a file is magic + row groups + footer; “concat” means append row groups and write one new footer, not `UNION ALL` + sort.
  - ref: https://pkg.go.dev/github.com/apache/arrow-go/v18/parquet/file — `FileReader` / `Writer.AppendRowGroup` already in the dependency budget (`docs/DESIGN.md` §13).
  - perf: peak RAM is one row group, not decompressed UNION of 681Mi. The live OOM was DuckDB sort + 1e6-row writer vs `memory_limit=3276MB`. Concat does not open DuckDB.
  - product: each L0 is already `ORDER BY ts` at flush; lucene only packs non-overlapping time-adjacent runs (`planner.go` `rangesAdjacent`). Concat in `MinTs` order is globally sorted. Query still wraps `ORDER BY ts`. Time-clustered row groups are preserved per source group.

- **Do not rely on DuckDB `ORDER BY` as “k-way” for logs**
  - ref: https://github.com/duckdb/duckdb/discussions/18737 — planner does **not** k-way merge already-sorted parquet row groups; `ORDER BY` is a full sort.
  - ref: https://duckdb.org/2025/09/24/sorting-again — k-way inside DuckDB’s sort is about *its* sorted runs after a sort, not skipping the sort.
  - perf: log tier packs are not time-adjacent (`findLogTierPack`); a full `ORDER BY` reintroduces the metrics OOM. A Go heap of one page/row-group per source is O(k) memory.
  - product: logs already use `UNION ALL BY NAME` because schemas vary (`ExecuteLogMerge`). K-way must keep `__prism_ts_ns` (logs-ts-preserve spec) and fill missing columns with null (today’s BY NAME contract).

- **Schema split before rewrite, not `union_by_name` on metrics**
  - ref: `docs/STORE.md` metrics union: “Fixed schema only: no `union_by_name`”.
  - perf: mismatched schema is rare (flush always writes contract-v1). Splitting avoids a rewrite of the whole pack.
  - product: the odd file stays a live L0 (“new type”). Later ticks may concat it with other files of the same fingerprint. Fallback COPY+sort is for true rewrite needs, not for mixing schemas.

- **Quarantine after 5 failed rewrites; do not spin every merge tick**
  - ref: existing sealed skip `Bytes >= MaxSegmentBytes` in `planner.go`; delete-grace sidecar pattern in `internal/store/merge/grace.go`.
  - perf: 359 identical OOMs in 6h burned ~400m CPU continuously. Five bounded attempts then skip is enough to prove a pack cannot rewrite.
  - product: unmergeable sources remain readable (query still opens L0). They are not deleted. A sidecar is durable across restarts (in-memory counters are not).

- **No new parquet library**
  - ref: `docs/DESIGN.md` §13 — Parquet = `apache/arrow-go/v18`.
  - perf: streaming one Arrow record batch / row group is enough to kill the OOM; verbatim compressed-page copy would be faster but is an optional later optimization.
  - product: one parquet stack in the repo; encoder, ingest, and merge stay on the same reader/writer.

## 5. Algorithm (normative)

### 5.1 Metrics `ExecuteMerge` (parquet)

1. If any source is `.duckdb`, use today’s `AtomicExportDuckDB` path (unchanged).
2. Read each parquet footer; compute a schema fingerprint (canonical column names + types + repetition).
3. Partition sources by fingerprint.
4. For each partition with **≥ 2** files, sort by `MinTs` (then path), concat:
   - dest tmp file, `AppendRowGroup` for every source row group in that order
   - atomic rename to `L{dest}/<unixNano>.parquet`
   - `StatSegment` for min/max ts (footer stats preferred; existing `StatSegment` OK)
   - `retireSources` + metrics catalog sync (unchanged)
5. Partitions with **1** file: leave the source live (do not retire). Not an error.
6. If concat fails (I/O, corrupt footer, schema drift mid-file): do **not** retire. Record an attempt (see 5.3). If attempts remain, **fallback** DuckDB COPY of that homogeneous partition:
   ```
   COPY (SELECT * FROM (<union>) ORDER BY ts) TO tmp (FORMAT parquet, ROW_GROUP_SIZE …)
   ```
   This is the old path, including DuckDB caps (`threads`, `memory_limit`, `preserve_insertion_order=false`).
7. After a successful dest, lifecycle still runs rollup (dest tier ≥ 1) and materialize. Materialize SQL errors stay skip-and-log (`materialize.Run`).

Concat must preserve every row’s `ts` / `timestamp_ms` / `__name__` / `labels` / `value` (existing `TestExecuteMergePreservesPerRowTimestamps`).

### 5.2 Logs `ExecuteLogMerge`

1. Stamp/project `__prism_ts_ns` as today (`projectLogIngestTSSQL`).
2. Build a union schema (all column names across sources; missing → null). Equivalent to `UNION ALL BY NAME` output schema.
3. K-way merge: min-heap over sources ordered by `__prism_ts_ns` (then `ts` if the column exists, then source index). Write dest parquet with that schema.
4. Filename / bounds / `retireSources` unchanged (`mergedLogBounds`, window-id name).
5. Fallback: today’s DuckDB `UNION ALL BY NAME` + COPY **with** `ORDER BY __prism_ts_ns` only if k-way cannot start. Same 5-attempt quarantine.

Heap must not load whole files. Test with a low memory budget / many files.

### 5.3 Quarantine sidecar

Next to a source path (same pattern as `.compacted`):

- `layout.MergeSkipMarker(path)` — present ⇒ planner/Scan skip as merge input (still queryable).
- Attempt counter: either inside that marker as `attempts=<n>\nreason=<oom|copy|…>` or a sibling written before skip. **5** failures (`mergeMaxRewriteAttempts = 5`) then write skip with reason `too-large` (or `rewrite-exhausted`).

`FindMerges` / `FindLogMerges` / `ScanTier` / `ScanLog*` must ignore skip-marked files the same way they ignore `.compacted` holds.

Lifecycle: on merge error, increment attempts for **all sources in that action**, return/log, next tick sees skip or a smaller remaining pack.

### 5.4 Compatibility (do not break)

| Contract | Still true |
|---|---|
| Metrics schema | `__name__`, `labels`, `value`, `timestamp_ms`, `ts` — no `union_by_name` on the metrics query plane |
| Per-row timestamps | never rewrite to merge wall-clock or filename |
| Delete grace | dest durable first, then hold/unlink sources |
| Catalog | `metricsmeta.SyncAfterChangeRoots` after metrics merge |
| Logs `__prism_ts_ns` | preserved / stamped per source window |
| DuckDB `SegmentFormat` | still ATTACH+export, not concat |
| `RUN_JOBS=false` | still skips the ticker (unchanged) |
| Existing tests | `executor_test`, `metrics_ts_audit_test`, `grace_test`, `logs_ts_preserve_test`, `planner_test`, lifecycle merge tests stay green |

## 6. Acceptance checklist  (developer checks these off)

- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] Metrics concat: N same-schema time-adjacent L0s → one L1; row count = sum; `ts` globally non-decreasing; per-row values unchanged; sources retired (or grace-held)
- [x] Metrics concat does **not** use DuckDB COPY (assert via spy / no `merge copy:` error path / executor unit that fails if `db.Exec` COPY runs)
- [ ] Large pack: ≥14 files, compressed sum ≥200Mi (or a fixture that would OOM old COPY under `memory_limit=256MB`) concat succeeds under that cap
  - `TestExecuteMergeFourteenFilesNoCopy` is 14×50 rows with 2Ki labels (not ≥200Mi and would not OOM DuckDB COPY at `MemoryLimit=256MB`); add a fixture that meets either arm, or narrow this item to the bound the test actually proves.

- [x] Schema split: 5 files schema A + 1 file extra column → concat only A; extra file still live; no mixed schema dest
- [x] Singleton leftover is not deleted
- [x] Fallback COPY+sort: force concat fail (e.g. truncated parquet) → COPY path used; dest valid; attempts increment
- [x] After 5 rewrite failures, skip marker exists; 6th `FindMerges` does not include those sources; no error spin
- [x] Skip-marked files remain readable by `read_parquet` / StatSegment
- [x] Logs k-way: overlapping windows, extra column on one source, output ordered by `__prism_ts_ns`, extra column null-filled on other rows (`logs_ts_preserve` still passes)
- [x] Logs k-way peak: test with `MemoryLimit` too small for a full sort still succeeds (or concat/k-way path does not call DuckDB)
- [x] DuckDB-format metrics merge still uses AtomicExportDuckDB
- [x] Delete-grace + catalog tests still pass
- [ ] `make lint test` green locally (+ `make full-tests` — merge is I/O)
  - Reviewer: `make lint` green. Store `go test -race -tags duckdb_arrow ./internal/store/{layout,merge,lifecycle}` green (`-count=1`). `make test` failed on unrelated `TestE2E_LoggingThreePhaseParquet` (file-watch 5s); isolation re-run failed twice with the same error. `make full-tests` skipped (docker compose v2.29.7 is present; not run).


## 7. Test matrix (must exist before any homelab promote)

Extensive, table-driven, in `internal/store/merge/*_test.go` (and lifecycle tick if quarantine is wired there). Fixtures via `testparquet` + a helper that writes an extra column for schema-split.

**Metrics concat**

| # | Case | Expected |
|---|---|---|
| M1 | 3 ordered L0s, identical schema | 1 dest, 3 rows, ordered `ts`, inputs gone |
| M2 | Same as today’s timestamp audit (filename ≠ row ts) | row `ts` preserved, not merge clock |
| M3 | 14 files / large bytes, `MemoryLimit=256MB` | success; no OOM |
| M4 | Empty sources | error `merge: no sources` (unchanged) |
| M5 | One source only (executor called anyway) | no dest / no delete (or no-op error); do not invent a rewrite |
| M6 | Schema A×5 + schema B×1 | dest from A only; B live |
| M7 | Two schema families each ≥2 files | two dests **or** one tick one dest + leftover family next tick (pick one in impl, test it). Prefer **one dest per ExecuteMerge call** (first homogeneous group ≥2) to keep “one action per tick” |
| M8 | Concat I/O fail then COPY succeeds | dest OK, attempts=1, no skip marker |
| M9 | COPY OOM/error ×5 | skip marker, planner omits |
| M10 | Skip marker + 6 more live L0s | merge the 6, skip the marked |
| M11 | Row-group count dest ≥ 1; concat does not drop pages | row count exact |
| M12 | Existing grace hold still works on concat dest | compacted sidecar + delayed unlink |

**Logs k-way**

| # | Case | Expected |
|---|---|---|
| L1 | 3 landing windows, same schema | L0 dest, `__prism_ts_ns` ordered |
| L2 | Overlapping `ts` across files | heap order by `__prism_ts_ns`, not concat-by-file |
| L3 | Extra column on source 2 | present on those rows, null elsewhere |
| L4 | Preserve existing `__prism_ts_ns` (COALESCE) | `logs_ts_preserve` |
| L5 | Fallback after k-way start fail, 5× then skip | same as M9 |
| L6 | Landing refresh + tier pack still planned | planner tests unchanged |

**Lifecycle / regression**

| # | Case | Expected |
|---|---|---|
| C1 | `TickMerge` after skip markers | no `merge tenant` error spam; other tenants merge |
| C2 | `TestExecuteMergePreservesPerRowTimestamps` | green |
| C3 | `TestExecutorMergeCreatesOrderedSegmentAndDeletesInputs` | green |
| C4 | Planner sealed / shrink / overlap tests | green |
| C5 | Query still reads dest via catalog (lifecycle test or merge_catalog_test) | green |

Homelab promote (prism-cache image) is **blocked** until this matrix is in-tree and `make lint test` (+ `full-tests`) is green on the release tag. Reproducing `user-fqsejat4-apps` is soak, not a gate: after promote, lucene may concat the 503 L0s without the OOM loop.

## 8. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
  - `Executor` / `NewExecutor` still say merge is DuckDB COPY/ATTACH while parquet is concat/k-way first. `docs/STORE.md` says a corrupt footer takes COPY; `firstHomogeneousPack` skips fingerprint errors and can return `ErrNoHomogeneousPack` with no COPY. Spec §5.2 orders the heap by `__prism_ts_ns` then `ts`; `kwayHeap.Less` only uses ingest-ts then source index. Align comments + STORE.md (and the `ts` tie-break if it is still required).
- [x] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes
  - Gate 3 fails; `make test` is not green; Arrow concat/k-way never assert allocator balance (`CheckedAllocator`); `make full-tests` not run.

## 9. Reviewer notes

History (oldest→newest on the branch): `docs(store/merge)` → `test(store/merge)` (tests only) → `feat(store/merge)`. TDD contract holds.

Reviewer re-ran: `make lint` (0 issues). `CGO_ENABLED=1 go test -count=1 -race -tags duckdb_arrow ./internal/store/layout/ ./internal/store/merge/ ./internal/store/lifecycle/` green. `make test` failed on `TestE2E_LoggingThreePhaseParquet` (`logging_test.go:102` Eventually; pipeline `Failed to detect creation of …/app.log`). Isolation (`-run TestE2E_LoggingThreePhaseParquet`, twice) also failed in 5s — not only a `./...` parallel race. Diff does not touch `internal/e2e`. Docker compose v2.29.7 is installed; `make full-tests` was not run.

M8 records rewrite attempts in the lifecycle tick on ExecuteMerge error, not on a successful COPY fallback — a concat-then-COPY success does not write an attempts sidecar (sources are retired).

## 10. Homelab follow-up (not this PR)

After merge to prism `main` and a store release tag:

1. Bump `prism-store` image in `homelab-apps` `services/prism-cache` (and live-demo writer).
2. Promote via the normal apps → gitops auto-promote path.
3. Soak `user-fqsejat4-apps`: compact log should show concat success (or shrinking L0 count), **not** `merge copy: Out of Memory`; CPU should fall off the 400–500m plateau.

Do not promote on a build that lacks the test matrix in §7.
