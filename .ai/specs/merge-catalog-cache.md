# Spec: Merge planner uses existing metrics/log catalog

<!--
  Loop state: .ai/workflows/feature-loop.md
  Issues: prism#171 (epic), #172 (impl), #173 (compose e2e), #174 (release);
  homelab-gitops#1229 (pin + Argo + prod verify).
-->

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/merge-catalog-cache-1cdb`
- **Owner phase:** developer
- **PLAN phase(s):** store merge / catalog (post–v1.0.19)
- **Issues:** [#171](https://github.com/prism-utils/prism/issues/171) epic · [#172](https://github.com/prism-utils/prism/issues/172) impl · [#173](https://github.com/prism-utils/prism/issues/173) compose e2e · [#174](https://github.com/prism-utils/prism/issues/174) release · [gitops#1229](https://github.com/prism-utils/homelab-gitops/issues/1229) pin/prod
- **Label:** `merge-improvement`

## 1. Task

Every merge tick lists hot+cold segments and currently `StatSegment`s each file (in-memory DuckDB + `MIN/MAX(ts)`). On a live idle tenant that is ~6.4s p50 with no compact. Query already has `metricsmeta` / `logmeta` with path/min/max/bytes. Extend those catalogs with `mtime_ns`, plan merges from `readdir` + lookup, `StatSegment` only on miss, upsert after mutations, and **full-rebuild the JSON only when it is corrupt**, with ERROR logs and a Prometheus counter so a rebuild loop cannot hide.

Ship: tests-first PR → CI → squash-merge → tag `v1.0.20` (or next) → gitops pin → wait Argo Healthy/Synced → verify prod merge ticks. No human gate after this spec is READY.

## 2. Scope

- **In scope:**
  - `internal/store/metricsmeta` and `internal/store/logmeta`: `mtime_ns` on `ManifestFile`; incremental upsert/drop/persist; hydrate from JSON at process use; **full rebuild only on parse failure**.
  - `ScanAllTiersRoots` / `ScanLogTiersRoots` (and callers): readdir + catalog lookup; miss → bounds for that file only.
  - Stop `SyncAfterChangeRoots` (and logs equivalent) from `RebuildManifestRoots` on the compact/flush/promote path; replace with incremental dest upsert + source drop.
  - Include **cold L0** in the catalog (today rebuild scans cold from L1).
  - Metrics: `prism_store_catalog_lookup_total{plane="metrics|logs",result="hit|miss"}`, `prism_store_catalog_rebuild_total{plane,reason}` (`reason=corrupt` for the only production rebuild).
  - slog `ERROR` `msg="metrics catalog full rebuild"` / `logs catalog full rebuild` with `tenant`, `reason=corrupt`, `path`, `err`.
  - `docs/STORE.md` catalog/merge section; `docs/TESTING.md` compose target.
  - Docker compose e2e: `deploy/docker-compose.merge-catalog.yml`, `test/e2e` test, `make merge-catalog-e2e` (also covered by `make e2e` / `make full-tests` via the e2e tag).
- **Out of scope:**
  - New parallel cache structure or env flag to disable this.
  - Changing Lucene / catch-up / daily / `MaxSegmentBytes` policy (cold unsealed files stay mergeable; sealed files stay skipped).
  - Skipping all cold files.
  - Hot snapshot ticker, Grafana JSON, billing UI, gitops pin (follow-up #1229 after the release tag exists).

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: New in-memory map vs extend catalogs? — A: Extend `metricsmeta` / `logmeta` only.
- [x] Q: Skip listing and only update on merge ops? — A: No. `readdir` every tick; cache bounds only.
- [x] Q: Skip Lucene/catch-up on all cold? — A: No. Cold is a root, not “done”. Unsealed cold files remain candidates. Sealed (`Bytes >= MaxSegmentBytes`) already skipped.
- [x] Q: When is full rebuild allowed? — A: Only if the manifest **file exists and JSON parse fails**. Missing file = empty catalog + per-file misses. Restart = hydrate JSON, not rebuild. Mutations = incremental.
- [x] Q: How do we notice a rebuild bug? — A: ERROR slog + `prism_store_catalog_rebuild_total`. No silent rebuild.
- [x] Q: Compose before prod? — A: Yes (#173), required for `make full-tests`.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Catalog as planner input, not restat: Iceberg manifests carry per-file size and min/max so planners do not open data files ([Iceberg spec — manifests](https://iceberg.apache.org/spec/#manifests)).
  - ref: https://iceberg.apache.org/spec/#manifests
  - perf: O(dirents + cache hits) vs O(files × DuckDB open). Hits are a few hundred bytes of JSON; a miss still stats one file.
  - product: Query already trusts this catalog; merge must too so idle and busy ticks share one source of truth.

- Full rebuild only on corrupt metadata, always logged: ClickHouse’s extra MergeTree metadata cache was deprecated because inconsistency was dangerous ([CH#51303](https://github.com/ClickHouse/ClickHouse/pull/51303)). Prefer a small JSON catalog with an explicit rebuild signal over a silent second cache.
  - ref: https://github.com/ClickHouse/ClickHouse/pull/51303
  - perf: Incremental upsert avoids a second `FileBounds` pass after every compact (today `SyncAfterChangeRoots` restats the tree).
  - product: Operators can grep `catalog full rebuild` / alert on the counter; a loop is a P0, not a mystery 6s tick.

- Identity `(path, size, mtime)`: same fingerprint rsync/make use for “file unchanged”.
  - ref: https://www.gnu.org/software/make/manual/html_node/How-Make-Works.html
  - perf: `os.Stat` is tens of µs; DuckDB `read_parquet` MIN/MAX is the 6s.
  - product: Immutable segments almost never rewrite in place; size+mtime is enough. Path change on promote is a drop+miss.

- Still merge unsealed cold: promote is a data-age clock (`COLD_AFTER`); compact is a separate policy (`bucket: day`, Lucene count, catch-up). Dest of a pack remains the hot root then promote, as today.
  - ref: https://iceberg.apache.org/spec/#manifests (data files stay mergeable regardless of storage location)
  - perf: Planner over catalog structs is cheap; ExecuteMerge still costs when a pack is real.
  - product: “1 GiB then ignore” is `MaxSegmentBytes`; “one segment per day” still packs ≥2 files for that UTC day after they live on cold. A singleton day file is not rewritten (existing `SelectCompact` rule).

## 5. Acceptance checklist  (developer checks these off)

- [x] `ManifestFile` includes `mtime_ns`; query catalog readers still work
- [x] `ScanAllTiersRoots` / `ScanLogTiersRoots` hit catalog on matching path+size+mtime (no DuckDB)
- [x] Miss (new file, size/mtime change) stats that file once, upserts, later hits
- [x] Vanished files are dropped from the catalog (promote/purge/retention)
- [x] Compact/flush/promote persist incrementally — **no** `RebuildManifestRoots` on those paths
- [x] Corrupt JSON: one full rebuild, ERROR log with `reason=corrupt`, `prism_store_catalog_rebuild_total` +1; next idle scan does not increment it
- [x] Missing manifest: no rebuild counter; files filled via misses
- [x] Cold L0 files are in the catalog and in the merge scan set
- [x] Unsealed cold files still planned when Lucene/catch-up/daily would; sealed files still skipped
- [x] `prism_store_catalog_lookup_total{plane,result}` increments
- [x] `docs/STORE.md` + `docs/TESTING.md` describe behavior and `make merge-catalog-e2e`
- [x] Compose e2e (#173): idle ticks are catalog hits; pack still creates L1; corrupt JSON rebuilds once and is logged
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` including merge-catalog compose)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_

## 8. Ship after ALL_OK (orchestrator — no human gate)

1. PR to `prism-utils/prism` `main`, wait CI, squash-merge (#172+#173 in one PR is OK).
2. Tag `v1.0.20` (or next unused) on the merge SHA; wait `release.yml` + Trivy. Do not pin a failed scan (#174).
3. homelab-gitops#1229: bump `prism-store` tag lockstep (prism-proxy, site-main `PRISM_CACHE_IMAGE_TAG`, live-demo, dev+prod). Merge. Wait Argo Healthy/Synced.
4. Prod: `user-fqsejat4-apps` (and canary) image tag, `catalog_rebuild_total==0`, no ERROR rebuild logs, idle merge observe p50 ≪ 6s when there is no compact.
