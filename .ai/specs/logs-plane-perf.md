# Spec: Logs plane compaction + Loki open-set pruning (prism#77)

Status: ALL_OK

- **Slug / branch:** `feat/logs-plane-perf`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Store / query + lifecycle
- **Issues:** [#77](https://github.com/elk-utilities/prism/issues/77) epic; children #78–#89

## 1. Task

Make the logging use case first-class in prism-store the way metrics already are:
stop Loki blowing DuckDB expression depth under ~1000 tiny windows, time-prune
the Parquet open set, share one logs relation with `/sql`, avoid loading
`message` on label APIs, compact + retain log windows, and ship a release so
homelab can upgrade.

## 2. Scope

- **In scope:** #78–#89 gate behaviors (see Acceptance); docs in STORE.md / CONFIG.md; SemVer release after merge.
- **Out of scope:** Homelab-apps agent flush defaults; full LogQL; metrics hot_current for logs; raising `max_expression_depth` as the fix.

## 3. Open questions — resolved

- [x] Q: One PR vs many? — A: **One branch/PR** for the epic so one release ships the plane; small commits still TDD-ordered.
- [x] Q: Timestamp source after dropping per-file UNION? — A: Prefer window id from `<unix_ns>-*.parquet`; else file mtime (keeps existing fixtures).
- [x] Q: Logs tier layout? — A: `logs/<artifact>/tiers/L{n}/` (per-artifact schemas); landing stays `logs/<artifact>/*.parquet`.
- [x] Q: Shared relation vs Loki-only fork? — A: **One path list + read_parquet list**; Loki adds `__prism_ts_ns` via filename/mtime map JOIN.

## 4. Decision log

- **List `read_parquet([...], union_by_name=true)` instead of UNION-per-file:**
  - ref: https://duckdb.org/docs/stable/data/multiple_files/overview — multi-file read is first-class; Grafana DuckDB DS already survives via glob/list.
  - perf: expression depth O(1); planning cost no longer scales with UNION nesting.
  - product: fixes live admin 500 without raising depth limits.

- **Time prune by window id / mtime before open:**
  - ref: https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping — part pruning by partition/minmax.
  - perf: 1h Grafana range opens O(overlap) files, not O(all history).
  - product: matches Loki/CH operator expectations.

- **Per-artifact logs tiers under `logs/<artifact>/tiers/L{n}/`:**
  - ref: existing metrics `tiers/L{n}` + Loki boltdb→TSDB compaction (compact then delete inputs).
  - perf: reduces live landing file count under SEGMENTS_PER_TIER.
  - product: schemas differ per artifact; must not mix raw/template/summary in one merge.

- **Label APIs omit `message` projection:**
  - ref: Grafana Loki label APIs / CH DISTINCT on indexed cols only.
  - perf: avoids touching fat line bodies for variable refreshes.
  - product: `query_range` still returns lines.

## 5. Acceptance checklist

- [x] (#80) `TestLokiLogsSQLConstantDepthUnder1100Files`
- [x] (#79) `TestLokiOpenSetTimePrunedToRange`
- [x] (#87) `TestLokiAndSQLShareIdenticalLogsRelationSQL`
- [x] (#89) `TestLokiLabelAPIsDoNotProjectMessageColumn`
- [x] (#78) `TestLogsTierMergeCompactsPastSegmentsPerTier`
- [x] (#85) `TestLogsRetentionEnforcesFileCapAndMaxAge`
- [x] (#81) `TestLogsFileMetaCacheServesSecondLabelsWithoutFullRescan`
- [x] (#82) `TestLokiLabelValuesUsesCardinalityIndexNotMessageScan`
- [x] (#83) `TestLogsQueryDefaultsToRecentSegmentsNotFullHistory`
- [x] (#84) `TestLogsIngestCoalesceRespectsMaxAgeOrMaxBytes`
- [x] (#86) `TestLogsQuerySandboxThreadsIndependentOfMergeThreads`
- [x] (#88) `TestLogsManifestUpdatedOnLandAndReadByPlanner`
- [x] Docs: STORE.md + CONFIG.md logs lifecycle
- [x] Tests written first (`test:` commit precedes implementation)
- [x] `make lint test` green locally (+ targeted store tests)
- [ ] Release tagged after merge (v1.8.0)

## 6. Mandatory review gates

- [x] **Gate 1 — Follows the guidelines**
- [x] **Gate 2 — Tests cover edge cases**
- [x] **Gate 3 — Docs & comments match**
- [x] **Gate 4 — Comments are atomic**
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
