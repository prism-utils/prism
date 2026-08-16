# Spec: Store: metrics open-set prune, auto-hot, time-partitioned L0

<!--
  Loop state for prism#141. Process: .ai/workflows/feature-loop.md
-->

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/metrics-openset-prune-baa6`
- **Issue:** [prism#141](https://github.com/prism-utils/prism/issues/141)
- **Owner phase:** reviewer
- **PLAN phase(s):** store query / merge (Phase 4-adjacent; query open-set)

## 1. Task

Shared-RO `prism-store` executes Grafana PromQL/`/sql`/`/query` itself (it is not a proxy to tenant `prism-cache`). Today `collectMetricsSources` unions `hot/current.*` plus every live `tiers/L*` file with no `[start,end)` prune, so `now-1h` opens tens of L0 blobs. Loki already skips log files whose `MaxTsNs < start || MinTsNs >= end`. This change copies that catalog pattern to **metrics**, auto-selects the hot snapshot when the query range sits inside snapshot coverage, and catalogues new L0 segments with min/max timestamps so prune does not need to open every footer on every query. Query CPU stays on the shared reader.

## 2. Scope

- **In scope:**
  - Metrics open-set prune for PromQL, `POST /{ns}/sql` metrics view, and structured `GET /{ns}/query` tier Parquet/DuckDB parts.
  - Auto-hot using the **hot snapshot’s actual min/max ts** (manifest or parquet/DuckDB stats); fallback to process `HOT_WINDOW_*` only when stats are missing.
  - Request `hot_only=true` still force-on; process `QUERY_HOT_ONLY=true` still cannot widen; `hot_only=false` still prunes by overlap.
  - Metrics manifest (logs analog) written on flush/merge/retention; new L0 catalogued with min/max ts; target new L0 on the order of the hot window (existing huge files remain until merged; prune still skips non-overlap).
  - `docs/STORE.md` + `docs/CONFIG.md` (+ DESIGN.md ADR note if topology text would otherwise be wrong).
- **Out of scope (this PR — Phase 0 locked):**
  - Homelab charts/gitops, Grafana datasource URL, `grafanaProxyUrl()` tests (homelab-apps follow-up; two-PR contract).
  - Merge-time business materializations (prism#140).
  - `MODE=cluster` scatter-gather / proxying Grafana to tenant `prism-cache`.
  - Changing Grafana timeout numbers.

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Re-open option 1+5 vs federate Grafana to tenant cache? — A: **Do not re-open.** Option 1+5 is locked: shared RO is the only Grafana query CPU; `hot_only` means “this process opens only the hot snapshot,” not “tenant does hot / shared does cold.”
- [x] Q: Auto-hot source of coverage? — A: **Snapshot actual min/max ts.** Shared reader often has `RUN_JOBS=false` and an unused `HOT_WINDOW_MINUTES=1440` while the writer flushes on 10m. Using the reader’s env would auto-hot a 24h Explore window incorrectly. Fallback to `HOT_WINDOW_*` only if stats are missing.
- [x] Q: Files with unknown bounds? — A: **Fail closed:** skip + log. Footer / `MIN(ts)/MAX(ts)` parse is allowed as a **fallback to obtain bounds** (then apply overlap). Prefer a metrics manifest so the query path does not Stat every file.
- [x] Q: `/sql` without a time window? — A: Optional `start`/`end` on the JSON body (RFC3339) and as query params. When absent, do **not** time-prune (unbounded `SELECT` is valid); `QUERY_HOT_ONLY` / request `hot_only` still apply. PromQL and structured `/query` always have a window.
- [x] Q: PromQL lookback / range selectors? — A: Expand the open-set start by `LookbackDelta` (and the expression’s max range, when parseable) so `rate(x[5m])` still sees enough samples. Document the expansion.
- [x] Q: Writer `QUERY_HOT_ONLY=true`? — A: **Alerts-only `hot_only` is enough** for this PR. Rulers already send `hot_only=true`. Homelab may set writer `QUERY_HOT_ONLY=true` as belt-and-suspenders in a follow-up; not required here.
- [x] Q: Grafana `ViewSQL` (static initSQL)? — A: Remains a full live-set union (no per-query window). Grafana Prometheus + `/sql` with `start`/`end` are the pruned planes.
- [x] Q: Homelab HOT_WINDOW alignment / grafana URL tests? — A: **Out of this PR** (user lock). Checkboxes copied below under follow-up; they do not block `ALL_OK` here.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Option 1+5 (shared executes; prune + auto-hot + time-partitioned L0; no tenant Grafana CPU).** Rejected 2/3/4/6: pointing Grafana at `prism-cache` or scatter-gather would bill dashboard reads to tenant pods.
  - ref: Apache Iceberg scan planning — manifest list + per-file column min/max skip files whose bounds miss the predicate (https://iceberg.apache.org/docs/latest/performance/ ). Same idea as Loki’s `filterLogFiles` (`MaxTsNs < start || MinTsNs >= end`) in-tree.
  - perf: a 1h PromQL against 7d of 1h L0 opens O(hot + overlapping files), not ~60 L0 (~GiB) `open()`s. Manifest avoids per-query DuckDB `MIN/MAX` over every file.
  - product: query CPU stays on shared RO (billing); tenant writer stays ingest/merge. Matches how lakehouse engines skip files before opening them.

- **Metrics manifest analogous to logs, not DuckDB hive glob as the primary prune.**
  - ref: Iceberg manifests store per-file `lower_bounds`/`upper_bounds` so planners skip without reading data files (https://iceberg.apache.org/spec/ ). DuckDB hive/filename pushdown (https://duckdb.org/docs/current/data/partitioning/hive_partitioning.html ) is complementary but would require a directory layout change (`year=/month=`) that breaks existing `tiers/L{n}/<unix_ns>-<id>.parquet` paths.
  - perf: JSON catalog is one small read vs N parquet footers; hive dirs would prune inside `read_parquet(glob)` but Grafana/PromQL sandbox currently lists explicit paths.
  - product: copy the logs pattern operators already trust; existing huge files stay readable and skippable once catalogued.

- **Auto-hot uses snapshot stats, not reader `HOT_WINDOW_*`.**
  - ref: Iceberg snapshot summaries describe *what is in the snapshot*, not a client’s configured window (https://iceberg.apache.org/docs/latest/performance/ ).
  - perf: a shared reader with unused `HOT_WINDOW_MINUTES=1440` must not drop overlapping L0 for `now-24h`. Using real `[min_ts,max_ts]` keeps 10m Grafana panels on `hot/current.*` only.
  - product: `hot_only=true` remains a force; `QUERY_HOT_ONLY=true` still cannot widen; `hot_only=false` still prunes.

- **Unknown bounds: skip + log; stats/footer to recover bounds.**
  - ref: Iceberg InclusiveMetricsEvaluator cannot skip when stats are absent and must treat files as maybe-match — we invert for safety (do not silently scan “everything”) per the issue (https://lakeops.dev/blog/apache-iceberg-query-planning ).
  - perf: skip is cheaper than opening a mystery GiB blob; one-time Stat on catalog rebuild recovers bounds for old files.
  - product: fail closed on garbage files; never `open()` a non-overlapping known file.

- **Writer `QUERY_HOT_ONLY` not required in this PR.** Alerts already pass `hot_only=true` (`docs/ALERTING.md`). Homelab chart flag is a follow-up.

## 5. Acceptance checklist  (developer checks these off)

Copied from prism#141 (homelab-follow-up items marked N/A this PR).

### Definition of done

- [x] Metrics PromQL, `/sql` metrics view, and structured `/query` tier parts use an overlap open set (manifest or equivalent). Loki behavior unchanged / still pruned.
- [x] Auto-hot documented and implemented in-store; `hot_only` request still force-on; process `QUERY_HOT_ONLY=true` still cannot widen.
- [x] New flushed L0 segments are time-bounded and catalogued with min/max ts.
- [x] Homelab writer vs shared `HOT_WINDOW_*` aligned in chart defaults + gitops — **N/A this PR** (out of scope; homelab-apps follow-up).
- [x] Grafana URL still shared loopback (unit test) — **N/A this PR** (homelab-apps). Writer not used as Grafana query plane (documented here).
- [x] `docs/STORE.md` + `docs/CONFIG.md`: open-set, auto-hot, layout, “shared executes, tenant does not serve Grafana.”
- [x] Homelab `docs/solutions/` pointer — **N/A this PR** (after apps wiring PR).
- [x] Spec Decision Log records option 1+5 (and why 2/3/4/6 were rejected: tenant query CPU / billing).
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

### Prune

- [x] Unit: metrics open-set with `start`/`end` covering only file B does **not** include file A (`max_ts < start`) or file C (`min_ts >= end`).
- [x] Unit: overlapping file (range crosses `start`) **is** included.
- [x] Unit: `hot_only=true` on a 24h range still opens **only** hot snapshot files (0 tier paths).
- [x] Unit: `QUERY_HOT_ONLY=true` process ignores a request that tries to widen (no API to widen — assert tiers absent).
- [x] Unit: PromQL `count(up)` 1h fixture with 7d of partitions records opened paths; count is **O(hot + overlapping)**, not all fixtures.
- [x] Test spy / path list: skipped files are never passed to `read_parquet` / `ATTACH`.

### Auto-hot

- [x] Unit: range `now-hotWindow`…`now` → open set is only `hot/current.*` even without `hot_only` query param.
- [x] Unit: range `now-24h`…`now` with `QUERY_HOT_ONLY=false` → hot + overlapping cold, not hot-only.
- [x] Unit: `hot_only=true` + 24h range → hot only.

### Layout

- [x] Flush/merge test: new L0 has min/max in manifest (and/or filename); catalog round-trip.
- [x] 1h query over 7×1h partitions opens at most hot + 2 L0 files in the fixture.

### Homelab wiring (follow-up; does not block this PR)

- [x] Chart/gitops HOT_WINDOW — **deferred** to homelab-apps (Phase 0 lock).
- [x] `grafanaProxyUrl()` default remains `http://localhost:8080` — **deferred** (not in this repo).
- [x] Writer `QUERY_HOT_ONLY=true` **or** explicit decision: **alerts-only `hot_only` is enough** (written in §3).

### Gates (developer does not check these)

_(reviewer owns §6)_

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
