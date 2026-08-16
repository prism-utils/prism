# Spec: Store merge-time configurable materialization queries

Status: READY
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/merge-materialize-baa6`
- **Owner phase:** developer
- **PLAN phase(s):** Store query/merge (prism#140)
- **Issue:** https://github.com/prism-utils/prism/issues/140

## 1. Task

When a merge already has files open, run named read-only DuckDB SQL against
`merge_input` / `merge_output` and write
`<tenant>/materializations/<name>/<ts>-<id>.parquet`. Grafana home panels that
today re-evaluate huge PromQL on every load instead scan this small columnar
artifact via `/sql` view `mat_<name>`. This is **not** `ROLLUP_STEPS` (those
drop labels; PromQL/SQL do not read them). Empty config is byte-identical to
today’s merge. Homelab dashboard JSON is out of scope (homelab-apps#693).

## 2. Scope

- **In scope:**
  - YAML config file pointed at by `MATERIALIZATIONS_FILE` (empty/unset = off).
  - Package `internal/store/materialize` hooked after dest rename in
    `lifecycle` merge (metrics and logs when `on` matches), next to
    `rollup.BuildFromMerge`.
  - Layout helper `layout.MaterializationDir`.
  - Compacted-sidecar skip for replaced source basenames so live `mat_*` rows
    are not double-counted.
  - `/sql` sandbox views `mat_<name>` that open **only** live materialization
    files (never raw `tiers/L*` / hot).
  - `docs/STORE.md`, `docs/CONFIG.md`, `--help` / env table, `docs/DESIGN.md`
    package mention.
- **Out of scope:**
  - Homelab dashboard JSON / which SQL Grafana runs (homelab-apps#693).
  - Changing `ROLLUP_STEPS` schema or making PromQL read those label-less
    rollups.
  - PromQL-over-arbitrary-schema (v1 is Grafana-queryable via `/sql` only).
  - MODE=cluster scatter-gather; tenant prism-cache federation.
  - Flush-time materialization (merge only).

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: PromQL vs SQL for v1 Grafana? — A: **SQL sandbox view `mat_<name>`**.
      PromQL-over-arbitrary-schema is out of v1. Grafana DuckDB `/sql` is the
      documented path.
- [x] Q: Config vehicle? — A: **YAML file + `MATERIALIZATIONS_FILE` env**
      (same pointer pattern as `AUTHZ_POLICY_FILE`; not a 4 KiB env blob).
- [x] Q: How to avoid double-count on re-merge? — A: dest materialization
      uses the **same basename** as the dest segment; when sources are
      compacted, matching-basename files under each `materializations/<name>/`
      get the same `.compacted` sidecar. Query listing skips compacted.
- [x] Q: Bound relations? — A: both `merge_input` (UNION ALL of source
      paths) and `merge_output` (dest segment). Recommended SQL uses
      `merge_output`.
- [x] Q: `RUN_JOBS=false`? — A: merge ticker never starts; `Builder.Run`
      is also a no-op when `RunJobs` is false so a mistaken caller cannot
      write.
- [x] Q: Invalid SQL load vs runtime? — A: load rejects invalid **name**,
      path traversal, empty SQL, non-SELECT / multi-statement / forbidden
      keywords. Runtime DuckDB errors (missing column, type) **log + skip
      that name**; merge + rollups still succeed.
- [x] Q: Empty tenant bind? — A: if no live files, `CREATE VIEW mat_<name>
      AS SELECT NULL WHERE 1=0`. Optional `_seed.parquet` is written on first
      successful run directory create so a documented glob still opens; the
      view never unions raw tiers.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Query path is SQL view `mat_<name>`, not PromQL:** Grafana-queryable in v1
  without inventing PromQL-over-arbitrary-schema.
  - ref: https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/
    — recording rules precompute expensive expressions for dashboards; they
    still emit **metrics** with a valid metric name. Our artifacts are
    **arbitrary-schema** (Last events / Open alerts), so PromQL is the wrong
    serving API; `/sql` is the analogue of “query the recorded result”.
  - perf: dashboard load opens a handful of small parquet files instead of
    ~1 GiB L0 + nested PromQL. CPU stays on the writer at merge time.
  - product: Grafana already has a DuckDB/`/sql` datasource in this stack;
    homelab-apps#693 only changes panel SQL.

- **Compute at merge (compaction), not at insert and not at query:** ClickHouse
  incremental MVs run on insert blocks; we already paid DuckDB to open merge
  inputs. Running named SQL then is the cheap incremental step.
  - ref: https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view
    — a materialized view is a trigger that runs a query on new blocks and
    writes a target table; query time reads the derived table.
  - perf: one extra `COPY (sql) TO parquet` per configured name per merge,
    bounded by existing `DUCKDB_THREADS` / `DUCKDB_MEMORY_LIMIT`. Shared RO
    (`RUN_JOBS=false`) never runs it.
  - product: matches “the file is already open during merge”; keeps Grafana
    30s dataproxy viable.

- **Idempotent replace via compacted sidecar, not dbt MERGE unique_key:** we
  do not upsert into one growing table; we write a new dest file and retire
  source-derived files, same as tiers.
  - ref: https://docs.getdbt.com/docs/build/incremental-strategy
    — dbt `incremental_strategy='merge'` upserts on `unique_key` to avoid
    duplicates. Our unique key is the dest segment basename; retirement is
    the `.compacted` sidecar already used by merge grace (STORE.md).
  - perf: no full-table MERGE join; O(sources) marker writes.
  - product: same live-set rule operators already reason about for tiers;
    `/sql` listing skips compacted so prism queries never double-count.

- **Config is a YAML file, fail-closed at load:** Prometheus rule_files reject
  the process (or reload) when the file is malformed; runtime eval of a bad
  expr must not take down compaction.
  - ref: https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/
    — `promtool check rules` / load-time syntax; a failed evaluation discards
    that rule’s series rather than stopping the server.
  - perf: parse once at start; merge path has no YAML I/O.
  - product: operators get a crash-loop with a named path on bad config;
    a typo’d column in SQL does not freeze merge/rollups.

## 5. Acceptance checklist  (developer checks these off)

Copied from prism#140 plus the feature-loop TDD/lint items.

### Issue definition of done

- [ ] Spec under `.ai/specs/` with Status `ALL_OK` before merge (feature loop).
- [ ] Config schema documented; invalid name / non-SELECT SQL rejected at load; runtime SQL error skips that name only.
- [ ] Merge with a configured materialization writes a live file under the documented path; empty config = **byte-identical** to today’s merge (no extra files).
- [ ] Existing `ROLLUP_STEPS` downsample still built on L1+; tests prove no regression.
- [ ] `RUN_JOBS=false` does not write materializations.
- [ ] A query path exists that returns **only** materialization rows for that name (no raw L0 union required for that query).
- [ ] Compaction/replace does not double-count materialization rows (compacted sidecar or equivalent).
- [ ] `docs/STORE.md` + `docs/CONFIG.md` updated; `--help` / env table complete.
- [ ] `make lint` and `make test` (and `make full-tests` if query/merge/e2e paths are touched) green.

### Issue tests the agent must check off before merge

- [ ] `git log` shows a **test-only commit before** implementation commits (reviewer gate).
- [ ] Unit: `BuildFromMerge` / new hook with one materialization SQL (`SELECT 1 AS x` or a real aggregate over `read_parquet` test files) creates the output file with expected columns.
- [ ] Unit: empty materialization list → no `materializations/` files (or only seed), merge output unchanged.
- [ ] Unit: invalid SQL at runtime → merge output still exists; error logged; process does not return merge failure.
- [ ] Unit: invalid config at startup/load → explicit error (name, path traversal, non-SELECT).
- [ ] Unit: `minTier` skips L0 when set to 1; runs on L0 when 0.
- [ ] Unit: `RUN_JOBS=false` path never calls the writer.
- [ ] Unit: compacted/replaced merge input does not leave **two live** materialization files covering the same rows.
- [ ] Query test: `/sql` (or chosen API) against `mat_<name>` (or documented view) returns the materialized rows and does **not** require opening fixture raw tier files (assert open-set / EXPLAIN / path list).
- [ ] Regression: existing rollup tests (`internal/store/rollup`, lifecycle merge→rollup) still pass.
- [ ] Docs mention that PromQL home panels must not assume label-less `ROLLUP_STEPS` files.
- [ ] Reviewer four mandatory gates (`docs/REVIEW.md`) all checked. No merge with unchecked boxes.

### Feature-loop extras

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

## 8. Implementation notes (locked design)

### Config (`MATERIALIZATIONS_FILE`)

YAML (JSON tags for symmetry with CONTRIBUTING §3.2):

```yaml
materializations:
  - name: last_events          # required; ^[a-z][a-z0-9_]{0,63}$
    sql: |                     # required; single SELECT or WITH…SELECT
      SELECT 1 AS x FROM merge_output
    on: metrics                # metrics | logs; default metrics
    format: parquet            # parquet | duckdb; default MERGE_SEGMENT_FORMAT
    minTier: 0                 # skip when dest tier < N; default 0 (incl. L0)
```

- Unset/empty env, missing file, or `materializations: []` → no-op (byte-identical merge).
- `Validate()` names the config path (`materializations[i].name`, …).
- Forbidden SQL keywords match `/sql` sandbox (INSERT/COPY/ATTACH/…).

### Layout

`DATA_DIR/<tenant>/materializations/<name>/<ts>-<id>.parquet`

Basename equals the dest merge segment basename so retirement is a lookup.
`layout.MaterializationDir(dataDir, tenant, name)`.

### Hook

After `ExecuteMerge` dest rename + existing L1+ `BuildFromMerge` rollups:

```
materialize.Run(ctx, dest, sources, destTier, cfg)
```

Per name: if `!RunJobs` skip all; if destTier < minTier skip; if `on` mismatches
skip; compact source basenames; `COPY (sql) TO tmp` then rename. DuckDB caps =
lifecycle threads/memory. Runtime error → slog + continue. Never fail the merge.

Bind:

- `CREATE VIEW merge_output AS SELECT * FROM read_parquet('<dest>')` (or ATTACH
  for duckdb format)
- `CREATE VIEW merge_input AS <UNION ALL of sources>`

### Query

`prepareSandboxConn` creates `mat_<name>` **before** `lockSandbox`, from a
directory listing that skips `.compacted`, `_seed.parquet`, and temps. The
union SQL must not mention `tiers/` or `hot/`. Assert that in the query test
(open-set / EXPLAIN / path list).

`QUERY_HOT_ONLY` does not hide `mat_*` (own open set, not a hot substitute).
