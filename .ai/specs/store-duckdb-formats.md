# Spec: Store configurable hot + merge formats (Parquet | DuckDB)

Status: ALL_OK

- **Slug / branch:** `feat/store-duckdb-formats`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Store / query + lifecycle
- **Pair:** Spec 1 of 2. Follow-up: `.ai/specs/agent-duckdb-transfer.md` (agent emit + store ingest of `.duckdb`). **Release/tag (`v1.9.0`) waits until both land on `main`.**

## 1. Task

Make prism-store’s **hot snapshot** and **merge/cold segment** on-disk formats
configurable as `parquet` (today’s default) or `duckdb`, for **metrics and
logs**. Ingest may still arrive as Parquet in this cut; conversion to the
configured hot format happens in the **hot/cache phase**. Query planners must
open whichever formats are present (ATTACH for `.duckdb`, existing
`read_parquet` / list open-set for Parquet), optionally using Hive partitioning
and/or ATTACH where it improves prune/perf. Prove a **Docker matrix** of format
configs (hot × merge, metrics + logs) with ingest → merge → query green. Docs +
contract note for format/version upgrade (convert prior `.duckdb` segments to
Parquet before upgrading DuckDB storage). Do **not** tag a release in this PR.

## 2. Scope

- **In scope:**
  - Config knobs (env, documented in `CONFIG.md` / `STORE.md`):
    - `HOT_SEGMENT_FORMAT` = `parquet` | `duckdb` (default **`parquet`**)
    - `MERGE_SEGMENT_FORMAT` = `parquet` | `duckdb` (default **`parquet`**)
    - `DUCKDB_STORAGE_VERSION` (or equivalent) — pin created `.duckdb` files to a
      declared storage compatibility version matching the bundled go-duckdb
  - Hot snapshot path: today `hot/current.parquet`; when hot format is `duckdb`,
    write checkpointed `hot/current.duckdb` (atomic replace; no orphan `.wal`
    after successful export). Live tenant `engine.duckdb` / `hot_current` table
    stays; this is the **export** format for sandboxes/replicas, not a removal
    of the engine.
  - Flush → L0 and logs tier merge: emit segments in `MERGE_SEGMENT_FORMAT`
    (`.parquet` or `.duckdb`). Default remains Parquet.
  - Logs: same knobs apply to logs hot (introduce logs hot export when
    `HOT_SEGMENT_FORMAT=duckdb`, or shared hot layout as designed) and logs
    tier merges when `MERGE_SEGMENT_FORMAT=duckdb`.
  - Query / file picker: detect segment extension; **ATTACH** (read-only)
    `.duckdb` sources; keep list/`read_parquet` for Parquet. Prefer Hive-style
    layout or ATTACH strategies when they reduce open-set / planning cost
    without breaking tenant isolation (`allowed_directories`, sandbox lock).
  - Mixed trees: tenants may temporarily contain both extensions during
    config flips; planners must union correctly.
  - **DuckDB version upgrade procedure (docs + test hook):** before upgrading
    the store’s DuckDB major/storage version, operators must set merge/hot to
    Parquet (or run a convert job) so **all prior `.duckdb` segments become
    Parquet** — avoids storage-format breakage. Document clearly; add a
    conversion helper or admin path if needed for the test.
  - OUTPUT_CONTRACT / STORE docs: additive `ext=duckdb` note; contract minor
    bump narrative (v1 → v1.1 or documented additive) — full agent emit is
    Spec 2.
  - Docker / compose matrix tests covering at least:
    1. hot=parquet, merge=parquet (baseline)
    2. hot=duckdb, merge=parquet
    3. hot=parquet, merge=duckdb
    4. hot=duckdb, merge=duckdb  
    Each: metrics + logs path, query (`/sql` and Loki or PromQL as applicable)
    succeeds against the resulting tree.
  - Unit/integration tests first; `make lint test` + relevant e2e/docker
    targets green.

- **Out of scope:**
  - Agent encoder / `dir`/`http`/Flight emitting `.duckdb` (Spec 2).
  - HTTP ingest accepting `.duckdb` bodies (Spec 2) — this PR may still
    ingest Parquet only and convert at hot export / merge.
  - Changing default formats away from Parquet.
  - Replacing live `engine.duckdb` with “hot file only.”
  - Homelab-apps chart bumps / gitops pin (after release).
  - Cutting `v1.9.0` / GHCR publish (orchestrator after Spec 2 merges).

## 3. Open questions — resolved

- [x] Q: Hot replace vs option? — A: **Option** (`HOT_SEGMENT_FORMAT`); default
      parquet; duckdb export when configured; convert at hot/cache regardless of
      inbound format.
- [x] Q: Cold/merge format? — A: **`MERGE_SEGMENT_FORMAT`**; default parquet;
      duckdb allowed; upgrade path converts old duckdb → parquet.
- [x] Q: Metrics only or logs too? — A: **Both**.
- [x] Q: One PR or two? — A: **This is store-only PR**; agent+ingest is Spec 2.
- [x] Q: Release in this PR? — A: **No** — tag after Spec 2 on `main`.
- [x] Q: Query improvements? — A: Keep file picker; apply **ATTACH** and/or
      **Hive partitioning** where beneficial.

## 4. Decision log

- **Configurable hot + merge formats (default parquet):**
  - ref: https://duckdb.org/docs/current/internals/storage — `.duckdb` storage
    is version-sensitive; Parquet remains the portable cold default.
  - perf: duckdb hot avoids repeated Parquet encode/decode for sandbox reads;
    Parquet cold stays smaller for transfer/retention.
  - product: operators opt in; existing deployments unchanged.

- **Hot export = checkpointed single-file `.duckdb` (ATTACH), not live engine
  share for sandboxes:**
  - ref: https://duckdb.org/docs/current/sql/statements/attach.html — ATTACH
    opens an external database; RO replica mounts need a flushed file, same as
    today’s `hot/current.parquet`.
  - perf: ATTACH preserves DuckDB statistics/layout; still isolates per-request
    sandboxes from writer locks.
  - product: preserves `QUERY_HOT_ONLY` + reader-replica topology.

- **Strict close/checkpoint before publish of any `.duckdb` artifact:**
  - ref: DuckDB storage / backup practice — copy only after checkpoint so no
    dependent `.wal` is required for a consistent open.
  - perf: one checkpoint cost at export/merge seal; avoids corrupt opens.
  - product: “ship the database” only when the file is self-contained.

- **Pin `STORAGE_VERSION` on create; upgrade path = rewrite to Parquet:**
  - ref: https://duckdb.org/docs/current/internals/storage — `STORAGE_VERSION`
    / compatibility; newer DuckDB may not read newer-than-supported files.
  - perf: conversion is offline/ops cost, not per-query.
  - product: clear operator rule prevents silent breakage on store upgrades.

- **Query: ATTACH duckdb segments + keep Parquet open-set; Hive when layout
  supports prune:**
  - ref: https://duckdb.org/docs/current/data/partitioning/hive_partitioning —
    hive keys enable directory prune before open.
  - perf: fewer opens / less planning than a flat bag of files.
  - product: file picker remains the source of truth; formats are pluggable.

- **Docker matrix of hot×merge configs is acceptance, not optional:**
  - ref: existing `make loki-e2e` / `promql-e2e` / store integration pattern in
    `docs/TESTING.md`.
  - perf: catches ATTACH vs parquet planner regressions under real mounts.
  - product: format knobs are useless if only unit-tested in isolation.

## 5. Acceptance checklist

- [x] `HOT_SEGMENT_FORMAT` / `MERGE_SEGMENT_FORMAT` env wired (default parquet);
      invalid values rejected at startup
- [x] Hot snapshot writes `hot/current.duckdb` when configured; atomic,
      checkpointed, no required sibling `.wal` for open
- [x] Metrics flush/L0 and logs tier merge emit `.duckdb` when
      `MERGE_SEGMENT_FORMAT=duckdb`, else `.parquet`
- [x] Query sandboxes (metrics + logs / Loki as applicable) open mixed trees;
      ATTACH for duckdb, `read_parquet` for parquet; tenant isolation unchanged
- [x] Docs: CONFIG.md + STORE.md format knobs; DuckDB upgrade → convert
      segments to Parquet procedure; OUTPUT_CONTRACT additive note for `ext`
- [x] Conversion/upgrade helper or documented admin path covered by a test
- [x] Docker matrix: four hot×merge combos; metrics + logs; queries pass
- [x] Tests written first (`test:` commit precedes implementation)
- [x] `make lint test` green; docker/e2e matrix target(s) green
- [x] No SemVer tag in this PR

## 6. Mandatory review gates

- [x] **Gate 1 — Follows the guidelines**
  - Independent re-check: DESIGN.md § Arbitrary SQL sandbox documents Parquet `read_parquet` + `.duckdb` ATTACH; mixed trees; matches delivered sandbox behavior.
- [x] **Gate 2 — Tests cover edge cases**
- [x] **Gate 3 — Docs & comments match**
  - Independent re-check of `a63174f`: `SQLConfig.RunJobs` field comment says immutable parquet|duckdb segments and matches the handler block (`hot/current.{parquet|duckdb}`, ATTACH).
- [x] **Gate 4 — Comments are atomic**
  - Independent re-check: `sandboxMetricsUnionSQL` / `sandboxLogsRelationSQL` / handler prose have no cross-symbol refs; `921cc4e` fixes still present.
- [x] Full docs/REVIEW.md checklist passes
  - Re-verify on `cf2f6bf`: `make lint test` green (0 issues); Makefile CGO override only; gates 1–4 still hold.

## 7. Reviewer notes

- **ALL_OK** on `cf2f6bf`: Makefile fix is correct and minimal — `CGO_ENABLED=1` on `e2e` / `promql-e2e` / `loki-e2e` only (matches `format-matrix-e2e` / `make test`). Reproduced: `CGO_ENABLED=0` → `undefined: bindings.Type`; `CGO_ENABLED=1` → e2e package compiles. `make lint test` green (0 issues). Gates 1–4 unchanged (no product/docs/comment drift). TDD (`eb99e2b` before feat); no SemVer tag.
- Optional follow-up (non-blocking): unit-test empty `DUCKDB_STORAGE_VERSION` rejection; matrix e2e only asserts `/sql` counts, not on-disk extensions.
