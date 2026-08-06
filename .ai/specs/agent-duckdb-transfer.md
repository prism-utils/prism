# Spec: Agent `.duckdb` transfer + store ingest

Status: IN_REVIEW

- **Slug / branch:** `feat/agent-duckdb-transfer`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Agent outputs + store ingest
- **Pair:** Spec 2 of 2 (Spec 1 `store-duckdb-formats` merged as #92). After this
  merges to `main`, **orchestrator** cuts **`v1.9.0`** and verifies GHCR
  `prism` + `prism-store` images (developer does not tag).

## 1. Task

Let the agent **encode and ship windows as native `.duckdb` files** (dir, HTTP,
and Flight) — write → checkpoint/close → transfer raw bytes — and make
**prism-store ingest** accept `.duckdb` bodies for the same artifacts. After
land, the store’s configured `HOT_SEGMENT_FORMAT` / `MERGE_SEGMENT_FORMAT`
(from Spec 1) still govern hot export and merge output. Prove Docker/e2e
agent→store `.duckdb` (+ mixed Parquet/DuckDB hot/merge) green.

## 2. Scope

- **In scope:**
  - Agent `duckdb` encoder: RecordBatch → single-table checkpointed `.duckdb`
    bytes (`EncodedBlock.Format = "duckdb"`); pin `STORAGE_VERSION` to store’s
    go-duckdb-compatible version (same default as Spec 1 / `DUCKDB_STORAGE_VERSION`).
  - `dir` output: filename `… .duckdb` when block format is duckdb.
  - `http` output: Content-Type **`application/vnd.duckdb`** when shipping duckdb
    (configurable override still allowed).
  - `flight` output: when block format is `duckdb`, DoPut sends **raw duckdb
    file bytes** (not Arrow IPC); descriptor path remains
    `[pipeline, branch, start, end]` and **app metadata / trailing path segment
    carries `format=duckdb`** so the receiver can branch. Existing Arrow IPC
    Flight path unchanged for `arrow` encoder.
  - Store HTTP ingest: accept duckdb via Content-Type `application/vnd.duckdb`
    **or** `application/octet-stream` / empty CT when body magic matches DuckDB
    (`DUCK` at header magic offset). Parquet (`PAR1` / existing path) unchanged.
  - Store Flight ingest: if descriptor marks `format=duckdb`, land raw bytes;
    else keep Arrow→Parquet (current) behavior.
  - Land → engine/hot path; hot/merge conversion still follows Spec 1 knobs.
  - Reject incompatible storage version with a clear 4xx/error.
  - OUTPUT_CONTRACT v1.1: `ext` may be `parquet` | `duckdb`; document CT + magic
    + Flight framing + version pin.
  - Docker/e2e: agent→store duckdb ingest→query; at least one mixed hot/merge
    combo with duckdb agent payload.
  - Docs: CONFIG (encoder), STORE ingest, OUTPUT_CONTRACT, TESTING.

- **Out of scope:**
  - Changing Spec 1 hot/merge knobs.
  - Homelab-apps chart bump.
  - Developer cutting the SemVer tag (orchestrator after merge).

## 3. Open questions — resolved

- [x] Q: Which outputs? — A: **dir + http + Flight**.
- [x] Q: Store ingest? — A: **Accept `.duckdb`**.
- [x] Q: Contract? — A: Additive `ext=duckdb`.
- [x] Q: Release? — A: **`v1.9.0`** by orchestrator after this PR merges.
- [x] Q: HTTP Content-Type + Flight framing? — A: CT
      **`application/vnd.duckdb`**; ingest also sniffs DuckDB magic when CT is
      octet-stream/empty. Flight: raw duckdb bytes when `format=duckdb` in
      descriptor metadata; Arrow IPC path unchanged otherwise.

## 4. Decision log

- **Ship checkpointed `.duckdb` bytes:**
  - ref: https://duckdb.org/docs/current/internals/storage
  - perf: no Parquet encode on agent when duckdb selected.
  - product: “ship the database” for edge→store.

- **Pin STORAGE_VERSION to store go-duckdb line:**
  - ref: https://duckdb.org/docs/current/internals/storage
  - perf: fail fast vs per-window export/import.
  - product: clear incompatibility errors.

- **Content-Type `application/vnd.duckdb` + magic sniff fallback:**
  - ref: DuckDB header magic `DUCK` —
    https://duckdb.org/docs/current/internals/storage ; no IANA-registered
    duckdb MIME — vendors use explicit vnd types or octet-stream.
  - perf: one header/magic check; no full parse to classify.
  - product: explicit CT for agents; octet-stream + magic keeps curl/tools
    working without breaking Parquet (`PAR1`) clients.

- **Flight: opaque duckdb DoPut when format=duckdb; Arrow path otherwise:**
  - ref: existing prism Flight = Arrow IPC for columnar ingest
    (`internal/output/flight`); DuckDB file is not Arrow IPC.
  - perf: avoids Arrow→Parquet→DuckDB round-trip when agent already sealed
    duckdb.
  - product: one Flight server, two payload modes selected by descriptor
    metadata — Parquet/Arrow clients unchanged.

## 5. Acceptance checklist

- [x] Agent `duckdb` encoder + dir/http/Flight emit when configured
- [x] Store HTTP + Flight ingest accept `.duckdb`; Parquet/Arrow paths unchanged
- [x] Incompatible storage version rejected clearly
- [x] OUTPUT_CONTRACT + CONFIG/STORE/TESTING docs updated
- [x] Docker/e2e agent→store duckdb (+ mixed format) green
- [x] Tests written first (`test:` commit precedes implementation)
- [x] `make lint test` green (+ full-tests / e2e as required)
- [x] Spec notes release is orchestrator-owned after merge (no tag in this PR)

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases**
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

Developer: implementation complete (`make lint test` green). E2e via
`make agent-duckdb-e2e` (compose `deploy/docker-compose.agent-duckdb-e2e.yml`).
Do **not** tag `v1.9.0` in this PR — orchestrator after merge.
