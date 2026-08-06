# Spec: Agent `.duckdb` transfer + store ingest

Status: DRAFT

- **Slug / branch:** `feat/agent-duckdb-transfer`
- **Owner phase:** orchestrator (blocked)
- **PLAN phase(s):** Agent outputs + store ingest
- **Pair:** Spec 2 of 2. **Blocked on** `feat/store-duckdb-formats` merged to
  `main`. After this merges: orchestrator cuts **`v1.9.0`**, waits for release
  CI (goreleaser → GHCR `prism` + `prism-store` images), verifies tags/images.

## 1. Task

Let the agent **encode and ship windows as native `.duckdb` files** (dir, HTTP,
and Flight) — write → checkpoint/close → transfer raw bytes — and make
**prism-store ingest** accept `.duckdb` bodies for the same artifacts. After
land, the store’s configured `HOT_SEGMENT_FORMAT` / `MERGE_SEGMENT_FORMAT`
(from Spec 1) still govern hot export and merge output. Prove Docker scenarios
with agent→store `.duckdb` paths and mixed Parquet/DuckDB configs; then
**publish `v1.9.0`**.

## 2. Scope

- **In scope:**
  - Agent: `duckdb` encoder (or format option on existing parquet encoder path)
    producing a single-table (or contract-shaped) checkpointed `.duckdb` window;
    `dir` names `… .duckdb`; `http` Content-Type for duckdb; Flight payload as
    duckdb bytes with same descriptor provenance as Parquet.
  - Store ingest: detect/accept `.duckdb` (content-type and/or magic); validate
    tenant/artifact; land into the engine/hot path; convert to configured hot
    format at hot/cache phase when needed.
  - OUTPUT_CONTRACT: `ext` may be `parquet` | `duckdb`; schemas unchanged;
    contract minor bump; version sensitivity + STORAGE_VERSION documented.
  - Pin producer STORAGE_VERSION to store-compatible version; reject
    incompatible files with a clear error.
  - Docker/e2e: agent ships duckdb → store ingest → query; also parquet agent →
    duckdb hot (conversion); matrix with Spec 1 merge formats as applicable.
  - After merge to `main`: tag `v1.9.0`, confirm GHCR images published.

- **Out of scope:**
  - Re-implementing hot/merge format knobs (Spec 1).
  - Homelab-apps chart bump (separate ops task after image exists).
  - Metric LogQL / unrelated store features.

## 3. Open questions — resolved (Phase 0 with store pair)

- [x] Q: Which outputs? — A: **dir + http + Flight**.
- [x] Q: Store ingest? — A: **Accept `.duckdb`**; primary goal is agent→store
      raw duckdb bytes.
- [x] Q: Contract? — A: Additive `ext=duckdb`; OK.
- [x] Q: Release? — A: **`v1.9.0`** after this PR on `main` (both specs done).

- [ ] Q: Exact HTTP Content-Type string and Flight data encoding framing —
      A: PENDING — resolve at Spec 2 READY time with Decision Protocol (must
      match detectability in ingest without breaking Parquet clients).

## 4. Decision log

- **Ship checkpointed `.duckdb` bytes (no re-encode on consumer ingest path
  beyond configured hot conversion):**
  - ref: https://duckdb.org/docs/current/internals/storage — native DB file is
    the unit of transfer when versions align.
  - perf: zero Parquet encode on agent when duckdb selected; store may still
    convert at hot if `HOT_SEGMENT_FORMAT` differs.
  - product: matches “ship the database” for low-CPU edge → store.

- **Producer/consumer STORAGE_VERSION must match store’s go-duckdb line:**
  - ref: https://duckdb.org/docs/current/internals/storage
  - perf: avoids runtime export/import fallback on every window.
  - product: fail fast with actionable error beats silent corruption.

_(Additional decisions for Content-Type / Flight framing logged when Spec 2
moves to READY.)_

## 5. Acceptance checklist

- [ ] Agent emits `.duckdb` via dir, http, and Flight when configured
- [ ] Store ingest accepts `.duckdb` windows; Parquet ingest unchanged
- [ ] Incompatible storage version rejected clearly
- [ ] OUTPUT_CONTRACT + CONFIG/STORE docs updated
- [ ] Docker/e2e agent→store duckdb (+ mixed format) green
- [ ] Tests written first; `make lint test` (+ full/e2e as required) green
- [ ] After merge: `v1.9.0` tagged; GHCR `prism` + `prism-store` images published

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases**
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_

## Blocker

Do **not** set `Status: READY` or open `feat/agent-duckdb-transfer` until
`store-duckdb-formats` is squash-merged to `main`.
