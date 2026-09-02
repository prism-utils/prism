# Spec: Materialize bindMergeViews for DuckDB merge dests

Status: IN_REVIEW

- **Slug / branch:** `cursor/materialize-duckdb-dest-1cdb`
- **Owner phase:** developer
- **PLAN phase(s):** Store merge/materialize
- **Issue:** https://github.com/prism-utils/prism/issues/166

## 1. Task

After same-type log merge (#164) writes `.duckdb` dests, `materialize.Run`
still builds `merge_output` with `read_parquet(dest)`. DuckDB files have no
parquet footer, so every logs artifact merge on prod (`user-fknjdouh-apps`)
logs `No magic bytes found at end of file` and `merge logs tenant ERROR`.
Bind dest (and sources) by payload magic: ATTACH duckdb, `read_parquet` parquet.

## 2. Scope

- **In scope:** `internal/store/materialize` bind of `merge_output` /
  `merge_input`; tests; STORE.md one-line if the materialize dest assumption
  is documented as parquet-only.
- **Out of scope:** Grafana dashboards; gitops pin; changing merge dest
  format; transcoding duckdb→parquet at materialize time.

## 3. Open questions

- [x] Q: Table name inside dest? — A: logs plane uses
  `segformat.LogsRelationForPath` (`logs` under `/tiers/`); metrics plane uses
  `segformat.MetricsTable`.
- [x] Q: Unusable dest? — A: return bind error; do not `read_parquet`.

## 4. Decision log

- Bind by payload magic, not filename extension.
  - ref: https://duckdb.org/docs/stable/sql/statements/attach.html — ATTACH
    READ_ONLY for checkpointed single-file databases.
  - perf: one ATTACH per dest/source vs a failing `read_parquet` that aborts
    the whole materialize pass.
  - product: Grafana SQL already works from existing `mat_*`; merge must stop
    erroring so summary/template materializations keep refreshing.

## 5. Acceptance checklist

- [x] DuckDB dest → ATTACH READ_ONLY + `SELECT *` from the plane table;
      parquet dest still `read_parquet`.
- [x] DuckDB sources are ATTACHed into `merge_input`, not skipped.
- [x] Unusable dest payload does not call `read_parquet`.
- [x] Tests written first (a `test:` commit precedes implementation)
- [x] `make lint test` green locally

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
