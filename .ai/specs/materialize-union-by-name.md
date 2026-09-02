# Spec: materialize merge_input UNION ALL BY NAME

Status: IN_REVIEW

- **Slug / branch:** `cursor/materialize-union-by-name-1cdb`
- **Owner phase:** developer
- **Issue:** https://github.com/prism-utils/prism/issues/168

## 1. Task

v1.0.18 ATTACHes duckdb sources into `merge_input` then joins them with
plain `UNION ALL`. Log files do not share a column count, so prod merge
ticks fail with Binder Error and skip dest materializations. Use
`UNION ALL BY NAME` (same as log merge export).

## 2. Scope

- **In scope:** `inputViewSQL` join operator; tests with two duckdb sources
  of different width.
- **Out of scope:** Grafana; changing dest ATTACH; transcoding schemas.

## 3. Open questions

- [x] Q: Parquet + duckdb mixed? — A: BY NAME covers that too.

## 4. Decision log

- UNION ALL BY NAME, not projecting a shared column list.
  - ref: https://duckdb.org/docs/stable/sql/query_syntax/setops.html — BY NAME
    aligns columns by name and fills missing with NULL.
  - perf: one view bind vs failing the whole materialize pass.
  - product: dest-only mat items must still run after a heterogeneous pack.

## 5. Acceptance checklist

- [x] `merge_input` joins with `UNION ALL BY NAME`.
- [x] Two duckdb sources with different column counts bind and count as 2.
- [x] Tests written first (a `test:` commit precedes implementation)
- [x] `make lint test` green locally

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases**
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
