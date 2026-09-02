# Spec: materialize merge_input UNION ALL BY NAME

Status: ALL_OK

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

- [x] **Gate 1 — Follows the guidelines**
- [x] **Gate 2 — Tests cover edge cases**
- [x] **Gate 3 — Docs & comments match**
- [x] **Gate 4 — Comments are atomic**
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

**Verdict: ALL_OK** (2026-09-02). APPROVE.

Independent Phase 2 review of `beec84c` → `63986d7` vs `origin/main` (HEAD current).

**TDD:** `beec84c test(store): materialize merge_input must UNION ALL BY NAME` (tests + spec only) precedes `63986d7 fix(store): UNION ALL BY NAME for materialize merge_input`. Conventional commits; scope `store`.

**Scope:** one join operator in `inputViewSQL` plus `docs/STORE.md`. Matches spec / prism#168. Not a PLAN.md phase-slice; bugfix is a single slice.

**Gate 1:** existing `internal/store/materialize` package; no new component, deps, globals, or panics. Matches CONTRIBUTING + DESIGN.md merge-time materializations.

**Gate 2:** `TestRunDuckDBSourcesDifferentColumnsUnionByName` ATTACHes two logs-tier duckdb files of different width and asserts `COUNT(*) = 2` (positional UNION ALL would Binder-Error). Existing dest-bind / empty / SQL-skip / unusable-dest cases remain. Mixed parquet+duckdb is out of spec test scope; BY NAME covers it.

**Gate 3:** STORE.md now says `merge_input` is UNION ALL BY NAME of sources. DESIGN.md does not name the set-op; no drift. No new code comments.

**Gate 4:** no new comments; no file/package/symbol pointers.

**REVIEW.md (N/A, not violated):** new Factory, Validate(), goroutine/goleak, encoder round-trip, hot-path bench, new deps.

**Checks (this worktree):**
- `make lint test`: golangci-lint 0 issues; `go test -race -tags duckdb_arrow ./...` all ok.
- Uncached: `TestRunDuckDBSourcesDifferentColumnsUnionByName` PASS (0.07s).
- `make full-tests`: OK (e2e ~304s). Compose `http-sink` failed to bind `18080` (kubectl port-forward on this host); existing integration tests use `httptest` and still passed. Not a product defect.
