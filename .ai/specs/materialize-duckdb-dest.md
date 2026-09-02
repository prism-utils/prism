# Spec: Materialize bindMergeViews for DuckDB merge dests

Status: ALL_OK

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

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [x] **Gate 3 — Docs & comments match the task and the delivered code**
- [x] **Gate 4 — Comments are atomic**
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

**Verdict: ALL_OK** — no unchecked items.

TDD history (`git log origin/main..HEAD`, oldest first):
- `37990c6` `test(store): materialize duckdb dest must ATTACH not read_parquet` — tests + spec only (`builder_test.go`, `.ai/specs/…`)
- `40b71e6` `fix(store): ATTACH duckdb dests when binding materialize merge_output` — `builder.go` + STORE.md
- `fa2204b` `docs(spec): mark materialize-duckdb-dest IN_REVIEW`

Branch is not behind `origin/main`.

Checks re-run by reviewer (not trusted from developer):
- `make lint test` — golangci-lint 0 issues; `go test -race -tags duckdb_arrow ./...` ok
- `CGO_ENABLED=1 go test -count=1 -race -tags duckdb_arrow ./internal/store/materialize/` — ok (1.349s)
- `make full-tests` not run: no new HTTP/encoding/wiring; DuckDB ATTACH/COPY I/O is exercised in-process by the unit tests.

### prism review — ATTACH duckdb dests when binding materialize merge_output

**Scope & TDD**
- [x] Scope matches a single PLAN.md phase-slice (store merge/materialize bind; no format change / grafana / gitops).
- [x] History shows a `test:` commit BEFORE implementation commits.
- [x] Tests describe behavior clearly (duckdb dest Run, duckdb source in merge_input, unusable dest rejects).
- [x] Conventional Commit messages; `store` scope; hooks implied green (lint/test pass).

**Architecture & patterns**
- [x] Not a new component; change stays in `internal/store/materialize`. Bind-by-magic matches query/merge Payload sniffing.
- [x] No new import of `pipeline` or a sibling component. `segformat` is a shared lib.
- [x] No new init()/global state.

**Config / Errors & lifecycle / Memory**
- [x] No config surface change. ATTACH dest errors wrap as `materialize: merge_output`; unusable dest does not `read_parquet`. Source ATTACH skip-on-fail matches existing query-plane ATTACH.
- [x] `context.Context` first arg; no new goroutines; no unbounded buffers; one ATTACH per dest/source.

**Tests (TESTING.md)**
- [x] No `time.Sleep`. Failure path: junk dest → bind error. Happy path: parquet dest still covered by existing `mergeFixture` tests; duckdb dest/source added.
- [x] `make test` (with -race) green.

**Dependencies & build**
- [x] No new dep. CGO already required for store DuckDB tests.

**Observability & docs (gates 3 & 4)**
- [x] No new slog/counters needed.
- [x] STORE.md records payload-magic bind; DESIGN.md materialize note does not claim parquet-only dests.
- [x] Comments atomic — only `//nolint:gosec` reasons on the new SQL strings; no file/package/symbol pointers.

**Verdict**
- [x] APPROVE
