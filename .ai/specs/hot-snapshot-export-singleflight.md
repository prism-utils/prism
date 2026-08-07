# Spec: Serialize per-tenant hot snapshot exports (prod attach conflict)

Status: ALL_OK

- **Slug / branch:** `fix/hot-snapshot-export-singleflight`
- **Owner phase:** developer (urgent prod fix; orchestrator process compressed)
- **PLAN phase(s):** Store query / engine hardening

## 1. Task

Grafana dashboards firing several concurrent PromQL queries against one
prism-store tenant fail with `Unique file handle conflict: Cannot attach "exp" -
the database file ".../hot/current.duckdb.tmp" is already attached by database
"exp"`. Every PromQL (and `/sql`) request exports a fresh hot snapshot before
opening its sandbox, and the DuckDB export path takes only a shared read lock,
so two requests for the same tenant run `ATTACH '<final>.tmp' AS exp` on the
same tenant connection at the same time. The fixed alias and fixed temp path
make that a hard conflict and the whole query 500s. Serialize per-tenant hot
snapshot exports so concurrent requests share one export instead of racing.

## 2. Scope

- **In scope:**
  - `internal/store/engine/snapshot.go` — per-tenant export serialization
    (singleflight), exclusive tenant lock around the export
  - `internal/store/segformat/export.go` — unique temp path per export attempt
    (defense in depth)
  - `internal/store/engine` regression test for concurrent DuckDB-format exports
  - `docs/STORE.md` — document the serialized/shared export
- **Out of scope:**
  - Caching or rate-limiting snapshots across requests (freshness semantics stay
    "every request sees a snapshot at least as new as its arrival")
  - Changing the per-request export pattern in PromQL / `/sql` handlers
  - The Parquet export path's behavior (already collision-safe; it inherits the
    same serialization)

## 3. Open questions

- [x] Q: Singleflight (share one in-flight export) or a plain per-tenant mutex
  (every caller re-exports in turn)? — A: Singleflight. Callers only need a
  snapshot that is current as of their arrival; a concurrent export that started
  after the caller arrived satisfies that, and N dashboards panels then cost one
  export instead of N serialized ones. `golang.org/x/sync` is already a direct
  dependency.
- [x] Q: Keep `te.mu.RLock()` for the export? — A: No. ATTACH/DETACH mutate the
  connection catalog, which is a write, so the export takes the exclusive lock
  like every other catalog-mutating operation.
- [x] Q: Also make the temp path unique? — A: Yes, cheap defense in depth so an
  out-of-process or future concurrent exporter cannot clobber a partial file.

## 4. Decision log

- Per-tenant `singleflight.Group` keyed by tenant:
  - ref: https://pkg.go.dev/golang.org/x/sync/singleflight — duplicate function
    calls for the same key share one execution and its result
  - perf: a 12-panel dashboard refresh does one export instead of 12; the export
    is a full copy of `hot_current`, so this is the dominant per-request cost
  - product: removes the 500s without adding staleness a dashboard can observe —
    a shared export still starts at or after the waiter's request
- Exclusive `te.mu.Lock()` for the export instead of `RLock`:
  - ref: https://duckdb.org/docs/stable/sql/statements/attach — ATTACH adds a
    database to the connection's catalog, and an alias may be attached once
  - perf: exports already serialize on the tenant's single connection, so the
    stronger lock costs nothing beyond what DuckDB enforces anyway
  - product: makes the invariant explicit rather than relying on callers
- Unique temp file per export attempt:
  - ref: https://duckdb.org/docs/stable/sql/statements/attach — the file path
    identifies the attached database, so two attempts must not share one path
  - perf: one extra 4-byte random suffix per export
  - product: matches how the Parquet export already avoids clobbering

## 5. Acceptance checklist

- [x] Regression test: many concurrent `ExportHotSnapshot` calls on one tenant
      with `HOT_SEGMENT_FORMAT=duckdb` all succeed (no attach/unique-file-handle
      error), snapshot is readable, no `.tmp` leftovers
- [x] `ExportHotSnapshot` serializes per tenant and waiters reuse the result
- [x] Export holds the tenant lock exclusively
- [x] `AtomicExportDuckDB` uses a per-attempt temp path
- [x] `/sql` exports are covered by the same serialization (same entry point)
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally

## 6. Mandatory review gates

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [x] **Gate 3 — Docs & comments match the task and the delivered code**
- [x] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

- TDD history confirmed: `aebe070 test(store/engine): …` → `3d9494a
  test(store/query): …` → `4a8a1b5 fix(store/engine): …`. Both tests were run
  against the pre-fix tree and failed for the right reason (engine: `Unique file
  handle conflict: Cannot attach "exp"`; handler: HTTP 500 `query failed`).
- `make lint` (0 issues) and `make test` (race) green locally. No I/O format or
  wiring change, so `make full-tests` was not required.
- Freshness trade-off is recorded on `ExportHotSnapshot` and accepted: a waiter
  joins an export that may have begun up to one export duration before it
  arrived, well inside the 15s snapshot ticker bound.
