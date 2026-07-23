# Spec: go-duckdb v2 (DuckDB ≥1.2) + zero-copy lazy-view SQL sandbox

Status: IN_REVIEW

- **Slug / branch:** `feat/duckdb-v2-sandbox`
- **Owner phase:** orchestrator → developer → reviewer + security-review
- **Security-critical** (changes the arbitrary-SQL sandbox isolation mechanism). Threat model unchanged: OWASP BOLA — arbitrary SQL must never read another tenant's data or the host filesystem.
- **Why:** the RBAC + HTTP `/sql` benchmark showed count/aggregation at ~280–300 ms and **~470–483 MiB peak RSS**. Root cause: the sandbox **materializes** the whole tenant table in memory per request (`CREATE TABLE metrics AS …`), forced by bundled DuckDB **v1.1.3** where `allowed_directories` does not exist. Upgrading go-duckdb to **v2** (DuckDB ≥1.2) lets the sandbox confine reads with `allowed_directories` and use a **lazy view** — no per-request copy → large memory + latency drop. RBAC is preserved exactly.

## 1. Task
1. Upgrade `github.com/marcboeker/go-duckdb v1.8.5` → `github.com/marcboeker/go-duckdb/v2 v2.4.3` (bundles DuckDB ≥1.2; confirm `SELECT version()` ≥ 1.2). Fix all import paths + any v1→v2 API changes. Keep the whole store engine (ingest, flush, merge, rollup, query, seed, testparquet) + bench cgo driver green.
2. Rewrite the `/{ns}/sql` sandbox to be **zero-copy**: enforce `allowed_directories=[<abs tenantRoot>]`, expose `metrics` as a **VIEW** (not a materialized TABLE) over the tenant's own parquet, then disable external access + lock configuration, then run user SQL. RBAC middleware, routes, and status semantics are untouched.

## 2. Design (resolved)

### Dependency upgrade
- `go get github.com/marcboeker/go-duckdb/v2@v2.4.3`; drop the v1 require; `go mod tidy`. Update the 8 importers: `internal/store/{engine,merge,rollup,seed,testparquet}`, `internal/store/query/sql.go`, and the two test files (`bench/internal/store/duckdb_like_test.go`, `internal/store/query/sql_bootstrap_test.go`).
- Resolve v2 API deltas (e.g. `duckdb.NewConnector`, Arrow/appender types, any renamed symbols) minimally and idiomatically. Do NOT change engine behavior/semantics beyond what the API rename requires.
- Verify CI toolchain still builds: `CGO_ENABLED=1` for store/bench (already set in `.github/workflows/ci.yml` + `Makefile`); the static agent `cmd/prism` (`CGO_ENABLED=0`) must be unaffected (no go-duckdb in its dep graph) — assert `go list -deps ./cmd/prism` unchanged.

### Sandbox rewrite — `internal/store/query/sql.go`
Replace materialize-then-lock with **allowed-dirs + lazy view** on the same dedicated per-request `:memory:` connection, single `*sql.Conn`, in order:
1. `SET threads=…` / `SET memory_limit='…'` (from config) / `SET max_temp_directory_size='0B'` / `LOAD parquet`.
2. `SET allowed_directories=['<abs tenantRoot>']` — **now REQUIRED**: on failure, return an internal error (no longer best-effort / `isUnknownConfig`-swallowed). This is the read boundary.
3. `CREATE VIEW metrics AS <union of hot snapshot + tier parquet via read_parquet>` — the existing `sandboxMetricsUnionSQL` (fixed schema `"__name__",labels,value,timestamp_ms,ts`, symlink-safe `collectSafeParquetPaths`, no `union_by_name`/`filename`). **No `CREATE TABLE` / no materialization.**
4. `SET enable_external_access=false` + extension knobs + `SET lock_configuration=true` (last).
5. Run user SQL (timeout ctx, row cap + `truncated`) against the view; scan generically.
- The lazy view's `read_parquet` targets are under `tenantRoot`, permitted by `allowed_directories` even with external access disabled. User SQL therefore reads only this tenant's files and cannot reach any other path (blocked by `enable_external_access=false` + `allowed_directories`, unbypassable due to `lock_configuration=true`).
- Keep the per-request fresh `ExportHotSnapshot`, ephemeral sandbox (never shared across tenants), read-only validation, and all error mapping / `tenant.UnknownTenantBody` behavior.
- Remove now-dead `isUnknownConfig` swallow for `allowed_directories` (keep it only if still needed elsewhere).

### Docs
- `docs/STORE.md` (Arbitrary SQL API) + `docs/DESIGN.md` §15: update the sandbox description from "materialize-then-lock (DuckDB 1.1.3, `allowed_directories` no-op)" to "**`allowed_directories` + lazy view** (DuckDB ≥1.2), zero-copy". Note the bundled DuckDB version bump. Do NOT rewrite historical `bench/RESULTS*.md`.

### Out of scope (later PRs)
- Arrow streaming query transport (next PR).
- New benchmark run (orchestrator, after transport PR).
- Any change to ingest/Flight, RBAC policy/roles, or query HTTP contract.

## 3. Open questions (resolved)
- [x] Which go-duckdb → `v2 v2.4.3` (latest v2; DuckDB ≥1.2). Confirm bundled version via `SELECT version()` in a test.
- [x] Isolation mechanism → `allowed_directories` (DuckDB-enforced) + `enable_external_access=false` + `lock_configuration=true`; lazy view, no copy.
- [x] Keep hot snapshot per request → yes (freshness).

## 4. Decision log (Decision Protocol)
- **Adopt `allowed_directories` + lazy view (requires DuckDB ≥1.2 via go-duckdb v2).**
  - ref: DuckDB Securing guide — `allowed_directories` are readable even with `enable_external_access=false`; `lock_configuration=true` makes it tamper-proof (https://duckdb.org/docs/stable/operations_manual/securing_duckdb/overview). go-duckdb v2 release line bundles DuckDB ≥1.2 (https://github.com/marcboeker/go-duckdb/releases).
  - perf/memory: eliminates the per-request full-table copy — DuckDB streams the aggregation from parquet, so peak RSS and latency for count/aggregation drop sharply. product: same airtight per-tenant isolation, now enforced by the engine's directory allowlist rather than by copying data.

## 5. Acceptance checklist (developer)
- [x] go-duckdb v2.4.3 in `go.mod`; all imports updated; `go mod tidy` clean; whole suite compiles.
- [x] A test asserts the bundled DuckDB `SELECT version()` is ≥ 1.2.
- [x] Sandbox uses `allowed_directories` (required, not best-effort) + `CREATE VIEW metrics` (no `CREATE TABLE`); external access disabled + config locked on the executing connection; view built before lock.
- [x] **Isolation tests still pass (critical):** cross-tenant `read_parquet`/`glob`/`parquet_metadata`/`parquet_schema`/`read_csv('/etc/passwd')`/`ATTACH`/`COPY TO` → 400; post-lock `SET enable_external_access=true` → error; `SELECT COUNT(*) FROM metrics` returns only this tenant's rows; symlink/out-of-root parquet excluded. Add an explicit test that user SQL `read_parquet('<abs path OUTSIDE tenantRoot>')` is denied by `allowed_directories`.
- [x] Correctness parity: COUNT(*) / GROUP BY avg over `metrics` match the engine union (existing parity tests) and the pre-upgrade JSON results.
- [x] Edge tests still green (no-parquet→400, empty result, unknown relation→400, concurrent same-tenant, burst-after-flush, timeout, row cap, `SQL_API_ENABLED=false`→404) + RBAC tests (reader 200 / writer 403 / unbound 404 / no-JWT 401) + cluster deny-before-proxy.
- [x] Whole store suite green under `-race`: `internal/store/{engine,merge,rollup,query,lifecycle,ingest,stats,cluster,...}` + e2e. Investigate any DuckDB 1.1.3→≥1.2 behavior diffs and fix in-code (no test weakening).
- [x] `go build ./cmd/prism-store ./bench/...` ok; `CGO_ENABLED=0 go build ./cmd/prism` ok + `go list -deps ./cmd/prism` unchanged; helm golden unaffected (no env change) — if `make` has a golden check, run it.
- [x] Docs updated (STORE.md, DESIGN.md) to the lazy-view sandbox + DuckDB version bump.
- [x] `make lint test` green; `git status` clean; no committed blobs/secrets.

## 6. Mandatory review gates (reviewer) — SECURITY-CRITICAL
- [ ] Gate 1 — Guidelines: minimal, idiomatic v2 migration; single-connection sandbox; wrapped errors; atomic comments (§3.8).
- [ ] Gate 2 — Edge cases: all the SQL-API edge/concurrency/timeout cases; engine/merge/rollup behavior unchanged after the DuckDB bump (spot-check tier merge, rollup math, snapshot); `allowed_directories` failure path returns an error (not fail-open).
- [ ] Gate 3 — Docs match code (sandbox mechanism + bundled version).
- [ ] Gate 4 — Atomic comments.
- [ ] **SECURITY AUDIT (must pass):** prove the lazy-view sandbox is still airtight on DuckDB ≥1.2 — `allowed_directories` confines all reads to `tenantRoot`, `enable_external_access=false` + `lock_configuration=true` applied on the executing connection and unbypassable; no cross-tenant read, no host-fs/network escape, per-request ephemeral, read-only holds; caps enforced; errors leak nothing; RBAC action still `query`; cluster edge+client enforcement intact. Confirm static agent build unaffected.
- [ ] Full `docs/REVIEW.md`; TESTING layering; TDD (`git log`) — isolation/parity tests present and green.

## 7. Reviewer notes
_(empty until first review)_

## 8. Developer notes
- go-duckdb v2 keeps `NewConnector(dsn, connInitFn)`; `nil` connInitFn still works. Import path is `/v2`; v2 pulls `duckdb-go-bindings` platform packages.
- Bundled DuckDB on this toolchain: `v1.4.1` (`TestBundledDuckDBVersionAtLeast12`).
- `allowed_directories` value must be absolute; `current_setting` returns a list type in v2 (scan as `any`).
- DuckDB ≥1.2 treats timezone-less `TIMESTAMPTZ` literals as session-local; bench harness uses explicit `Z` suffix + `TIMESTAMPTZ` comparisons (not a store-engine change).
- CI cross-platform: v2 uses per-OS `duckdb-go-bindings/*` static libs (linux/darwin amd64+arm64, windows-amd64).
