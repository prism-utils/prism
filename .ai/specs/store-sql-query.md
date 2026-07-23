# Spec: prism-store — arbitrary read-only SQL query API (RBAC-guarded, tenant-sandboxed)

Status: ALL_OK

- **Slug / branch:** `feat/store-sql-query`
- **Owner phase:** orchestrator → developer
- **Security-critical.** Follows the RBAC feature (#50). Threat model: OWASP API1 **BOLA** (arbitrary SQL must not read another tenant's data or the host filesystem) + resource abuse. One PR.
- **Why:** the existing `GET /{ns}/query` only serves a fixed time-bucketed range query. Operators (and the benchmark) need to run **arbitrary read-only SQL** (e.g. `COUNT(*)`, `GROUP BY`, `LIKE`) over a tenant's data **through the HTTP API**, so it is subject to RBAC — not only via the in-process engine.

## 1. Task

Add an HTTP endpoint that executes **arbitrary read-only SQL** against a **single
tenant's** data, returning generic JSON rows. It must be **RBAC-guarded** (action
`query`), **tenant-sandboxed** (a caller can query only its own tenant's data and
can reach no other file on the host), **read-only**, and **resource-bounded**
(timeout + memory + row cap). The store's structured `GET /{ns}/query` is unchanged.

## 2. Design (resolved)

### Endpoint
- **`POST {ROUTE_PREFIX}/{ns}/sql`** — request body JSON `{"sql": "<single SELECT>", "max_rows": <optional int>}`.
- Response `200`: `{"columns": ["…"], "rows": [[…], …], "row_count": N, "truncated": <bool>}` (generic — arbitrary projection). `Content-Type: application/json`.
- Errors: `400 bad query` (empty SQL, parse/bind/exec error, non-SELECT, multiple statements, blocked function); `404` for unknown/unauthorized tenant (shares `tenant.UnknownTenantBody`); `500` for internal failures. Never leak sandbox paths or other-tenant names in errors.
- Route registered on the **same plane as `GET /{ns}/query`** (admin plane when split); when RBAC is on it is wrapped by the authz middleware mapping this route → **`ActionQuery`**; when RBAC is off it is gated by `ADMIN_TOKEN` exactly like the query route.
- Config flag **`SQL_API_ENABLED`** (default `true`) to allow operators to disable the surface entirely; when `false` the route is not registered (and a request would `404`).

### Tenant sandbox (the security core) — new `internal/store/query` code (e.g. `sql.go`)
Execute every request in a **dedicated, ephemeral DuckDB sandbox** that is isolated from the engine and every other tenant:

1. **Resolve + validate** `{ns}` (`ValidateTenant`); confirm the tenant root exists (else `404` with `UnknownTenantBody`). Compute the absolute, cleaned tenant root `DATA_DIR/<ns>`.
2. **Fresh hot snapshot:** ask the engine to export the tenant's current hot buffer to `<tenantRoot>/hot/current.parquet` (reuse the engine's existing snapshot export; add a small exported method if none is public) so the SQL sees committed hot + tier data at query time. Document that visibility is "committed hot (as of snapshot) + all tiers."
3. **Open an isolated in-memory DuckDB** (`:memory:`, its own `duckdb.NewConnector`) whose connection-init applies, in order, and **locks last**:
   - `SET memory_limit='<DUCKDB_MEMORY_LIMIT or default>'`
   - `SET max_temp_directory_size='0B'` (no spill-to-disk escape; raise only if needed via a documented cap)
   - `SET allowed_directories=['<absolute tenantRoot>']` — the ONLY readable location
   - `SET enable_external_access=false` — blocks `ATTACH`, `COPY` to/from files, httpfs, and `read_*` outside allowed dirs
   - `SET allow_community_extensions=false; SET autoinstall_known_extensions=false; SET autoload_known_extensions=false; SET allow_unsigned_extensions=false`
   - `SET lock_configuration=true` — **applied last**; user SQL cannot re-enable any of the above
   - ref: DuckDB "Securing DuckDB" — https://duckdb.org/docs/stable/operations_manual/securing_duckdb/overview
   - Use a single `*sql.Conn` (`db.Conn(ctx)`) for init + view + user SQL so settings apply to the exact connection running the query (avoid pool routing).
4. **Expose only tenant relations.** Create read-only view(s) over the tenant's own parquet (all under `tenantRoot`, thus permitted by `allowed_directories`), reusing the existing hot-snapshot + tier union shape (see `view.go` / `query.go`):
   - `metrics` — fixed schema `("__name__", labels, value, timestamp_ms, ts)` unioning `hot/current.parquet` (if present) + `tiers/L*/*.parquet`.
   - (Only `metrics` is required. Do NOT invent a logs relation — the store has no logs ingest.)
   - `parquet` reader must be available without autoload (DuckDB bundles it statically); verify, and if necessary `LOAD parquet` BEFORE disabling external access.
5. **Run the user SQL** with a **timeout context** (`SQL_API_TIMEOUT_SECONDS`, default 30) so go-duckdb interrupts long queries, and a **row cap** (`min(request max_rows, SQL_API_MAX_ROWS default 100000)`): read up to cap+1 to set `truncated`. Scan generically (columns + `[]any` per row) via `rows.Columns()` / `rows.Scan(into []any)`.
6. **Read-only guarantee:** the sandbox has no base tables and no external access, so `COPY TO`, `ATTACH`, `INSTALL/LOAD`, and reads outside `tenantRoot` all fail; any `CREATE`/`INSERT` a caller writes affects only the throwaway in-memory instance and is discarded. Still **reject non-SELECT** up front: accept only a single statement beginning (after trimming/comments) with `SELECT` or `WITH`; reject embedded `;` that would start a second statement. Treat this as defense-in-depth, not the primary boundary.
7. **Dispose** the sandbox instance/connection per request (or a small per-tenant pool keyed by tenant root); never share a sandbox across tenants.

### Config wiring — `cmd/prism-store`
- New env: `SQL_API_ENABLED` (default `true`), `SQL_API_MAX_ROWS` (default `100000`), `SQL_API_TIMEOUT_SECONDS` (default `30`). Reuse `DUCKDB_MEMORY_LIMIT`/`DUCKDB_THREADS` for the sandbox caps. Register the `POST /{ns}/sql` route beside the query route, wrapped identically (RBAC middleware → `ActionQuery`, else `ADMIN_TOKEN`).
- Cluster: the coordinator must route `POST /{ns}/sql` to the owning client exactly like `GET /{ns}/query` (add the method+path to the router/guard), preserving edge authorize-before-proxy + client re-enforcement. Do NOT let the coordinator hold an engine.

### Out of scope
- Writes/DDL of any kind; a logs relation; cross-tenant/federated SQL; SQL over Flight; result pagination/cursors (single capped response only); a SQL console UI.

## 3. Open questions  (resolved before READY)
- [x] Endpoint shape → `POST /{ns}/sql`, JSON in/out, generic columns/rows.
- [x] Isolation → dedicated in-memory DuckDB sandbox + `allowed_directories=[tenantRoot]` + `enable_external_access=false` + `lock_configuration=true`; read-only; timeout + row cap + memory cap.
- [x] RBAC → action `query` (reader/admin); RBAC-off → `ADMIN_TOKEN`. Cluster routes it like `/query`.
- [x] Default on? → `SQL_API_ENABLED` default `true` (first-class capability the requester asked for), operator-disableable.
- [x] Hot freshness → export a fresh hot snapshot per request; visibility = committed hot + all tiers.

## 4. Decision log  (Decision Protocol)
- **In-memory DuckDB sandbox with locked-down config for untrusted SQL.**
  - ref: DuckDB Securing guide (`enable_external_access=false`, `allowed_directories`, `lock_configuration=true`, extension knobs) — https://duckdb.org/docs/stable/operations_manual/securing_duckdb/overview ; OWASP API1 BOLA — https://owasp.org/API-Security/editions/2023/en/0xa1-broken-object-level-authorization/ .
  - perf: one snapshot export + view creation per query (measured by the benchmark); acceptable for an analytical API. product: arbitrary analytical SQL over a tenant's own data with a provable no-cross-tenant / no-host-fs guarantee that upholds the RBAC isolation promise.
- **`allowed_directories=[tenantRoot]` instead of pre-baked whitelisting only.**
  - ref: same DuckDB guide — allowed dirs are readable even with external access disabled.
  - perf: n/a. product: even if a view were mis-built, reads are hard-scoped to the tenant's own directory — the isolation boundary is enforced by the engine, not by string-building.

## 5. Acceptance checklist  (developer checks these off)
- [x] `POST /{ns}/sql` handler in `internal/store/query`: parses `{sql,max_rows}`, runs in the sandbox, returns generic `{columns,rows,row_count,truncated}`; 400/404/500 as specified; unknown/unauthorized tenant uses `tenant.UnknownTenantBody`.
- [x] Sandbox construction sets, in order, `allowed_directories=[abs tenantRoot]`, `enable_external_access=false`, extension knobs, `lock_configuration=true` (last), on a single dedicated connection; `metrics` view built from hot snapshot + tiers; fresh hot snapshot exported before the view.
- [x] **Isolation tests (critical, CGO):** with tenant A's data present alongside tenant B's under `DATA_DIR`, a request to A's `/sql` cannot read B's data — `SELECT * FROM read_parquet('<B path>')` → **error** (external access denied), `ATTACH '<B engine.duckdb>'` → error, `COPY (...) TO '<path>'` → error, `read_csv('/etc/passwd')`/`read_parquet('/etc/*')` → error, and `SET enable_external_access=true` → error (locked). `SELECT COUNT(*) FROM metrics` on A returns only A's rows.
- [x] Read-only: `INSERT`/`UPDATE`/`DELETE`/`CREATE TABLE`/`DROP`/`ATTACH`/`COPY`/`INSTALL`/`LOAD`/`PRAGMA`/`SET`(post-lock) and multi-statement input are rejected (non-SELECT) or provably cannot affect real tenant data; a `CREATE TABLE` in one request is not visible to the next (ephemeral sandbox).
- [x] Limits: query exceeding `SQL_API_TIMEOUT_SECONDS` is interrupted → error (not a hang); result beyond the row cap sets `truncated=true` and stops at the cap; `SQL_API_MAX_ROWS`/`SQL_API_TIMEOUT_SECONDS`/`SQL_API_ENABLED` honored; `SQL_API_ENABLED=false` → route absent (`404`).
- [x] RBAC integration: with RBAC on, a `reader`/`admin` for the tenant can `POST /sql`; a `writer` gets **403**; a caller not bound to the tenant gets **404**; missing/invalid JWT → **401**. With RBAC off, `ADMIN_TOKEN` gates it like `/query`. httptest-covered.
- [x] Cluster: coordinator routes `POST /{ns}/sql` to the owning client (authorize-before-proxy, no upstream on deny); client re-enforces. Test added.
- [x] Correctness: `SELECT COUNT(*) FROM metrics` and `SELECT "__name__", avg(value) FROM metrics GROUP BY "__name__"` over a seeded tenant return the same numbers as the in-process engine union (parity test vs existing query path).
- [x] `cmd/prism-store`: env wired; route registered on the correct plane; usage/`docs` updated: `docs/STORE.md` (new "Arbitrary SQL API" section — endpoint, relation+schema, sandbox guarantees, limits, RBAC action, `SQL_API_*` envs), `docs/CONFIG.md` (env rows), `docs/DESIGN.md` §15 (short ADR note w/ the refs above). If Helm chart exists, expose `SQL_API_*` (default on) values/env.
- [x] `make lint test` (`-race`) green; `go build ./cmd/prism-store` ok; **`CGO_ENABLED=0 go build ./cmd/prism` still ok and its dep graph unchanged** (feature is store-only); `make tidy` clean; no secrets/blobs committed.

## 6. Mandatory review gates  (reviewer owns) — SECURITY-CRITICAL
- [x] **Gate 1 — Guidelines:** cohesive handler + sandbox builder; single-connection config; wrapped errors; ctx-aware timeout; no globals; atomic comments (§3.8).
- [x] **Gate 2 — Edge cases:** tenant with no tiers / no hot snapshot; empty result; huge result (cap+truncate); timeout interrupt; malformed JSON; SQL referencing an unknown relation → 400; concurrent `/sql` on the same tenant (sandbox isolation, no shared-state race); snapshot export racing lifecycle flush.
- [x] **Gate 3 — Docs match code:** endpoint, JSON shape, `metrics` schema, sandbox guarantees, `SQL_API_*` env names/defaults, RBAC action, cluster routing.
- [x] **Gate 4 — Atomic comments** (§3.8).
- [x] **SECURITY AUDIT (must pass):** prove no cross-tenant read (BOLA) and no host-fs/network escape via arbitrary SQL — `enable_external_access=false` + `allowed_directories=[tenantRoot]` + `lock_configuration=true` applied on the executing connection and unbypassable by user SQL; sandbox is per-request/ephemeral and never shared across tenants; read-only holds; timeout + row + memory caps bound resource abuse; errors leak no paths/tenant names; RBAC action correctly `query`; cluster edge+client enforcement preserved. Confirm the static agent build is unaffected.
- [x] Full `docs/REVIEW.md` checklist; TESTING.md layering; TDD verified via `git log` (sandbox/isolation tests written first).

## 7. Reviewer notes

### 2026-07-23 — REQUEST CHANGES (security OK; Gate 2 + TDD)

Gate 2 edge-case tests added in `sql_test.go`. Git history rewritten: tests-only first commit, implementation second. Re-review Gate 2 + TDD gate.

### 2026-07-23 — Security hardening round (developer)

Independent review: five Medium items fixed — WITH/DML read-only gate, `MaxBytesReader` on `/sql` body, sandbox `DUCKDB_THREADS`, symlink-safe parquet materialization, extended file-builtin isolation tests. Re-review security + Gate 2.

### 2026-07-23 — APPROVED (`ALL_OK`)

Re-review after developer fixes. **All §6 gates pass.**

**TDD:** `b47ea12` is tests-only (`sql_test.go` only); compile fails with undefined `query.SQLConfig` / `SQLHandler` / `ExportHotSnapshot` (red for the right reason). `5de670a` adds implementation.

**Gate 2:** New tests pass — `TestSQLNoParquetTenant400` (via `tenantHasParquetSources` pre-check), `TestSQLEmptyResult200`, `TestSQLUnknownRelation400`, `TestSQLConcurrentSameTenantIsolated` (12 workers), `TestSQLAfterHotSnapshotAndFlush`, `TestSQLBurstQueriesAfterFlush`.

**Security (unchanged, re-confirmed):** materialize-then-lock on single `:memory:` conn — bootstrap → `CREATE TABLE metrics AS …` (tenant parquet only) → `lockSandbox` (`enable_external_access=false`, extension knobs, `lock_configuration=true` last) → user SQL. `TestSQLIsolationCrossTenant` still blocks cross-tenant/host-fs attacks and post-lock `SET enable_external_access=true`. RBAC `ActionQuery` + cluster deny-before-proxy intact.

**Commands:** `make lint` 0 issues; `make test -race` green; `go build ./cmd/prism-store` ok; `CGO_ENABLED=0 go build ./cmd/prism` ok; `go list -deps ./cmd/prism` 447 packages unchanged vs `origin/main`; `make tidy` clean; `git status` clean.

### 2026-07-23 — APPROVED hardening round (`ALL_OK` confirmed)

Re-validated commits `3ef6240` (tests-only) → `e5b44e7` (fix). **No High findings; five Medium items verified fixed.** All §6 gates remain checked; materialize-then-lock boundary, RBAC `ActionQuery`, and cluster deny-before-proxy unchanged.

**Five Medium fixes verified:**
1. **WITH/DML read-only gate** — `validateReadOnlySQL` strips comments/strings, rejects forbidden keywords, `mainQueryAfterWith` requires top-level SELECT; `TestValidateReadOnlySQL_withDMLO400`, `TestSQLWithDMLRejected400`, `TestSQLWithSelect200`.
2. **Body size cap** — `http.MaxBytesReader` on `/sql` body (`SQL_API_MAX_BODY_BYTES`, default 1 MiB); `TestSQLMaxBodyBytes400`.
3. **Sandbox threads** — `SET threads=<DUCKDB_THREADS>` in bootstrap when set; `TestApplySandboxBootstrap_appliesThreads`.
4. **Symlink-safe materialization** — `collectSafeParquetPaths` / `safeTenantParquetFile` (Lstat + EvalSymlinks + under-root); `TestSQLSymlinkParquetExcluded`.
5. **Extended file-builtin isolation** — `glob`/`parquet_metadata`/`parquet_schema`/`read_json`/`read_text` all 400 post-lockdown in `TestSQLIsolationCrossTenant`.

**TDD (hardening):** `3ef6240` adds only test files; compile fails (`undefined: sandboxLimits`). `e5b44e7` implements fixes.

**Docs/Helm:** `SQL_API_MAX_BODY_BYTES` + `DUCKDB_THREADS` (sandbox) in `docs/STORE.md` limits + `docs/CONFIG.md`; Helm `sqlAPIMaxBodyBytes` / `SQL_API_MAX_BODY_BYTES` env wired.

**Commands:** `make lint` 0 issues; `make test -race` green (all hardening tests pass); builds + deps unchanged; `make tidy` clean; `git status` clean.
