# Spec: prism-store — tiered storage engine (hot window → flush → L0, snapshot, tenant LRU)

Status: CHANGES_REQUESTED

- **Slug / branch:** `feat/store-engine`
- **Owner phase:** orchestrator → developer
- **Issue:** elk-utilities/prism#24 (Epic #21) — depends on #22 (merged); pairs with #23.

## 1. Task

Port the **per-tenant tiered DuckDB engine** — the heart of `prism-store` — from
`homelab-apps/services/prism-proxy` `internal/engine` into `internal/store/engine`.
This is the first slice that links **DuckDB via `github.com/marcboeker/go-duckdb`**
(CGO), per the #22 ADR. It gives the store a real ingest-into-hot path, the
hot→L0 flush, near-real-time hot snapshots, a bounded tenant LRU of open
DuckDB handles, and an idempotent legacy `metrics-raw` importer. No HTTP/Flight
receiver yet (that is #23); no compaction/rollups/retention (that is #25).

## 2. Scope

- **In scope** (`internal/store/engine` + a `internal/store/testparquet` test helper):
  - **`go.mod`:** add `github.com/marcboeker/go-duckdb` (v1 line, `go get …@latest`); `go mod tidy`. First CGO dependency in the module. Confirm the agent stays static: `CGO_ENABLED=0 go build ./cmd/prism` must still pass.
  - **Per-tenant DuckDB:** one `engine.duckdb` at `<DATA_DIR>/<tenant>/engine.duckdb` via `duckdb.NewConnector`+`sql.OpenDB`. Two tables `hot_current`/`hot_prev` with columns `("__name__" VARCHAR, labels VARCHAR, value DOUBLE, timestamp_ms BIGINT, ts TIMESTAMP)`, created idempotently on open. `RowGroupSize` default 1,000,000; `MaxOpenTenants` default 32 — both from `Config` (env-wired in #23/#25).
  - **Bounded LRU** of open tenant DBs (`container/list`); eviction closes the connection; guarded by a single mutex covering the LRU + the `flushAt` schedule map.
  - **`Ingest(tenant string, body io.Reader) (int64, error)`:** stream body → temp parquet; **empty body → (0, nil) no-op** (temp removed); `maybeFlushDue` first; `INSERT INTO hot_current SELECT "__name__", labels, value, timestamp_ms, CAST(? AS TIMESTAMP) AS ts FROM read_parquet('<tmp>')` where **`ts` = `clock().UTC()`** (proxy ingest time, NOT `timestamp_ms`) passed as a **bound parameter**; schedule a flush at `now + HotWindow` on the first insert into an empty schedule. Temp file always removed.
  - **Flush hot→L0 (`FlushDue()` + internal `flushTenant`):** when `clock ≥ scheduled`: `DROP hot_prev` → `RENAME hot_current → hot_prev` → recreate empty `hot_current`; if `hot_prev` empty → drop + clear schedule; else `COPY (SELECT * FROM hot_prev ORDER BY ts) TO '<tenant>/tiers/L0/<unixNano>.parquet.tmp' (FORMAT parquet, ROW_GROUP_SIZE …)` → atomic rename → drop `hot_prev` → clear schedule. `maybeFlushDue` rolls over on an ingest arriving past the deadline.
  - **Hot snapshot (`ExportHotSnapshots()` / per-tenant):** atomically write `<tenant>/hot/current.parquet` from `hot_current ORDER BY ts` (tmp+rename) so reads see in-flight rows (freshness bound ~15s, wired to a ticker in #25).
  - **Idempotent legacy import (`importLegacyMetricsRaw`):** on first tenant open, import `<tenant>/metrics-raw/*.parquet` (skip `_seed.parquet`/dotfiles) into L0 as `legacy-<nanos>.parquet` with `ts` parsed from the `metrics-raw-<nanos>-` filename; write a `.legacy-import-done` marker; skip files whose target already exists. Runs once per tenant open path.
  - **Accessors for later slices/tests:** `DB(tenant) (*sql.DB, error)` (caller must not close), `HotRowCount(tenant)`, `QueryHotTs(tenant)`, package `ListL0(dataDir, tenant)`, `Close()` (evict all). Tenant validation via `internal/store/tenant.TenantAllowed` (reject empty/leading-dot/abs/traversal) — do **not** duplicate a local validator.
  - **Atomic tmp+rename helper** used by flush, snapshot, and legacy import.
  - **`internal/store/testparquet`:** DuckDB-backed fixture builder (`Row`, `WriteFile`, `WriteWindow`, `WriteSegmentWithTs`) producing contract-v1 `metrics-raw` parquet, for engine/ingest/query tests.
  - Dirs `0750`, files `0640`.
- **Clean-ups vs upstream (required — "no macarronic code"):** use `strings.Contains` (drop the hand-rolled `contains`/`indexSubstring`); use `strings.HasPrefix`; bind the `ts` value as a SQL parameter rather than string-formatting it. `read_parquet('…')`/`COPY … TO '…'` paths stay formatted (DuckDB cannot bind file paths) — they are **server-controlled temp/tenant-scoped** paths, validated + `filepath.ToSlash`-escaped, with a short atomic comment noting the parameter-vs-path split. No `//nolint` without cause.
- **Out of scope:** HTTP/Flight ingest handler + auth (#23); compaction/rollups/retention/metering + the four tickers (#25); query builder (#26); `/admin/ensure`, `/stats`, seeds (#27). Do not wire anything into `cmd/prism-store` yet beyond what compiles.

## 3. Open questions  (resolved before READY)

- [x] Which DuckDB driver? → `github.com/marcboeker/go-duckdb` (v1 line) — the upstream driver, verified building on this toolchain (DuckDB v1.1.3).
- [x] Clock injection? → keep the engine's `now func() time.Time` (nil → `time.Now`); deterministic in tests. Don't introduce a competing clock type here.
- [x] Parameterize SQL? → yes for the `ts` value; file paths remain formatted+escaped (driver limitation) with an atomic comment.
- [x] Reuse `tenant.TenantAllowed`? → yes; delete the duplicate local validator.

## 4. Decision log  (Decision Protocol)

- **Embedded DuckDB over parquet for the hot store + tiers.**
  - ref: https://duckdb.org/docs/data/parquet/overview — native zero-copy parquet scan + `COPY … TO` with row-group control.
  - perf: columnar vectorized execution; `ORDER BY ts` COPY yields time-clustered segments that make range scans and later merges cheap.
  - product: single-file embedded engine per tenant = simple isolation, no external service.
- **`ts` = ingest time, bound as a parameter.**
  - ref: https://pkg.go.dev/database/sql#DB.Exec — parameterized args avoid formatting/injection and are the idiomatic path.
  - perf: negligible; product: correctness — reads/rollups bucket on arrival time, matching the frozen contract the consumer already relies on.
- **Bounded LRU of open DuckDB handles.**
  - ref: https://pkg.go.dev/container/list — O(1) move-to-front/evict.
  - perf: caps open file handles + DuckDB arenas under many tenants; product: predictable memory on the shared central pod.

## 5. Acceptance checklist  (developer checks these off)

- [x] `go-duckdb` added; `go mod tidy` clean; `CGO_ENABLED=0 go build ./cmd/prism` still passes; `go build ./cmd/prism-store` passes.
- [x] `Ingest`: empty body → `(0, nil)` no-op (no temp leak); non-empty inserts rows with `ts=clock()` (bound param), not `timestamp_ms` — asserted by `QueryHotTs`.
- [x] Multiple windows within one hot window accumulate in `hot_current` and produce a **single** L0 file on flush (schedule set once).
- [x] Flush: after the hot window, `hot_prev` → one sorted `tiers/L0/<nanos>.parquet`; empty `hot_prev` → no file + schedule cleared; `hot_current` recreated empty.
- [x] Hot snapshot: `hot/current.parquet` reflects in-flight `hot_current` rows, written atomically (no partial `.tmp` left).
- [x] Legacy import: `metrics-raw/*.parquet` → `L0/legacy-<nanos>.parquet` (ts from filename); `_seed.parquet`/dotfiles skipped; marker written; second open is a no-op (idempotent).
- [x] LRU: exceeding `MaxOpenTenants` evicts + closes the oldest handle; `Close()` closes all; no "database is closed"/leak under `-race`.
- [x] `testparquet` helper produces valid contract-v1 `metrics-raw` parquet used by the tests above.
- [x] Uses `strings.Contains`/`HasPrefix` and `tenant.TenantAllowed`; no hand-rolled substring/validator; file-path formatting carries an atomic explanatory comment.
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1.
- [x] `make lint test` green locally (runs with `CGO_ENABLED=1`).

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Guidelines:** factory/config pattern, no globals (the `now` closure/injected clock is fine), slog only (engine returns wrapped errors, does not log), memory discipline (temp files removed on every path incl. errors), mutex covers LRU+schedule, no goroutine leaks. `internal/store/engine` is a leaf (no import of `pipeline`).
  — `.github/workflows/ci.yml:23-26`: lint uses `golangci-lint-action` with default `CGO_ENABLED=0`; typecheck fails on `internal/store/testparquet` (go-duckdb `undefined: Conn`). Wire CI to `make lint` or set `CGO_ENABLED=1` on that step.
- [ ] **Gate 2 — Edge cases:** empty window; flush with empty hot_prev; first flush when `hot_current` absent; missing tables (`does not exist`/`Catalog Error`) treated as empty via `strings.Contains`; legacy import when `metrics-raw/` absent; concurrent ingest+flush under `-race`; LRU eviction closing an in-use handle is avoided.
  — No test for concurrent ingest+flush under `-race`; also missing tests for empty `hot_prev` flush, first flush with absent `hot_current`, and legacy import when `metrics-raw/` is absent (code paths exist, Gate 2 list requires coverage).
- [x] **Gate 3 — Docs/comments match code:** update `docs/STORE.md` engine section (hot/flush/snapshot/LRU/legacy, env knobs `HOT_WINDOW_*`, `MaxOpenTenants`, `RowGroupSize`) to exactly what landed; no forward references.
- [x] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol; the path-formatting comment is self-contained.
- [ ] Full docs/REVIEW.md checklist passes; TESTING.md layering respected (unit + golden where a parquet fixture is asserted).
  — Blocked by Gate 1 (CI lint/CGO parity) and Gate 2 (edge-case test gaps).

## 7. Reviewer notes

**REQUEST CHANGES** (2026-07-22). Scope is clean (#24 engine only): no ingest HTTP/Flight, compaction, query, or `/stats`/`/admin` creep; `cmd/prism-store` unchanged. History OK (`6f67fa5 test:` before `4ec3208 feat:`). Upstream parity holds (`strings.Contains`/`HasPrefix`, bound `ts` param, `tenant.TenantAllowed`, no duplicate validator). Local checks green: `make lint test` (0 issues; engine `-race` 1.835s), `CGO_ENABLED=0 go build ./cmd/prism`, `go build ./cmd/prism-store`, `go vet ./...` (all exit 0 on macOS default CGO). **CI parity:** `make lint`/`make test` force `CGO_ENABLED=1` (Makefile) and pass; CI **lint** job does not — it invokes `golangci-lint-action@v8` directly (`.github/workflows/ci.yml:23-26`) with CGO disabled, reproducing the `testparquet`/go-duckdb typecheck failure. Fix CI lint before merge. Gate 3/4 hold; Gate 2 needs concurrent + listed edge-case tests.
