# Spec: Arrow IPC streaming query transport for POST /{ns}/sql

Status: READY

- **Slug / branch:** `feat/sql-arrow-transport`
- **Owner phase:** orchestrator → developer → reviewer + security-review
- **Security-sensitive** (adds a second response encoder over the same arbitrary-SQL sandbox + RBAC). Isolation/threat model is unchanged from #54; the Arrow path MUST reuse the exact same sandbox (allowed_directories + lazy view + lock) and RBAC middleware.
- **Why:** the JSON `/sql` path buffers the **entire** result set in Go (`[][]any` + `json.Marshal`) → O(rows) memory + serialization cost; the benchmark flagged API memory as too high. This adds a **content-negotiated Arrow IPC stream** that streams RecordBatches **zero-copy from DuckDB** (go-duckdb v2 Arrow interface) straight to the socket — bounded, per-batch memory and a columnar, high-throughput transport. JSON remains the default; RBAC is applied identically. Delivers the transport-layer part of #53.

## 1. Task
Add an Arrow IPC streaming response to `POST /{ns}/sql`, selected by HTTP content negotiation, streaming DuckDB Arrow RecordBatches without buffering the full result. Same sandbox + RBAC + caps + cluster routing. Thread the required `duckdb_arrow` build tag through the store build/test/lint/docker/release path (agent stays untagged + CGO-free).

## 2. Design (resolved)

### Content negotiation (`internal/store/query/sql.go`, untagged)
- Parse `Accept`. If it contains `application/vnd.apache.arrow.stream` → Arrow stream. Otherwise (incl. `application/json`, `*/*`, empty) → existing JSON path, unchanged.
- All pre-query steps are shared and identical for both encoders: tenant validation, body cap, `validateReadOnlySQL`, `resolveSandboxTenantRoot` (symlink/containment → 404), parquet probe, `ExportHotSnapshot`, sandbox open, `SET allowed_directories` (required), `CREATE VIEW metrics`, `lockSandbox`. **Do not fork the sandbox setup** — factor it so both encoders run against the same locked `*sql.Conn`.
- Dispatch to `writeArrowResponse(ctx, w, conn, userSQL, rowCap, logger)` which has two build-tagged implementations.

### Arrow encoder (`internal/store/query/sql_arrow.go`, `//go:build duckdb_arrow`)
- Enforce the row cap in-engine to keep memory bounded and detect truncation: run `SELECT * FROM (<userSQL>) AS _prism_q LIMIT <rowCap+1>` (userSQL is a single validated SELECT/WITH; DuckDB permits a CTE inside the subquery). Never fetch more than rowCap+1 rows.
- On the locked sandbox conn: `conn.Raw(func(dc driver.Conn) error { a, err := duckdb.NewArrowFromConn(dc); rr, err := a.QueryContext(ctx, wrapped); … })`.
  - If `QueryContext`/schema retrieval fails **before** any bytes are written → return the error to the handler so it maps to `400 bad query` (sandbox/exec) — status not yet sent.
  - On success: set `Content-Type: application/vnd.apache.arrow.stream`, declare `Trailer: X-Prism-Truncated`, `WriteHeader(200)`, then `ipc.NewWriter(w, ipc.WithSchema(rr.Schema()))`; loop `rr.Next()` → write each `rr.Record()`, tracking total rows; if rows would exceed `rowCap`, slice the final record to `rowCap` rows, set truncated, stop. `Flush()` the `http.Flusher` after each batch. Close the IPC writer. Set trailer `X-Prism-Truncated: true|false`.
  - Mid-stream errors (after 200) cannot change status: log and terminate the stream (client observes a short/broken IPC stream) — document this; it matches standard streaming semantics.
- Bounded memory: exactly one RecordBatch is materialized at a time; nothing accumulates.

### Arrow stub (`internal/store/query/sql_arrow_stub.go`, `//go:build !duckdb_arrow`)
- `writeArrowResponse` returns HTTP `406 Not Acceptable` ("arrow transport unavailable") so the package still builds/tests without the tag (IDE, `go build ./...`). Production/CI always build WITH the tag.

### Build-tag plumbing
- Thread `duckdb_arrow` (with CGO) through the STORE path only, via a `STORE_TAGS := duckdb_arrow` (or append to `-tags`) in `Makefile`: `test`, `lint` (`golangci-lint --build-tags duckdb_arrow`), `store-integration`/`integration` (`-tags integration,duckdb_arrow`), `e2e` (`-tags e2e,duckdb_arrow`), `bench`/`bench-api`, `docker-store`, and `.goreleaser*.y*ml` store build (`flags/tags: [duckdb_arrow]`, cgo). The agent `build` target and `CGO_ENABLED=0 go build ./cmd/prism` stay untagged.
- `.golangci.yml`: add `duckdb_arrow` to `build-tags` so the tagged file is linted.
- CI uses make targets, so it inherits the tag; confirm the `store`/`full` jobs compile the tagged package.
- Invariant: `go list -deps ./cmd/prism` unchanged (arrow only under `internal/store/**`); `CGO_ENABLED=0 go build ./cmd/prism` still succeeds.

### RBAC + cluster (unchanged, must stay enforced)
- Same `WrapSQL` → `ActionQuery`; Arrow requests authorized identically (reader 200 / writer 403 / unbound 404 / no-JWT 401). No new route.
- Cluster coordinator (`internal/store/cluster/router.go`): the reverse proxy MUST stream Arrow (set `proxy.FlushInterval = -1`, or per-request flushing) and forward `Accept` (default). RBAC deny-before-proxy + client `OwnedTenantGuard` unchanged. Add/keep a test that an Arrow `/sql` request is denied at the coordinator before proxying for an unauthorized tenant.

### Client helper for the benchmark
- Add a small reusable Arrow-stream client decoder (e.g. `bench/internal/store` method `CountMetricsArrowAPI` / `AggregateMetricsArrowAPI` + a `ScanMetricsArrowAPI` returning row count) using `ipc.NewReader`. Bounded decode (read batches, don't retain all). The actual benchmark run + results are a later orchestrator step; here just land the client + a test proving it round-trips against a live tenant.

### Docs
- `docs/STORE.md` (Arbitrary SQL API): document the `Accept: application/vnd.apache.arrow.stream` content negotiation, the Arrow IPC stream + `X-Prism-Truncated` trailer, RBAC parity, and the `duckdb_arrow` build requirement.
- `docs/CONFIG.md` / `docs/MIGRATION.md`: note the build tag (ops who build from source). `docs/DESIGN.md` §15: add the Arrow transport to the query surface.

### Out of scope
- Flight SQL server (separate). Changing the JSON contract. Ingest transport. New benchmark results (final orchestrator step). Non-`metrics` schemas.

## 3. Open questions (resolved)
- [x] Transport → **Arrow IPC stream over HTTP**, content-negotiated (reuses RBAC/cluster/HTTP stack) rather than a separate Flight server.
- [x] Zero-copy source → **go-duckdb v2 Arrow interface** (`duckdb_arrow` tag), confirmed to compile here.
- [x] Row cap / truncation → engine-side `LIMIT rowCap+1` + `X-Prism-Truncated` trailer.
- [x] Build without the tag → stub returns 406 so `go build ./...` still works.

## 4. Decision log (Decision Protocol)
- **Arrow IPC streaming via go-duckdb v2 Arrow interface, content-negotiated on the existing endpoint.**
  - ref: Arrow IPC streaming format (https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format); go-duckdb v2 Arrow interface behind `duckdb_arrow` build tag (https://github.com/marcboeker/go-duckdb) — verified `go build -tags duckdb_arrow` links in this repo.
  - perf/memory: streams one RecordBatch at a time zero-copy from DuckDB → bounded RSS + columnar throughput vs the O(rows) JSON buffer. product: opt-in via `Accept`, JSON default preserved, RBAC + tenant isolation identical, so no client breakage and no security regression.

## 5. Acceptance checklist (developer)
- [ ] `POST /{ns}/sql` with `Accept: application/vnd.apache.arrow.stream` returns a valid Arrow IPC stream (`Content-Type` set); JSON remains default for other/absent Accept.
- [ ] Sandbox setup is shared (single code path) between JSON and Arrow; Arrow runs against the SAME locked conn (allowed_directories + lazy view + external-access off + lock).
- [ ] Streaming is bounded: one RecordBatch materialized at a time; row cap enforced via `LIMIT rowCap+1`; `X-Prism-Truncated` trailer set correctly (false when under cap, true when exceeded); final record sliced to rowCap.
- [ ] Pre-stream errors map correctly (empty/invalid/non-SELECT → 400; unknown/symlinked tenant → 404; sandbox exec error before bytes → 400); disabled API → 404; body cap honored.
- [ ] **TDD tests (tag `duckdb_arrow`):** Arrow↔JSON parity for COUNT(*), GROUP BY avg, and a multi-row scan (decode Arrow with `ipc.Reader`, compare values); empty result → schema-only stream; scan over cap → truncated trailer + exactly rowCap rows; RBAC on Arrow (reader 200 / writer 403 / unbound 404 / no-JWT 401); isolation on Arrow (cross-tenant `read_parquet` → 400, no stream); cluster deny-before-proxy for Arrow.
- [ ] Stub path (`!duckdb_arrow`): `writeArrowResponse` → 406; package builds & base tests pass without the tag.
- [ ] Build plumbing: `make test`/`lint`/`store-integration`/`integration`/`e2e`/`bench*`/`docker-store` and goreleaser carry `duckdb_arrow` (CGO on); `.golangci.yml` build-tags updated; `CGO_ENABLED=0 go build ./cmd/prism` ok and `go list -deps ./cmd/prism` unchanged (no arrow/duckdb).
- [ ] Bench Arrow client decoder added + test (round-trips against a live tenant).
- [ ] Docs updated (STORE.md, CONFIG.md, MIGRATION.md, DESIGN.md §15).
- [ ] `make lint test` green (with tag); e2e/integration green; `git status` clean; TDD history (tests-first).

## 6. Mandatory review gates (reviewer) — SECURITY-SENSITIVE
- [ ] Gate 1 — Guidelines: shared sandbox path (no duplicated/weakened setup for Arrow); idiomatic build tags + stub; wrapped errors; atomic comments §3.8.
- [ ] Gate 2 — Edge cases: truncation boundary (exactly cap / cap+1), empty result, mid-stream error semantics, flusher present, timeout ctx honored during streaming, body/row caps.
- [ ] Gate 3 — Docs match code (content negotiation, trailer, build tag).
- [ ] Gate 4 — Atomic comments.
- [ ] **SECURITY AUDIT:** Arrow path reuses the identical locked sandbox + RBAC (`ActionQuery`) — no cross-tenant/host-fs escape via the Arrow/`conn.Raw` route; the `LIMIT` wrapper can't be used to break out (it wraps the validated SELECT only); no info leak in errors/trailers; cluster deny-before-proxy holds for Arrow; static agent build unaffected.
- [ ] Full `docs/REVIEW.md`; TESTING layering; TDD (`git log`).

## 7. Reviewer notes
_(empty until first review)_
