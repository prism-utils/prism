# Spec: benchmark over the HTTP SQL API with RBAC on (attached profile)

Status: READY

- **Slug / branch:** `chore/rbac-docs-bench` (docs already committed on this branch)
- **Owner phase:** orchestrator → developer (harness) → orchestrator runs the actual benchmark
- **Why:** the store now has an RBAC-guarded arbitrary SQL API (#51). Add a benchmark
  **profile** that drives the store's query workloads **through the HTTP SQL API with
  RBAC enabled**, and **attach** the results beside the existing baseline (do NOT
  replace baseline artifacts). Requester: "run the benchmark on top of the API."

## 1. Task
Add an opt-in `--api` profile to `bench/cmd/prism-bench` that:
- boots `prism-store` with **RBAC on** (JWT/OIDC via `OIDC_JWKS_FILE` + a policy file granting the bench principal `admin` on the bench tenant),
- runs **ingest over HTTP with a Bearer JWT**, and **count + aggregation over `POST {ROUTE_PREFIX}/{ns}/sql`** with the Bearer JWT,
- writes results to **profile-suffixed files** so baseline outputs are untouched,
- keeps ClickHouse as the comparison (native client), with an honest caveat about the protocol/enforcement asymmetry.

The developer builds + unit/integration-tests the harness (NO Docker run). The orchestrator runs `make bench-api` afterward to produce and commit the artifacts.

## 2. Design (resolved)

### JWT/JWKS minter — `bench/internal/authgen` (new, non-test)
- Generate an RSA key at runtime; build a **JWKS JSON file** (with `kid`, `use:sig`, `alg:RS256`) using `gopkg.in/go-jose/go-jose` (already a dep via `internal/store/auth`). Write it to the work dir.
- Mint a signed **RS256 JWT** with `iss`, `aud`, `sub` (the bench principal, e.g. `bench-admin`), and a future `exp`. Provide `Token()`; support a short-expiry helper for a negative test.
- Write a **policy YAML** (`bindings: [{subject: bench-admin, role: admin, tenants: [<tenant>]}]`) to the work dir.
- Reuse the exact issuer/audience the store is configured with. Do NOT import `internal/store/authtest` (its `NewJWTEnv` needs `*testing.T`).

### Store driver — `bench/internal/store` (extend `Driver` + cgo impl)
- `Start` gains RBAC wiring when the profile is API: set `AUTHZ_POLICY_FILE`, `OIDC_ISSUER`, `OIDC_JWKS_FILE`, `OIDC_AUDIENCE` (and keep `AUTH_MODE=none` — RBAC forces HTTP ingest to JWT; Flight stays disabled so no fail-fast). Leave the default (non-API) path exactly as today (`AUTH_MODE=none`, no RBAC).
- `IngestMetricsHTTP` sends `Authorization: Bearer <jwt>` when a token is configured (baseline path sends none).
- New query methods for the API path (server **up**, over HTTP):
  - `CountMetricsAPI(ctx) (int64,error)` → `POST /{ns}/sql` body `{"sql":"SELECT COUNT(*) AS n FROM metrics"}`, parse `rows[0][0]`.
  - `AggregateMetricsAPI(ctx) error` → `POST /{ns}/sql` `SELECT "__name__", avg(value), min(value), max(value), count(*) FROM metrics GROUP BY "__name__"` (consume rows).
  - Both send the Bearer JWT; assert 200 + expected shape; a helper decodes the generic `{columns,rows,row_count,truncated}` response.
- Server lifecycle for the API run: expose `StopServer(ctx)` and `StartServer(ctx)` (or `Restart`) so the orchestrator can: ingest (server up) → stop → `Compact` (embedded engine builds tiers) → `CountLogsLike` (embedded, server down) → **restart with RBAC** → `CountMetricsAPI`/`AggregateMetricsAPI` (server up). `Compact` must release the embedded engine (close `d.eng`) before the server restarts so DuckDB file locks are free.
- Negative RBAC smoke (integration test, not in the timed run): a `reader`-role token → `/sql` 200; a `writer`-role token → `/sql` 403; a token bound to a different tenant → 404; no token → 401.

### Orchestrator — `bench/cmd/prism-bench/main.go`
- Add `--api` bool flag (default false). Thread a `profile` string (`"" | "api"`) through.
- **Baseline path unchanged.** API path reorders the query phase as above (logs_like while server is down; count/aggregation over HTTP with server up) and mints/points the driver at the JWT + policy + JWKS files.
- **Resource sampling per phase (API path):** ingest → `prism-store` binary; logs_like → bench process (embedded); **count + aggregation → `prism-store` binary** (server does the work now, not the embedded engine). Adjust the `usageFor` mapping for the API profile accordingly and update the "which process is sampled" sentence in the rendered notes.
- **Correctness gates still enforced:** metrics `COUNT(*)` via API == dataset rows == ClickHouse; logs LIKE via engine == ClickHouse == expected.
- **Outputs (profile-suffixed; baseline files untouched):** when `--api`, write `bench/results-api.json`, `bench/RESULTS-api.md`, `bench/results-timeseries-api.json`, and charts under `bench/charts-api/`. Add `Environment.Profile` (`"api"`) to the report; render a title/subtitle noting **RBAC on + queries via HTTP `/sql`**, and a caveat line: *prism-store count/aggregation are end-to-end HTTP + JWT/RBAC + per-request sandbox (materialize-then-lock); ClickHouse uses its native protocol client; logs LIKE remains engine-level (no logs API).* Fix chart-embed paths so `bench/RESULTS-api.md` references `charts-api/…svg`.
- `RenderMarkdown`/paths must be parameterized by profile without duplicating the renderer (small helper for the suffix + chart dir).

### Makefile + docs
- `make bench-api` → `go run ./bench/cmd/prism-bench --api $(BENCH_FLAGS)` (mirror the existing `bench` target's env/caps).
- Update `bench/README.md`: new "RBAC + HTTP SQL API profile" section — what it measures, the asymmetry caveat, how to reproduce (`make bench-api`), and the attached output paths. Do not alter the baseline description.

### Out of scope
- Switching ClickHouse to its HTTP interface (kept native; caveat documented).
- A logs SQL relation / logs-over-API (store has no logs ingest).
- Changing baseline artifacts or numbers.

## 3. Open questions (resolved)
- [x] Attach vs replace → profile-suffixed files; baseline untouched; new README subsection added by orchestrator after the run.
- [x] logs_like over API → not possible (no logs relation); keep engine-level in the API profile, server down, clearly labeled.
- [x] ClickHouse protocol → keep native; document the asymmetry rather than overclaim.
- [x] JWT source → runtime RSA + JWKS file + policy file in the work dir (`OIDC_JWKS_FILE`), signed with go-jose.

## 4. Decision log (Decision Protocol)
- **Attach a second profile instead of mutating the baseline.**
  - ref: reproducible-benchmark hygiene — keep an unchanged control; Datasette/DuckDB securing write-up shows API-path costs differ from embedded (https://github.com/simonw/research/blob/main/datasette-duckdb-safety/README.md).
  - perf: the API path adds HTTP + JWT verify + per-request sandbox/materialization overhead — a real, separately reported cost. product: side-by-side baseline vs RBAC/API makes the overhead legible without losing the control.
- **Keep ClickHouse native + caveat rather than force HTTP parity.**
  - ref: ClickHouse HTTP vs native interface tradeoffs (https://clickhouse.com/docs/en/interfaces/http).
  - perf/product: avoids re-baselining ClickHouse; the honest caveat prevents an apples-to-oranges misread while still giving context.

## 5. Acceptance checklist (developer)
- [ ] `bench/internal/authgen`: RSA keygen, JWKS file, RS256 JWT (matching `iss`/`aud`), policy YAML writer; a short-expiry token helper. Unit test: minted JWT verifies against the JWKS and the store's verifier config (or a focused parse test).
- [ ] Driver: RBAC-on `Start`, Bearer ingest, `CountMetricsAPI`/`AggregateMetricsAPI` over `/sql`, `StopServer`/`StartServer` (or `Restart`) with engine released before restart; baseline path byte-for-byte unchanged.
- [ ] **Integration test (no Docker):** boot `prism-store` with RBAC (JWKS file + policy), ingest a small window with Bearer, run `/sql` COUNT + GROUP BY and assert correct numbers; negative cases reader→200, writer→403, other-tenant→404, no-token→401.
- [ ] Orchestrator `--api`: reordered phases, per-phase sampling source fixed for API, correctness gates enforced, profile-suffixed outputs, baseline path unchanged when flag absent.
- [ ] Report: `Environment.Profile="api"`, RBAC/API title + caveat line, chart-embed paths correct for `RESULTS-api.md`.
- [ ] `Makefile` `bench-api` target; `bench/README.md` profile section (+ caveat + reproduce). Baseline docs unchanged.
- [ ] `make lint` + `make test` (`-race`) green (new tests included); `go build ./bench/... ./cmd/prism-store` ok; `CGO_ENABLED=0 go build ./cmd/prism` unaffected; `make tidy` clean; `git status` clean. **Do not run the full Docker benchmark** (orchestrator does that).

## 6. Mandatory review gates (reviewer)
- [ ] **Gate 1:** harness code cohesive; no duplicated renderer; wrapped errors; no globals; token/JWKS handling clean; atomic comments (§3.8).
- [ ] **Gate 2 — edge cases:** baseline run still works unchanged (flag absent); server restart releases DuckDB locks (no "database is locked"); expired/again-minted token; API count mismatch fails the gate loudly; charts-api dir created; profile suffix never collides with baseline files.
- [ ] **Gate 3:** `bench/README.md` + Makefile + rendered caveat match code (which workloads go over the API, the ClickHouse asymmetry, logs engine-level).
- [ ] **Gate 4:** atomic comments.
- [ ] TDD via `git log` (authgen + driver API tests first); full `docs/REVIEW.md` + TESTING layering.

## 7. Reviewer notes
_(empty until first review)_

## 8. Orchestrator post-merge-of-harness step (not a dev task)
- Run `make bench-api`; verify correctness gates pass; commit `bench/results-api.json`, `bench/RESULTS-api.md`, `bench/results-timeseries-api.json`, `bench/charts-api/*.svg`; add a **new** README benchmark subsection embedding the API/RBAC results **beside** the existing baseline (attach, not replace).
