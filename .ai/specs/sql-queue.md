# Spec: /sql in-flight queue (off by default) + configurable workers/memory caps + docs

Status: IN_REVIEW

- **Slug / branch:** `feat/sql-queue`
- **Ships as:** `v1.3.0` (feature; latest tag `v1.2.0`). Multi-arch package published after merge.
- **Owner phase:** orchestrator → developer (code+tests+helm) → developer (docs) → reviewer + security-review → merge → release.
- **Why:** `POST /{ns}/sql` has no concurrency cap, so worst-case memory scales unbounded as `concurrent_requests × DUCKDB_MEMORY_LIMIT` (Grafana-style dashboard fan-out is the concrete trigger). Add a bounded in-flight limiter (a "queue") with backpressure, **disabled by default** (backward-compatible). Make DuckDB worker threads/memory apply uniformly to **all** DuckDB instances (engine, sandbox, and the currently-uncapped merge/rollup workers) for predictable cluster memory. Then document every flag, every prism-store feature, the memory model research, and a clean RBAC guide.

## 1. Task

### A. `/sql` in-flight limiter ("queue") — new package `internal/store/queue`
- A reusable HTTP middleware `queue.Limiter` with config `{Enabled bool, MaxInFlight int, MaxQueue int, Wait time.Duration}`.
- Semantics:
  - **Disabled (default) → transparent passthrough** (zero behavior change).
  - Enabled: at most `MaxInFlight` requests execute the handler concurrently. Additional requests **wait** up to `Wait` for a slot; at most `MaxQueue` may be waiting at once. If the waiting set is full, or the wait exceeds `Wait`, or the request context is cancelled first → respond **`429 Too Many Requests`** with `Retry-After: 1` and a short body (`too many concurrent queries`). Never leak a slot (release in `defer`).
  - Implementation: buffered-channel semaphore (cap `MaxInFlight`) for running slots + an atomic waiter counter bounded by `MaxQueue`. Acquire with `select` over the semaphore, a `time.NewTimer(Wait)`, and `r.Context().Done()`.
- **Placement (order matters):** in `newServeMux` at `cmd/prism-store/main.go:333`, wrap so the effective order is **auth → OwnedTenantGuard → limiter → SQLHandler** (auth + tenant guard reject cheaply *before* a slot is consumed):
  ```go
  h := query.SQLHandler(sqlCfg, eng, logger)
  h = queue.Middleware(sqlLimiter, h)            // innermost
  if ownedTenants != nil { h = cluster.OwnedTenantGuard(ownedTenants, h) }
  mux.Handle(query.SQLRoutePattern(cfg.routePrefix), protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapSQL, h))
  ```
  Applies in **standalone + client** modes (both call `newServeMux`). **Not** on the cluster coordinator (`runCluster`/`cluster.NewServeMux`) — it only reverse-proxies; the limiter must guard the data node that opens the sandbox. Build one shared `*queue.Limiter` from cfg and reuse it for the public and admin mux (do not create two independent limiters — a single limiter instance passed into `newServeMux`).

### B. Configurable workers / memory caps (uniform DuckDB governance)
- New env (mirror existing helpers in `main.go`):
  | Env | Default | Meaning |
  |---|---|---|
  | `SQL_API_QUEUE_ENABLED` | `false` | Master switch for the /sql limiter |
  | `SQL_API_MAX_INFLIGHT` | `4` | Max concurrent /sql queries when enabled |
  | `SQL_API_MAX_QUEUE` | `64` | Max requests allowed to wait for a slot |
  | `SQL_API_QUEUE_TIMEOUT_MS` | `5000` | Max wait before `429` |
  | `MAX_OPEN_TENANTS` | `32` | Tenant-engine LRU size (wire `engine.Config.MaxOpenTenants`) |
- Thread `DUCKDB_THREADS` + `DUCKDB_MEMORY_LIMIT` into the **merge** (`internal/store/merge/executor.go` — `ExecutorConfig`+`NewExecutor`, and `StatSegment`'s connector) and **rollup** (`internal/store/rollup/rollup.go` — `NewBuilder`) DuckDB connectors, applied via a connector-init callback that runs `SET threads` / `SET memory_limit` (mirror `internal/store/engine/engine.go:323-350`). Plumb via `lifecycle.Config{Threads, MemoryLimit}` set from `cfg.duckdbThreads`/`cfg.duckdbMemoryLimit` in `main.go:486`. This closes the "uncapped merge/rollup DuckDB" term.
- Wire `MaxOpenTenants: cfg.maxOpenTenants` into the `engine.New(...)` call (`main.go:478`).
- Extend the startup `logger.Info` block (`main.go:535`) with `sql_api_queue_enabled`, `sql_api_max_inflight`, `max_open_tenants`.
- Extend `clearStoreEnv` in `cmd/prism-store/main_test.go` with the new vars.

### C. Helm chart (`deploy/charts/prism-store/`)
- Add env entries in `templates/statefulset.yaml` + `values.yaml` for the new vars AND the previously-untemplated `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`, `QUERY_HOT_ONLY`, `RUN_JOBS`, `MAX_OPEN_TENANTS` (string-valued under `env:`, quoted, matching the existing `sqlAPI*` idiom). Only emit an env var when its value is non-empty for the optional string caps (`DUCKDB_*`) so defaults stay "unset"; booleans/ints always emitted.
- Regenerate the golden snapshot: `helm template golden deploy/charts/prism-store > deploy/charts/prism-store/testdata/golden/default.yaml` and commit.

### D. Documentation (explicit user requirement — must be thorough)
- **`docs/CONFIG.md` §14:** make the prism-store env table the **authoritative, complete** flag reference — every env var the binary reads, incl. the new ones and the previously-undocumented `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`, `QUERY_HOT_ONLY`, `RUN_JOBS`, `MODE`, `CLIENT_TENANTS`, `CLUSTER_CLIENTS`, `MAX_OPEN_TENANTS`, `ADMIN_LISTEN_ADDR`, RBAC vars. Cross-check against `loadConfig()` so none are missing.
- **`docs/STORE.md`:**
  - Ensure a complete **Features** overview (ingest, tiered storage, merges, rollups, retention, hot window, hot-snapshot, `/query`, arbitrary `/sql` (JSON + Arrow transport), hot-only, RBAC, cluster MODE, metering/stats, Flight ingest, run-jobs split).
  - Add the queue + 429 behavior to the SQL API Limits section; note merge/rollup now honor DuckDB caps and `MAX_OPEN_TENANTS`.
  - **Clean up the RBAC section** (`docs/STORE.md:55-141`) into a clear, self-contained "how to implement/operate RBAC" guide: identity (JWT/OIDC + JWKS), roles (reader/writer/admin), policy file format + hot-reload, deny-by-default, 404 anti-enumeration, cluster defense-in-depth (coordinator authorize-before-proxy + client re-enforce), k8s + Vault/secrets integration, and the Flight fail-fast. Tighten wording, fix any drift, ensure copy-paste-correct examples.
- **`docs/MEMORY.md` (NEW):** capture the memory research as a standalone reference:
  - Per-instance soft ceilings vs. no global pool; the `peak ≈ Σ active DuckDB instances × DUCKDB_MEMORY_LIMIT + Go heap` model.
  - What `DUCKDB_MEMORY_LIMIT` means + how it scales (streaming scans ~constant; GROUP BY/JOIN ~cardinality; ORDER BY/result ~rows; ×threads); soft cap → spill; sandbox sets `max_temp_directory_size='0B'` → fail-fast (no spill) vs. engine can spill.
  - Hot-buffer growth = time-bounded (`HOT_WINDOW_*` + `FLUSH_TICK_SECONDS`), no row/byte cap; the `docs/STORE.md` sizing formula.
  - `RUN_JOBS=false` reader/writer split (drops the merge/rollup term; keep readers read-only).
  - Transport clarification: Arrow is a response *encoding* of the same `/sql` endpoint, not a separate path; helps Go heap + wire, not sandbox memory.
  - The `/sql` queue (this feature) as the lever that bounds the concurrency term; sizing recipe (`inflight × threads ≈ cores`; container hard-limit > Σ soft caps + headroom; set `GOMEMLIMIT`/`GOMAXPROCS` via pod env — honored by the Go runtime, not read by the app).
  - Heterogeneous tenants: no per-tenant limit in-process today → isolate via cluster mode per-tenant/per-class pods with k8s limits; `MAX_OPEN_TENANTS` lever.
  - Link from `README.md`, `docs/STORE.md`, `docs/CONFIG.md`.
- **`docs/MIGRATION.md`:** list the new env vars (all default to prior behavior). **`docs/DESIGN.md`:** one-line note on the limiter + uniform DuckDB caps. **`README.md`:** mention the optional /sql queue and link `docs/MEMORY.md`.

### Out of scope
- Per-tenant limit maps / admin override API (documented as the cluster-mode path instead). Changing default behavior (queue stays off). The bench harness.

## 2. Design decisions (Decision Protocol)
- **Bounded semaphore + waiter cap + wait-timeout → 429 with Retry-After, disabled by default.**
  - ref: Prometheus `query.max-concurrency`, Loki `max_concurrent`, Go stdlib `golang.org/x/sync/semaphore` patterns — max-concurrency with backpressure is the standard TSDB approach.
  - perf/product: turns the unbounded `concurrent /sql × limit` memory term into a constant ceiling and prevents DuckDB thread oversubscription; 429 + `Retry-After` gives clients orderly backpressure instead of random OOM/500s. Off-by-default preserves current behavior for single-user/low-QPS.
- **Reuse `DUCKDB_THREADS`/`DUCKDB_MEMORY_LIMIT` for merge/rollup (not new worker-only envs).**
  - ref: existing engine/sandbox already apply these; DuckDB `SET memory_limit`/`threads` docs.
  - perf/product: one uniform knob to bound every DuckDB instance → predictable cluster memory; fewer flags to reason about. `inflight × threads ≈ cores` is the sizing rule.
- **Limiter on data node only; coordinator unchanged.** ref: `cluster/router.go` reverse-proxy has no sandbox — a coordinator queue would gate proxy slots, not memory.

## 3. Open questions — resolved
- [x] Flag to enable, default off → `SQL_API_QUEUE_ENABLED=false`.
- [x] Also make workers configurable → `SQL_API_MAX_INFLIGHT` (concurrent queries) + `DUCKDB_THREADS` (per-query threads), applied everywhere incl. merge/rollup; `MAX_OPEN_TENANTS` wired.
- [x] Publish multi-arch → tag `v1.3.0` post-merge (release workflow builds amd64+arm64 with `duckdb_arrow`).

## 4. Acceptance checklist (developer)

### Code
- [x] `internal/store/queue` package: `Limiter` + `Middleware`; disabled→passthrough; `MaxInFlight` concurrency; `MaxQueue` waiter bound; `Wait` timeout; context-cancel; `429`+`Retry-After`; slot always released.
- [x] Env wired in `loadConfig()` + `serverConfig` (5 new fields) with the defaults in §1.B; single shared `*queue.Limiter` passed into `newServeMux` and applied to `/sql` in the documented order.
- [x] `MaxOpenTenants` wired into `engine.New`.
- [x] `DUCKDB_THREADS`/`DUCKDB_MEMORY_LIMIT` applied to merge (`NewExecutor` + `StatSegment`) and rollup (`NewBuilder`) connectors via init callback; plumbed through `lifecycle.Config`.
- [x] Startup log + `clearStoreEnv` updated.

### Tests (TDD, tests-first)
- [x] Queue unit tests: (a) disabled = passthrough; (b) never more than `MaxInFlight` concurrent (blocking handler + barrier); (c) waiter beyond `MaxQueue` → immediate 429; (d) wait exceeds `Wait` → 429 with `Retry-After`; (e) slot released so later requests succeed; (f) client cancel returns promptly.
- [x] Config load test for new envs (incl. defaults) + `clearStoreEnv`.
- [x] Merge + rollup: assert `SELECT current_setting('memory_limit'|'threads')` reflects the configured value on their connections.
- [x] Existing `/sql` tests still green (limiter disabled by default).

### Helm + build
- [x] Chart env added; golden regenerated & committed; `helm lint`/golden check pass.
- [x] `make lint test` green with `-tags duckdb_arrow`; `go build -tags duckdb_arrow ./...`; `CGO_ENABLED=0 go build ./cmd/prism` ok; `make tidy` clean; `git status` clean.

### Docs (§1.D) — all required
- [x] `docs/CONFIG.md` §14 complete & matches `loadConfig()`.
- [x] `docs/STORE.md` features overview + SQL limits (queue) + **clean RBAC guide**.
- [x] `docs/MEMORY.md` created and linked from README/STORE/CONFIG.
- [x] `docs/MIGRATION.md`, `docs/DESIGN.md`, `README.md` updated.

## 5. Mandatory review gates (reviewer) — SECURITY-SENSITIVE (limiter sits in the auth/tenant path)
- [ ] Gate 1 — Guidelines: reusable middleware; single shared limiter; wrapped errors; atomic comments §3.8; no behavior change when disabled.
- [ ] Gate 2 — Edge cases: order is auth→guard→limiter→handler (auth/guard reject without consuming a slot); no slot leak on panic/cancel/timeout; `MaxInFlight<=0` guarded; 429 body/headers correct; limiter absent on coordinator; merge/rollup caps actually applied.
- [ ] Gate 3 — Docs match code: every env in CONFIG.md exists in `loadConfig()` and vice-versa; STORE features/RBAC accurate; MEMORY.md matches actual knobs; helm values ↔ template ↔ golden consistent.
- [ ] Gate 4 — Atomic comments.
- [ ] **SECURITY AUDIT:** limiter cannot bypass auth/RBAC/tenant guard; no cross-tenant slot starvation vector introduced beyond intended global cap (note: global not per-tenant — documented); no unbounded goroutine growth (waiters bounded by `MaxQueue`); default-off = no new surface.
- [ ] Full `docs/REVIEW.md`; TESTING layering; TDD via `git log` (tests-first).

## 6. Reviewer notes
_(empty until first review)_
