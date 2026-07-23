# Spec: prism-store — deployment `MODE` (standalone / client / cluster) with tenant-routed query federation

Status: READY

- **Slug / branch:** `feat/store-mode-cluster`
- **Owner phase:** orchestrator → developer
- **Feature 3 of 3** in the prism-store roles epic (ship as its own PR).

## 1. Task

Introduce a bootstrap **`MODE`** config with three values (default **`standalone`**):

- **`standalone`** — a self-contained node (current behavior, unchanged).
- **`client`** — a data-holding leaf that serves its own tenants and is fronted by a
  coordinator. It **only** answers for the tenants it owns; requests for any other
  tenant are rejected (data-isolation guard).
- **`cluster`** — a **coordinator/router** that holds **no data**. It serves an
  address, and for each incoming query it **forwards the request over HTTP to the
  client that owns that tenant**, returning that client's response. A query is
  routed to **exactly one** owning client and can **never** return data from a
  different tenant's client. Unknown tenant → error (never a fallthrough).

Identifier == tenant/namespace (`ns`), reusing the store's existing tenant boundary.
No RBAC yet (future); this delivers the tenant-scoped data abstraction RBAC will
build on.

## 2. Scope

### New package `internal/store/cluster`
- **`Mode` type + `ParseMode(string)`** → `standalone|client|cluster`; empty → `standalone`; invalid → error.
- **Client map parsing:** `ParseClients(env string)` → `map[tenant]*url.URL` from `CLUSTER_CLIENTS` formatted as `tenantA=http://host1:8080,tenantB=http://host2:8080`. Validate: non-empty tenant (via the existing tenant validator), well-formed absolute http(s) URL; dup tenant → error; empty map in cluster mode → startup error.
- **Owned-tenant parsing:** `ParseOwnedTenants(env string)` → set from `CLIENT_TENANTS` (comma list); required non-empty in client mode.
- **Coordinator `Router` (`http.Handler`)** for cluster mode:
  - Serves the same query route pattern (`GET <prefix>/{ns}/query`) plus `/healthz`, `/readyz`.
  - Extracts `ns` via `r.PathValue("ns")`; validates it; looks it up in the client map. **Miss → 404 `unknown tenant`** (no default upstream, no guessing).
  - Forwards to the owning client using **`httputil.ReverseProxy` with the `Rewrite` hook + `ProxyRequest.SetURL`** (preserve the full request path + query string; forward the inbound `Authorization` header; sensible `http.Transport` with timeouts). Cache one proxy per distinct client base URL. Stream the response back unmodified. On upstream failure → `ErrorHandler` returns `502`.
- **Client-side isolation guard** (`http.Handler` middleware) for client mode: wraps the query route; if the request's `ns` is not in the owned set, respond `404` before touching the engine.

### `cmd/prism-store/main.go`
- Read `MODE` (default `standalone`) into `serverConfig`; parse via `cluster.ParseMode`; invalid → startup error. Log the effective mode.
- **standalone** → existing `runServe` path unchanged.
- **client** → `runServe` path, but the query route is wrapped with the owned-tenant guard; read+validate `CLIENT_TENANTS` (required). Ingest/admin/jobs behave as today (jobs still honor `RUN_JOBS`). Log owned tenants.
- **cluster** → a new `runCluster(ctx, cfg, logger)` server path: **no engine, no ingest, no background jobs**; builds the `Router` from `CLUSTER_CLIENTS`; serves on `LISTEN_ADDR` with the same graceful-shutdown lifecycle as `runServe` (health endpoints + query routing only).

### Out of scope (documented as future)
- Scatter-gather / multi-client aggregation (decision: route-to-single-owner).
- Routing ingest or admin/`/stats`/`/ensure` through the coordinator (queries only for now).
- Dynamic client registration / service discovery (static config only).
- RBAC, per-client auth/mTLS, ret/failover/HA, health-aware routing (future; note in docs).

## 3. Open questions  (resolved before READY)

- [x] Transport cluster→client? → **Reuse the HTTP query API** via `httputil.ReverseProxy` (`Rewrite`+`SetURL`).
- [x] Distribution semantics? → **Route each query to the single client that owns the tenant**; no cross-client merge.
- [x] Discovery? → **Static** `CLUSTER_CLIENTS` env map.
- [x] Identifier vs tenant? → **Identifier == tenant/ns** (reuse existing boundary).
- [x] What makes `client` mode distinct from `standalone`? → the **owned-tenant isolation guard** (`CLIENT_TENANTS`), so a leaf refuses tenants it does not own — defense in depth beneath the coordinator's exact routing.
- [x] Does the coordinator hold data? → **No**; cluster mode is a pure router (no engine/ingest/jobs).
- [x] Unknown tenant at the coordinator? → **404**, never routed to any client.

## 4. Decision log  (Decision Protocol)

- **Coordinator = stateless HTTP routing layer that forwards per-tenant to owning clients (route-to-single-owner).**
  - ref: ClickHouse **Distributed table** — "a routing layer that stores no data itself … sits in front of the per-shard local tables and routes queries" (QueryPlane: https://queryplane.com/blog/clickhouse-cluster-replication-and-keeper-in-practice/ ). Compute/serving separation (ObsessionDB: https://obsessiondb.com/blog/building-on-decoupled-clickhouse ).
  - perf: forwarding is a thin proxy hop; the heavy scan runs on the owning client (query pushdown), and each client is sized for its tenant.
  - product: exact per-tenant routing gives the hard data-isolation the user requires and a clean seam for RBAC later.
- **Use `httputil.ReverseProxy` with the `Rewrite` hook (not `Director`).**
  - ref: Go stdlib `net/http/httputil` docs — `Rewrite`/`ProxyRequest.SetURL` is the current recommended API (https://pkg.go.dev/net/http/httputil ).
  - perf: stdlib streaming proxy, connection pooling via a shared `Transport`; no framework overhead.
  - product: minimal, dependency-free, secure header handling; matches "reuse the HTTP query API".
- **Static config discovery + identifier==tenant.**
  - ref: ClickHouse manual `Distributed` clusters are statically configured; tenant/shard identity is explicit.
  - perf: no coordination service; product: reproducible, simple, and the isolation key is the already-validated `ns`.

## 5. Acceptance checklist  (developer checks these off)

- [ ] `internal/store/cluster`: `ParseMode` (default standalone, invalid errors); `ParseClients` (valid map; rejects malformed URL, empty tenant, dup tenant, empty-in-cluster-mode); `ParseOwnedTenants` (required non-empty in client mode) — all unit-tested incl. error paths.
- [ ] Coordinator `Router`: **routing correctness + isolation** proven with `httptest` fake client servers — a query for tenant A reaches ONLY A's upstream; tenant B reaches ONLY B's upstream; **unknown tenant → 404 and NO upstream is contacted**; the response returned is exactly the owning client's response; `Authorization` header is forwarded; path + query string preserved; upstream error → `502`.
- [ ] Client-mode guard: with `CLIENT_TENANTS={A}`, a query for A is served; a query for B → `404` **before** the engine is touched (unit/httptest).
- [ ] `cmd/prism-store`: `MODE` parsed (default standalone; invalid → startup error, logged); **cluster** runs `runCluster` with **no engine/ingest/jobs** and serves health + query routing; **client** runs the engine-backed server with the owned-tenant guard on the query route; **standalone** unchanged. Startup logs include `mode` (+ owned tenants / client count).
- [ ] `runCluster` graceful shutdown: starts, serves, stops cleanly on ctx cancel; bind-failure returns a wrapped error; no goroutine leak (`-race`/goleak). A test asserts cluster mode does NOT open the engine/data dir.
- [ ] Docs: `docs/STORE.md` (+ `main.go` usage comment) document `MODE`, `CLUSTER_CLIENTS`, `CLIENT_TENANTS`, the three roles, the isolation guarantee, and the out-of-scope/future list (RBAC, ingest routing, scatter-gather, discovery). If `docs/DESIGN.md` has an ADR/architecture section, add a short mode/federation note (Decision Protocol reference included).
- [ ] `make lint test` (`-race`) green; `go build ./cmd/prism-store` ok; `CGO_ENABLED=0 go build ./cmd/prism` ok (no new deps; httputil is stdlib); `make tidy` clean; no committed blobs.

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Guidelines:** new package cohesive, handlers behind interfaces, no globals, ctx-bound server lifecycle, wrapped errors; `ReverseProxy` uses `Rewrite` (not deprecated `Director`) with a bounded `Transport`; comments self-contained.
- [ ] **Gate 2 — Edge cases:** unknown tenant (404, no upstream hit); client map with one vs many tenants; two tenants → same client URL; malformed `CLUSTER_CLIENTS`/empty in cluster mode → clear startup error; upstream down → 502 (not a hang/panic); `ROUTE_PREFIX` respected in the router path; client-mode guard rejects non-owned ns before engine; invalid `MODE` → startup error; cluster-mode server has no data dir/engine and still shuts down cleanly.
- [ ] **Gate 3 — Docs/comments match code:** env names/defaults, the three roles, and the isolation guarantee match the code; the future/out-of-scope list is accurate.
- [ ] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [ ] **Isolation audit (critical):** prove by test that a tenant's query can never be routed to, or return data from, another tenant's client; unknown/unmapped tenants are refused, not defaulted; the client-side guard is a genuine second layer.
- [ ] Full `docs/REVIEW.md` checklist; TESTING.md layering (parser unit tests + httptest router/guard tests; no external network in tests); TDD verified via `git log` (test-first).

## 7. Reviewer notes

_(empty until first review)_
