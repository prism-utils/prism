# Spec: prism-store — ingest receiver (HTTP + Flight) + tenant isolation + pluggable auth

Status: IN_REVIEW

- **Slug / branch:** `feat/store-ingest`
- **Owner phase:** orchestrator → developer
- **Issue:** elk-utilities/prism#23 (Epic #21) — depends on #22, #24 (both merged).

## 1. Task

Give `prism-store` its write entry point: an HTTP ingest handler (with the full
validation chain + status codes) landing windows into the merged
`internal/store/engine`, a `/healthz`+`/readyz` pair, **project-agnostic
pluggable auth** (`none|bearer|mtls|trusted-header`), and an optional
**Arrow Flight `DoPut`** receiver that lands the same way. All homelab coupling
(the baked `/prism-proxy/...` prefix, the Traefik-ForwardAuth assumption) is
removed and made configurable. Ported/generalized from `homelab-apps`
`services/prism-proxy` `cmd/prism-proxy/main.go` + `internal/ingest`.

## 2. Scope

- **In scope:**
  - **`internal/store/ingest`** (new logic package):
    - **Validation chain** helper used by both transports, in this order → status:
      1. auth (below) fails → `401 unauthorized`;
      2. tenant not `tenant.TenantAllowed` → `404 unknown tenant`;
      3. artifact not in `ALLOWED_ARTIFACTS` (well-formed + allowed) → `404 unknown artifact type`;
      4. body > `MAX_BODY_BYTES` (via `http.MaxBytesReader`) → `413 window too large`.
      Success → `engine.Ingest(tenant, body)`; empty body → `204`; else `204 No Content`.
    - **Pluggable auth** (`AUTH_MODE`): `none` (path tenant authoritative), `bearer` (`Authorization: Bearer <INGEST_TOKEN>` static; constant-time compare), `mtls` (require a verified TLS client cert; its CN must equal the path tenant), `trusted-header` (upstream gateway already authenticated; `X-Tenant` header must equal the path tenant). **In every mode the tenant in the path must match the authenticated tenant** or → `401`/`403` (choose per case, documented). Table tests per mode.
    - Reuse `internal/store/tenant` for tenant/artifact validators — no duplication.
  - **`cmd/prism-store` wiring:** register `POST <ROUTE_PREFIX>/{ns}/ingest/{artifact}` (prefix default empty → `/{ns}/ingest/{artifact}`); keep `GET /healthz` (`ok`) and `GET /readyz` (`MkdirAll(DATA_DIR)` writable → `503`/`ready`); construct the engine; `ReadHeaderTimeout` 15s; graceful shutdown 10s on SIGINT/SIGTERM. **No background tickers yet** (that is #25) — flush still happens opportunistically via the engine's `maybeFlushDue` on ingest past the deadline.
  - **Arrow Flight `DoPut` receiver** (config-gated by `FLIGHT_ADDR`, off by default): a Flight server whose `DoPut` reads the incoming Arrow IPC record stream, encodes it to a Parquet window, and lands it via `engine.Ingest`. Tenant is taken from the `FlightDescriptor` path (first element) and run through the **same validation chain + auth** (bearer via gRPC metadata `authorization`, mirroring the agent's authenticated Flight path). Reuse `internal/tlsconf` for TLS; reuse the Arrow Flight patterns already in the repo. Round-trips a real agent-produced window.
  - **Config (env, in `cmd/prism-store`):** `LISTEN_ADDR` (`:8080`), `FLIGHT_ADDR` (empty=off), `DATA_DIR` (`/data`), `ALLOWED_ARTIFACTS` (`metrics-raw`), `MAX_BODY_BYTES` (`268435456`), `INGEST_TOKEN` (empty), `AUTH_MODE` (`none`), `ROUTE_PREFIX` (empty). Document all in `docs/CONFIG.md` + `docs/STORE.md`.
  - **No homelab-specific strings** in core (`prism-proxy` prefix only reachable by setting `ROUTE_PREFIX=/prism-proxy` — a config value, not a constant).
- **Out of scope:** compaction/rollups/retention + tickers (#25); query API (#26); `/admin/ensure`, `/stats`, seeds (#27); Helm (#28); release (#29). Do not add those routes/handlers.

## 3. Open questions  (resolved before READY)

- [x] Where does the tenant come from under each auth mode? → **path is the addressed tenant; the authenticated identity must equal it** (bearer/none: no per-tenant identity, path is authoritative; mtls: cert CN==path; trusted-header: `X-Tenant`==path). Mismatch → reject.
- [x] Does Flight land the same way as HTTP? → yes: DoPut → Parquet → `engine.Ingest`, same validation chain; tenant from descriptor path[0].
- [x] Keep the legacy file-landing `ingest.Store.Land`? → **No.** The engine is the landing path now; do not port the raw dir writer (the legacy `metrics-raw/` importer in the engine covers historical files).
- [x] Background flush tickers here? → **No**, deferred to #25; rely on `maybeFlushDue`.

## 4. Decision log  (Decision Protocol)

- **Pluggable `AUTH_MODE` instead of assuming a service mesh.**
  - ref: https://pkg.go.dev/net/http#Request.TLS (verified client certs) + reverse-proxy trusted-header pattern (e.g. Traefik ForwardAuth / nginx `auth_request`).
  - perf: negligible per-request check. product: the store works behind any gateway (Traefik/nginx/Envoy) or standalone — the core "project-agnostic" lever the epic calls for.
- **Path tenant must equal the authenticated tenant (defense in depth).**
  - ref: OWASP path-traversal / broken-object-level-authorization guidance.
  - perf: none. product: a compromised/misrouted request cannot write into another tenant's partition even if the gateway is misconfigured.
- **Flight lands through the same engine path as HTTP.**
  - ref: https://arrow.apache.org/docs/format/Flight.html (`DoPut` streaming ingest).
  - perf: Flight avoids per-window HTTP overhead for high-rate agents; product: one landing/validation code path, two transports — no divergence in tenant isolation or contract.

## 5. Acceptance checklist  (developer checks these off)

- [x] HTTP ingest accepts a real agent-produced `metrics-raw` window → `204`; rows land in the tenant's `hot_current` (assert via engine `HotRowCount`).
- [x] Validation/status codes: unknown tenant `404`; unknown/malformed artifact `404`; body over limit `413` (`MaxBytesReader`); empty body `204` no-op; happy path `204`.
- [x] Auth modes covered by tests: `none` (open), `bearer` (missing/wrong `401`, correct `204`, constant-time compare), `trusted-header` (`X-Tenant` mismatch rejected, match ok), `mtls` (no/wrong client cert rejected, CN==tenant ok) — the last using an httptest TLS server with a client cert.
- [x] `ROUTE_PREFIX` respected (empty and e.g. `/prism-proxy` both route correctly); no hardcoded prefix.
- [x] `/healthz`=`ok`; `/readyz` writability → `200 ready`/`503`.
- [x] Flight `DoPut` (when `FLIGHT_ADDR` set) round-trips a window into `hot_current`; unauthenticated Flight rejected when a token is configured; tenant from descriptor validated.
- [x] All new config env parsed with the documented defaults; documented in `docs/CONFIG.md` + `docs/STORE.md`.
- [x] No homelab-specific string constants in core (grep clean for `prism-proxy`, `traefik`, homelab namespaces).
- [x] Tests written first (`test:` commit precedes implementation) — CONTRIBUTING.md §1.
- [x] `make lint test` green; `CGO_ENABLED=0 go build ./cmd/prism` still passes; `go build ./cmd/prism-store` passes.

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Guidelines:** factory/config patterns, no globals beyond compiled regex, slog only (handlers log at the edge, not libs), errors wrapped, `MaxBytesReader` used (not manual counting), graceful shutdown, mux handlers thin. `internal/store/ingest` is leaf (imports engine + tenant + tlsconf only).
- [ ] **Gate 2 — Edge cases:** empty body; oversize body; malformed/unknown artifact; malformed tenant + path traversal; each auth-mode failure; tenant/identity mismatch; Flight unauthenticated + bad descriptor; concurrent ingest under `-race`.
- [ ] **Gate 3 — Docs/comments match code:** `docs/CONFIG.md` + `docs/STORE.md` list exactly the env + routes + auth modes that landed; ingest handler order documented; no forward references.
- [ ] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [ ] Full docs/REVIEW.md checklist; TESTING.md layering (httptest unit + a Flight integration/golden where useful).

## 7. Reviewer notes

_(empty until first review)_
