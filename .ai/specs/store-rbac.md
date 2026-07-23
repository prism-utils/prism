# Spec: prism-store — RBAC (JWT/OIDC identity + per-tenant roles, deny-by-default)

Status: ALL_OK

- **Slug / branch:** `feat/store-rbac`
- **Owner phase:** orchestrator → developer
- **Security-critical.** Threat model: OWASP API1 **BOLA** (cross-tenant access) and API3/priv-esc. Delivered as ONE PR (per requester).

## 1. Task

Add role-based access control to prism-store so that **a principal can only act on
the tenants it is explicitly authorized for**, with **deny-by-default** and **no
privilege escalation**. Concretely: User A can never read, ingest, or see the
existence/metadata (incl. `/stats`) of User B's tenant, and cannot elevate its own
rights. Identity is a **verified JWT (OIDC/JWKS)**; authorization is a **fixed-role,
per-tenant policy** loaded from a **mounted file** (k8s ConfigMap/Secret / Vault),
hot-reloaded. Enforced in **all modes**, including the cluster coordinator (edge)
**and** clients (defense-in-depth).

## 2. Design (resolved)

### Identity (authN) — `internal/store/auth`
- **JWT bearer** in `Authorization: Bearer <jwt>`. Verify signature against a **JWKS**, obtained either via **OIDC discovery** (`OIDC_ISSUER`) or a **static JWKS** (`OIDC_JWKS_FILE`/`OIDC_JWKS_URL`). Validate `iss`, `aud` (`OIDC_AUDIENCE`, ≥1 required), `exp`/`nbf`/`iat` with small leeway. Principal **subject = `sub`** claim (non-empty required). Use a well-maintained library (e.g. `github.com/coreos/go-oidc/v3` for OIDC verification + JWKS refresh; or `golang-jwt` + a JWKS keyfunc). JWKS refresh/caching handled by the library; a static-JWKS path must work offline.
- A small `Verifier` interface so mTLS/static-token verifiers can be added later; **JWT/OIDC is the only implementation in this PR.**
- **No trust in client-supplied identity headers** (e.g. `X-Tenant`, `X-User`): identity comes only from the verified JWT. Any such inbound headers are ignored for authz.

### Authorization (authZ) — `internal/store/authz`
- **Fixed roles → permissions:**
  - `reader` → {`query`}
  - `writer` → {`ingest`}
  - `admin`  → {`query`,`ingest`,`ensure`,`stats`} (full control of the bound tenant)
- **Policy file (YAML)**, deny-by-default. Shape (illustrative):
  ```yaml
  bindings:
    - subject: "system:serviceaccount:teamA:ingest"
      role: writer
      tenants: ["team-a"]
    - subject: "alice@corp"
      role: reader
      tenants: ["team-a", "team-b"]
    - subject: "platform-admin"
      role: admin
      tenants: ["*"]     # cluster-wide admin
  ```
  - `tenants: ["*"]` = all tenants (only meaningful for operators; document the blast radius).
  - Parse + validate on load: known role, non-empty subject, valid tenant tokens (existing tenant validator, or `*`), no contradictory dup. **Invalid policy on startup → hard fail**; invalid policy on **reload → keep the previous good policy and log** (never fail-open, never crash).
  - **Hot reload:** re-read the file periodically (default ~15s, `AUTHZ_RELOAD_SECONDS`) by mtime; atomic swap under an `RWMutex`. (Poll, not fsnotify — dependency-free and robust to k8s ConfigMap symlink swaps and Vault-rendered files.)
- **`Authorizer.Authorize(principal, action, tenant) Decision`** — pure, deny-by-default; returns Allow / DenyForbidden / DenyNotFound distinction so the middleware can pick the right status. `AuthorizedTenants(principal, action)` helper for `/stats` scoping.

### Enforcement middleware — `internal/store/authz` (HTTP)
- Wrap query, ingest, ensure, stats handlers. Steps: extract bearer → `Verifier.Verify` (401 on missing/invalid/expired/bad-aud/iss/sig) → map route→action → extract tenant (`{ns}` path, or `?ns=` / "all" for stats) → `Authorize`.
- **Status semantics (anti-enumeration / BOLA):**
  - unauthenticated / invalid token → **401**
  - authenticated but **not authorized for that tenant** → **404** `unknown tenant` (byte-identical to a genuinely-unknown tenant; no existence signal)
  - authenticated, authorized for the tenant, but the **action isn't permitted by the role** → **403**
- Route→action: `GET /{ns}/query`→`query`; ingest route→`ingest`; `POST …/ensure`→`ensure`; `GET /stats`→`stats`.

### `/stats` enumeration fix
- When RBAC is enabled: `GET /stats?ns=X` requires `stats` on X (else 404). `GET /stats` **without** `ns` aggregates **only over tenants the principal has `stats` on** (a `*`-admin sees all; a scoped admin sees only its tenants; a non-admin → empty/403 per the rules). This closes the current all-tenant directory scan leak.

### Cluster (edge + client)
- **Coordinator (`cluster`)**: wrap the router's query route with the authz middleware — **authenticate + authorize before routing**; a denied/unknown tenant returns 404/403/401 and **no upstream is contacted**. Forward the **original JWT** (`Authorization`) to the owning client unchanged; do **not** inject or trust identity headers.
- **Client**: re-apply the authz middleware (independent JWT verification + authorization) in addition to the existing `OwnedTenantGuard` — a compromised/misconfigured coordinator still cannot make a client serve an unauthorized tenant.

### Config wiring — `cmd/prism-store/main.go`
- New env: `AUTHZ_POLICY_FILE` (enables RBAC when set), `OIDC_ISSUER`, `OIDC_JWKS_URL`, `OIDC_JWKS_FILE`, `OIDC_AUDIENCE` (comma list), `AUTHZ_RELOAD_SECONDS`.
- When **RBAC enabled**: the authz middleware guards query/ingest/admin on all planes and in standalone/client/cluster; the legacy shared `ADMIN_TOKEN`/`INGEST_TOKEN` gates are **superseded** for those routes (document precedence: if `AUTHZ_POLICY_FILE` is set, RBAC is authoritative). Startup fails fast if RBAC is enabled but OIDC/JWKS config is missing/invalid.
- When **RBAC disabled** (no policy file): **behavior is exactly as today** (existing `AUTH_MODE`/tokens) — fully backward compatible.

### No-privilege-escalation guarantees (must hold + be tested)
- Policy is read-only (mounted file); **no API mutates it**; principals cannot grant themselves roles.
- Deny-by-default: any subject/tenant/action without a matching binding is denied.
- `sub` cannot be spoofed (JWT signature verified); tokens for other systems rejected via strict `aud`/`iss`.
- A `reader`/`writer` cannot invoke admin actions (403); no route lets a lower role perform a higher action.

### Out of scope (documented as future)
- mTLS/SPIFFE and static-token verifiers (interface only), token revocation lists, OPA/Rego, RBAC for the Flight ingest path (HTTP is the RBAC surface this PR; note Flight still uses `AUTH_MODE`), UI, and audit-log shipping (structured deny logs are in scope; shipping is not).

## 3. Open questions  (resolved before READY)

- [x] Credential → **JWT/OIDC (JWKS)**. [x] Roles → **fixed reader/writer/admin, per-tenant**. [x] Policy → **mounted YAML file, deny-by-default, hot-reload**. [x] Denials → **404 hide / 403 forbidden-action / 401 unauth**. [x] Cluster → **edge + client**, scope `/stats`. [x] Delivery → **one PR**.
- [x] RBAC vs existing `AUTH_MODE`? → RBAC (when `AUTHZ_POLICY_FILE` set) supersedes token gates on HTTP data/admin routes; `AUTH_MODE` remains for backward-compat when RBAC off and for Flight. Documented precedence.

## 4. Decision log  (Decision Protocol)

- **JWT/OIDC identity verified against JWKS.**
  - ref: Kubernetes ServiceAccount tokens are OIDC JWTs (projected, audience-bound) — https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/ ; OWASP API Security Top 10 API1:2023 BOLA — https://owasp.org/API-Security/editions/2023/en/0xa1-broken-object-level-authorization/ .
  - perf: JWTs are stateless (no per-request lookup); JWKS cached. product: no shared secrets; native k8s + Vault-issued-JWT integration; subject identity for auditing.
- **Fixed roles + per-tenant bindings, deny-by-default, mounted policy file, hot-reload.**
  - ref: k8s RBAC model (Role/RoleBinding, deny-by-default) — https://kubernetes.io/docs/reference/access-authn-authz/rbac/ .
  - perf: O(bindings) in-memory check under RWMutex. product: familiar model; GitOps/ConfigMap/Vault-friendly; least-privilege; simple enough to audit.
- **404 for unauthorized tenants (anti-enumeration).**
  - ref: OWASP BOLA guidance — do not reveal object existence to unauthorized callers (link above).
  - perf: n/a. product: User A cannot even discover User B's tenant/metadata — directly satisfies the requirement.
- **Enforce at coordinator edge AND client (defense-in-depth); forward the JWT, trust no identity header.**
  - ref: zero-trust / defense-in-depth; OWASP BOLA.
  - perf: one extra stateless verify per hop. product: a misconfigured/compromised coordinator can't coerce a client into leaking another tenant.

## 5. Acceptance checklist  (developer checks these off)

- [x] `internal/store/auth`: JWT/OIDC `Verifier` (JWKS via OIDC discovery or static file/URL; validates sig/`iss`/`aud`/`exp`/`nbf`; `sub`→principal). Unit tests with a locally-generated signing key + JWKS (httptest, NO external network): valid; expired; bad signature; wrong `aud`; wrong `iss`; missing `sub`; malformed → all rejected with distinct errors.
- [x] `internal/store/authz`: policy parse/validate (deny-by-default; unknown role, empty subject, bad tenant, `*` handling, reload-keeps-old-on-error, startup-fails-on-bad-initial); `Authorize` permission matrix (reader/writer/admin × query/ingest/ensure/stats × same/other tenant) fully table-tested incl. deny-by-default for unbound subjects; `AuthorizedTenants` for stats scoping.
- [x] HTTP authz middleware: 401 (missing/invalid token), 404 (authed but tenant not authorized — identical to unknown tenant), 403 (authorized tenant but action not permitted); route→action mapping correct; identity headers from the client are ignored. httptest-covered for query/ingest/ensure/stats.
- [x] **BOLA/isolation tests (critical):** token for user A → query/ingest/`stats?ns=B` on tenant B all return **404**; A calling an admin route without admin → **403**; unbound subject → denied everywhere; `sub` spoof via header has no effect.
- [x] `/stats` scoping: `*`-admin sees all tenants; scoped admin sees only its tenants; `?ns=X` requires `stats` on X (else 404); non-admin cannot enumerate. Test proves no cross-tenant leak.
- [x] Cluster: coordinator authenticates+authorizes before routing — unauthorized/unknown tenant returns 404/403/401 with **no upstream contacted** (httptest fakes); JWT forwarded to the owning client; client independently re-enforces (a direct client request for an unauthorized tenant is still denied even if it bypasses the coordinator).
- [x] `cmd/prism-store`: RBAC enabled by `AUTHZ_POLICY_FILE`; OIDC env wired; fail-fast when RBAC on but OIDC/JWKS misconfigured; middleware applied to query/ingest/admin across standalone/client/cluster; **RBAC-off path byte-for-byte unchanged** (guarded by a test). Startup logs the effective RBAC state (enabled, issuer, #bindings) WITHOUT logging secrets/tokens.
- [x] Hot-reload test: editing the policy file changes decisions within the reload interval; an invalid edit keeps the last good policy and logs.
- [x] Docs: `docs/STORE.md` (+ `docs/CONFIG.md` env table + `main.go` usage) document the RBAC model, env vars, policy file format, the 401/403/404 semantics, precedence vs `AUTH_MODE`, and the k8s (projected SA token + ConfigMap) and Vault (Agent-rendered JWT + policy) wiring. `docs/DESIGN.md` §15 gets a short RBAC ADR note with the references above. If the Helm chart exists (`deploy/charts/prism-store`), add values + templates for policy mount + OIDC env (feature-flagged, default off).
- [x] No secret material committed; structured **deny** logs include subject + action + tenant + reason but never the raw token.
- [x] `make lint test` (`-race`) green; `go build ./cmd/prism-store` ok; **`CGO_ENABLED=0 go build ./cmd/prism` ok and its import graph contains NEITHER the OIDC/JWT lib NOR the YAML lib** (RBAC is store-only — verify with `go list -deps`); `make tidy` clean.

## 6. Mandatory review gates  (reviewer owns)  — SECURITY-CRITICAL

- [x] **Gate 1 — Guidelines:** cohesive `auth`/`authz` packages, `Verifier`/`Authorizer` interfaces, no globals, ctx-aware, wrapped errors; policy reload atomic under RWMutex; comments self-contained.
- [x] **Gate 2 — Edge cases:** missing/expired/wrong-aud/wrong-iss/tampered token; clock-skew leeway; empty/`*` tenant lists; unknown tenant vs unauthorized tenant are indistinguishable (404); reload race + invalid reload (fail-closed to last good, never fail-open, never crash); JWKS endpoint unreachable at startup (fail-fast) vs transient refresh failure (serve with cached keys); coordinator with a down client still authorizes first (no upstream on deny).
- [x] **Gate 3 — Docs/comments match code:** env names/defaults, policy schema, status-code semantics, precedence, and k8s/Vault wiring match the code.
- [x] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [x] **SECURITY AUDIT (must pass):** deny-by-default proven; **no cross-tenant read/ingest/metadata path** (BOLA) exists; **no privilege escalation** (policy immutable via API, no lower role reaching a higher action, `sub`/`aud`/`iss` strictly enforced, no trust of client identity headers); no fail-open on policy/JWKS errors; tokens/secrets never logged; 404-hide leaks no existence; cluster edge+client both enforce. Confirm the agent static build excludes the new deps.
- [x] Full `docs/REVIEW.md` checklist; TESTING.md layering; TDD verified via `git log` (security tests written first).

## 7. Reviewer notes

**2026-07-23 — APPROVE (prism-reviewer).** All four gates + SECURITY AUDIT pass. TDD: `33b8609 test:` precedes `724023d feat:`. Commands: `make lint` 0 issues; `make test -race` green; `CGO_ENABLED=0 go build ./cmd/prism` ok; `go build ./cmd/prism-store` ok; `go list -deps ./cmd/prism` excludes `go-oidc`/`go-jose`/`gopkg.in/yaml.v3`; `make tidy` + clean tree. Security enforcement: deny-by-default `Authorizer.Authorize` (unbound → `DecisionDenyNotFound`); anti-enumeration `Middleware.wrapTenantAction`/`WrapStats` 404 via `unknown tenant` body; JWT-only identity (`authenticate` discards X-User/X-Tenant); fail-closed policy reload (`tryReload` keeps prior on parse error); cluster `NewServeMux` wraps router before proxy (`TestRouterRBACDenyBeforeProxy`). Minor note (non-blocking): clock-skew tolerance is go-oidc library default, not an explicit env knob.

**2026-07-23 — Security fix round (developer).** Independent review found Flight fail-open when RBAC forced `AuthNone` on shared ingest config; fixed with separate HTTP/Flight auth modes + startup fail-fast when RBAC+Flight+`AUTH_MODE=none`. Also: stats handler fail-closed without RBAC scope, shared `tenant.UnknownTenantBody` for byte-identical 404 bodies, JWT alg=none/HMAC-confusion regression tests. TDD: `5b6ea66 test:` precedes fix commit.

**2026-07-23 — RE-APPROVE after security-fix round (prism-reviewer).** Re-ran gates on `5b6ea66`→`d09c7fb`. Verified: (1) Flight no longer fail-open — `httpIngestAuthMode`→AuthNone for JWT middleware only; `flightIngestAuthMode` keeps operator `AUTH_MODE`; `validateRBACFlight` fail-fast in `runServe`; `TestFlightKeepsBearerAuthWhenHTTPUsesAuthNone` + `TestValidateRBACFlightRejectAuthNone`. (2) Stats fail-closed — `StatsHandler` with `RBACEnabled` returns 403 without `StatsScopeFromContext` (`TestStatsHandlerRBACFailClosedWithoutScope`); RBAC-off unchanged. (3) 404 parity — `tenant.UnknownTenantBody` shared across authz/query/ingest/admin/cluster (`TestUnknownTenantBodyByteIdenticalAcrossHandlers`). (4) JWT hardening — alg=none, HMAC/RSA confusion, tampered payload rejected. Prior guarantees intact (deny-by-default, BOLA 404-hide, no escalation, no token leakage, cluster edge+client). Commands: `make lint` 0; `make test -race` green; builds ok; `go list -deps ./cmd/prism` excludes RBAC deps; `make tidy` clean. Independent security review findings addressed — no High/Medium remaining.
