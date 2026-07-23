# Migration: `prism-proxy` (homelab-apps) → `prism-store` (prism)

> Cutover plan for issue #30. `prism-store` is the generalized, project-agnostic
> port of `homelab-apps/services/prism-proxy`. The actual migration PRs land in
> **`homelab-apps`** and **`homelab-gitops`**; this document is the plan they
> follow. It stays in `prism` so the epic (#21) is self-contained.

## TL;DR

`prism-store` is a **drop-in** replacement for `prism-proxy`: identical on-disk
layout, identical `/stats` billing JSON, identical ingest/query wire paths **when
`ROUTE_PREFIX=/prism-proxy` is set**. The cutover is an **image swap + a couple of
env vars** (`ROUTE_PREFIX`, and `AUTH_MODE=bearer` if the deployment uses
`INGEST_TOKEN`) on the same PVC — no re-ingest, no dashboard changes, no billing
changes.

## Compatibility invariants (must not break)

| Invariant | Guarantee |
|---|---|
| **Ingest path** `POST /prism-proxy/{ns}/ingest/{artifact}` | Preserved by setting `ROUTE_PREFIX=/prism-proxy` (prism-proxy hardcoded this prefix; prism-store makes it configurable). |
| **Query path** `GET /prism-proxy/{ns}/query` | Same — governed by `ROUTE_PREFIX`. Grafana view SQL uses the exported helper (`prism-store print-view-sql`, #26); result set unchanged. |
| **Billing** `GET /stats?ns=` | Byte-for-byte identical JSON — top-level `totalWindows`, `onDiskBytes`, `compactionCpuSeconds` plus per-artifact `artifacts.<name>.{windows,latestUnixNanos}` — #27. `credit-metering.ts` keeps working unchanged. |
| **On-disk layout** (`tiers/`, `hot/`, `rollups/`, `engine.duckdb`, `.metering.json`) | Identical — the **existing PVC data is reused in place**. The legacy `metrics-raw/` importer (#24) covers any un-migrated tenant on first open. |
| **Provisioning** `POST /admin/tenants/{ns}/ensure` | Same handler + idempotent seeds — #27. |
| **OUTPUT_CONTRACT** (agent Parquet) | Unchanged — the agent (`cmd/prism`) is untouched. |

## Environment variable map

The env surface is a **superset** — every `prism-proxy` variable keeps its name.
Only set the two marked **must-set** to preserve wire compatibility; the rest
default to prism-proxy-equivalent behavior.

| `prism-proxy` (old) | `prism-store` (new) | Note |
|---|---|---|
| _(route hardcoded `/prism-proxy`)_ | `ROUTE_PREFIX` | **Must set** `=/prism-proxy` to keep ingest/query paths identical. |
| `INGEST_TOKEN` | `INGEST_TOKEN` | Same. With `AUTH_MODE=bearer` it is the ingest bearer. |
| _(implicit bearer/none)_ | `AUTH_MODE` | New: `none` (default) \| `bearer` \| `mtls` \| `trusted-header`. Set to match the current ForwardAuth/token posture. |
| `LISTEN_ADDR` | `LISTEN_ADDR` | Same (`:8080`). |
| — | `ADMIN_LISTEN_ADDR` | New (optional): bind `/admin/*`+`/stats`+`/query` on a separate port for NetworkPolicy isolation. Leave unset to keep single-port behavior. |
| — | `ADMIN_TOKEN` | New (optional): bearer for the admin plane. **Caution:** enabling it makes `/admin/*`, `/stats`, and `/query` require the bearer — update the reconciler and billing scraper first, or they break (prism-proxy had no admin-plane auth). |
| `DATA_DIR` | `DATA_DIR` | Same (`/data`). Point at the existing PVC. |
| `ALLOWED_ARTIFACTS` | `ALLOWED_ARTIFACTS` | Same (`metrics-raw`). |
| `MAX_BODY_BYTES` | `MAX_BODY_BYTES` | Same. |
| `HOT_WINDOW_MINUTES` / `HOT_WINDOW_SECONDS` | idem | Same. |
| `SEGMENTS_PER_TIER`, `MAX_SEGMENT_BYTES`, `MAX_TIER` | idem | Same (`MAX_TIER` was implicit at 8 in proxy). |
| `RETENTION_DAYS`, `RETENTION_TICK_HOURS`/`_SECONDS` | idem | Same. |
| `ROLLUP_STEPS` | `ROLLUP_STEPS` | Same (`1m,5m,1h`). |
| `HOT_SNAPSHOT_SECONDS`, `FLUSH_TICK_SECONDS`, `MERGE_TICK_SECONDS` | idem | Same. |
| `E2E_EXPOSE_QUERY_SQL` | `E2E_EXPOSE_QUERY_SQL` | Same (e2e-only). |
| — | `FLIGHT_ADDR` | New (optional): Arrow Flight `DoPut` receiver; leave unset for HTTP-only. |
| — | `AUTHZ_POLICY_FILE` | New (optional): enables **RBAC** (JWT/OIDC + deny-by-default per-tenant `reader`/`writer`/`admin` policy) on HTTP query/ingest/admin routes. Leave unset to keep prism-proxy token behavior. |
| — | `OIDC_ISSUER` / `OIDC_JWKS_URL` / `OIDC_JWKS_FILE` / `OIDC_AUDIENCE` / `AUTHZ_RELOAD_SECONDS` | New: JWT verification config, required when RBAC is enabled (`OIDC_JWKS_FILE` for offline/air-gapped). |
| — | `SQL_API_ENABLED` / `SQL_API_MAX_ROWS` / `SQL_API_TIMEOUT_SECONDS` / `SQL_API_MAX_BODY_BYTES` | New: arbitrary read-only SQL API (`POST {ROUTE_PREFIX}/{ns}/sql`); default on, RBAC-guarded. |
| — | `SQL_API_QUEUE_ENABLED` / `SQL_API_MAX_INFLIGHT` / `SQL_API_MAX_QUEUE` / `SQL_API_QUEUE_TIMEOUT_MS` | New (v1.3): optional `/sql` in-flight limiter; **all default off / backward-compatible** (`SQL_API_QUEUE_ENABLED=false`). |
| — | `DUCKDB_THREADS` / `DUCKDB_MEMORY_LIMIT` / `MAX_OPEN_TENANTS` / `QUERY_HOT_ONLY` / `RUN_JOBS` | New: DuckDB governance and reader/writer split; see [`MEMORY.md`](MEMORY.md). |
| — | **`duckdb_arrow` build tag** | **Build-from-source only:** `prism-store` release/CI builds pass `-tags duckdb_arrow` (CGO) to enable Arrow IPC streaming on `POST /sql` when clients send `Accept: application/vnd.apache.arrow.stream`. Prebuilt images include it; plain `go build ./...` without the tag serves JSON only (Arrow requests → `406`). |

> **RBAC precedence / caution.** When `AUTHZ_POLICY_FILE` is set, RBAC is
> authoritative on HTTP data/admin routes and **supersedes `INGEST_TOKEN`/`ADMIN_TOKEN`**
> there — clients must send a valid JWT. RBAC is **HTTP-only**: if `FLIGHT_ADDR`
> is enabled it must keep its own non-`none` `AUTH_MODE` (startup fail-fasts on
> `RBAC + FLIGHT_ADDR + AUTH_MODE=none`). Keep RBAC **unset** for a like-for-like
> prism-proxy cutover, and adopt it as a follow-up once JWT issuance (k8s SA tokens
> or Vault) is wired.

Full reference: [`docs/CONFIG.md`](docs/CONFIG.md) §14 and [`docs/MEMORY.md`](docs/MEMORY.md).

## Cutover steps (homelab-apps / homelab-gitops)

1. **Pin the release.** Choose a published `prism-store` tag; confirm
   `ghcr.io/elk-utilities/prism-store` is covered by the Kyverno `verify-images`
   allow-list (already `ghcr.io/elk-utilities/*`) — signatures are keyless cosign
   (#29), so no policy change is expected.
2. **Swap the image (path A — lowest risk).** In the existing homelab chart, set
   `proxy.image` → `ghcr.io/elk-utilities/prism-store:<tag>` and add
   `ROUTE_PREFIX=/prism-proxy` (+ `AUTH_MODE` to match current auth). Keep the
   Deployment/PVC/Service as-is. This is a pure runtime swap on the same volume.
3. **Env rename map + gitops values.** Apply the table above to
   `envs/{dev,prod}/apps/values/prism-proxy.yaml` (rename the file to
   `prism-store.yaml` if desired; keep the ApplicationSet mapping). Confirm the
   reconciler still calls `/admin/tenants/{ns}/ensure` and billing scrapes
   `/stats?ns=`.
4. **Dev soak.** On **dev**: reconcile a real tenant, post a `metrics-raw`
   window, run a query, hit `/stats?ns=`, and load the `prism-metrics-overview`
   dashboards + live-demo. All must be green and byte-compatible.
5. **Prod promotion.** Promote via the human `homelab-gitops` PR (post-merge
   promote uses the new image path).
6. **Delete `services/prism-proxy`.** Remove the code, its chart, and CI
   build/promote entries (`Makefile`, `post-merge-promote.yml`). Leave a
   `docs/solutions/` note pointing at `prism` (`docs/STORE.md`, this file).

### Optional path B — adopt prism's base chart

After path A is stable, migrate to prism's project-agnostic chart
([`deploy/charts/prism-store`](../deploy/charts/prism-store), #28) with a homelab
overlay carrying the consumer-specific bits (Traefik `IngressRoute`, ESO
`ExternalSecret`, ForwardAuth, Grafana wiring). Recommended second, not first.

## Rollback

Each step is reversible on the same PVC: revert `proxy.image` to the last
`prism-proxy` tag and drop `ROUTE_PREFIX`/`AUTH_MODE`. Because the on-disk layout
and `/stats` contract are identical, rolling back requires no data migration.
Roll back at the gitops layer (revert the values PR); the PVC data is untouched.

## Verification checklist (per environment)

- [ ] `/admin/tenants/{ns}/ensure` → `204` for a real tenant (idempotent).
- [ ] Agent posts `metrics-raw` to `/prism-proxy/{ns}/ingest/metrics-raw` → `2xx`.
- [ ] `/prism-proxy/{ns}/query?start=&end=` returns the expected series.
- [ ] Grafana `prism-metrics-overview` renders (view SQL from `print-view-sql`).
- [ ] `/stats?ns=` JSON matches the pre-cutover shape byte-for-byte; `credit-metering.ts` bills correctly off the same PVC.
- [ ] Compaction/rollups/retention tick (segment counts in `/stats` evolve; `.metering.json` accrues `compactionCpuSeconds`).

## Acceptance (issue #30, consumer-side)

Central stack runs the prism-published image on prod; a tenant ingests, queries,
sees dashboards, and is billed correctly off the same PVC; `homelab-apps` no
longer contains `services/prism-proxy`; OUTPUT_CONTRACT + `/stats` contract intact.
