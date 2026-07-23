# prism-store — Store design

> The durable, tiered columnar store and query server for prism pipeline
> outputs. Today it accepts **metrics** artifacts only (`metrics-raw` per
> `docs/OUTPUT_CONTRACT.md` v1); logging artifacts land in later sub-issues.
> The store is the **consumer** side of that contract — the agent (`cmd/prism`)
> is the producer.

Architecture decisions live in [`DESIGN.md`](DESIGN.md) §15. Implementation
lands sub-issue by sub-issue; this page describes the target shape.

---

## Role

`prism-store` receives HTTP-parquet and optional Arrow Flight windows from edge
agents, lands them into per-tenant DuckDB hot catalogs, maintains tiered Parquet
segments, materializes rollups, and exposes read-only query endpoints.

### Deployment modes (`MODE`)

Bootstrap **`MODE`** selects one of three roles (default **`standalone`**):

| Mode | Role | Data | Query routing |
|---|---|---|---|
| `standalone` | Self-contained node | Engine + ingest + jobs on `DATA_DIR` | Local engine |
| `client` | Data-holding leaf fronted by a coordinator | Same as standalone | Local engine, but only for tenants listed in `CLIENT_TENANTS`; other tenants → `404` before the engine runs |
| `cluster` | Stateless coordinator/router | **None** — no engine, ingest, or background jobs | Forwards `GET` and `POST` `<prefix>/{ns}/query` / `{ns}/sql` to the owning client URL from `CLUSTER_CLIENTS` |

**Isolation guarantee:** identifier == tenant/namespace (`ns`). The coordinator routes
each query to **exactly one** owning client; unknown/unmapped tenants get `404`
with **no** upstream contact. Client mode adds a second layer: the owned-tenant
guard rejects non-owned `ns` before the engine is touched.

| Variable | Default | Purpose |
|---|---|---|
| `MODE` | `standalone` | `standalone` \| `client` \| `cluster` |
| `CLIENT_TENANTS` | _(empty)_ | Comma-separated owned tenants; **required** in `client` mode |
| `CLUSTER_CLIENTS` | _(empty)_ | Static map `tenant=http://host:port,...`; **required** in `cluster` mode |

Cluster mode serves `/healthz`, `/readyz`, and query routing only on
`LISTEN_ADDR`. It forwards the inbound `Authorization` header and preserves
the full path + query string via `httputil.ReverseProxy`.

When **`AUTHZ_POLICY_FILE`** is set, RBAC is enforced on HTTP data and admin
routes in **all** modes (standalone, client, cluster coordinator, and client
leaves). See [RBAC](#rbac-jwtoidc--per-tenant-roles) below.

**Future / out of scope:** routing ingest, admin, `/stats`, or `/ensure`
through the coordinator; scatter-gather across clients; dynamic service
discovery; per-client mTLS; health-aware routing and failover.

---

## RBAC (JWT/OIDC + per-tenant roles)

Threat model: **OWASP BOLA** (cross-tenant access) and privilege escalation.
Identity is a verified **JWT** (OIDC/JWKS); authorization is a **deny-by-default
YAML policy** with fixed roles, hot-reloaded from a mounted file.

### Precedence vs `AUTH_MODE`

When **`AUTHZ_POLICY_FILE`** is set, RBAC is **authoritative** on HTTP query,
ingest, ensure, and stats routes — static `ADMIN_TOKEN` / `INGEST_TOKEN` gates
are not used for those routes. When the policy file is unset, behavior is
unchanged (legacy `AUTH_MODE` + tokens).

**Arrow Flight is HTTP-only for RBAC.** JWT/RBAC middleware does not protect
Flight `DoPut`. When RBAC is enabled and `FLIGHT_ADDR` is set, startup **fails**
if `AUTH_MODE=none` — operators must configure a non-`none` Flight auth mode
(`bearer`, `mtls`, or `trusted-header`) or disable Flight. HTTP ingest under
RBAC uses JWT; Flight keeps the operator-configured `AUTH_MODE` independently.

### Identity (`internal/store/auth`)

- Bearer JWT in `Authorization: Bearer <token>`.
- Signature verified against JWKS from OIDC discovery (`OIDC_ISSUER`), static
  file (`OIDC_JWKS_FILE`), or URL (`OIDC_JWKS_URL`).
- Validates `iss`, `aud` (`OIDC_AUDIENCE`, comma-separated), `exp`/`nbf`/`iat`.
- Principal = non-empty `sub`. Client identity headers (`X-Tenant`, `X-User`, …)
  are **ignored**.

### Authorization (`internal/store/authz`)

Fixed roles:

| Role | Actions |
|---|---|
| `reader` | `query` |
| `writer` | `ingest` |
| `admin` | `query`, `ingest`, `ensure`, `stats` |

Policy file (YAML):

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
    tenants: ["*"]
```

- `tenants: ["*"]` grants the role on all tenants (operator blast radius).
- Invalid policy at **startup** → process exit. Invalid policy on **reload** →
  keep last-good policy and log (never fail-open).
- Hot reload: poll file mtime every `AUTHZ_RELOAD_SECONDS` (default 15s).

### HTTP status semantics (anti-enumeration)

| Condition | Status |
|---|---|
| Missing / invalid JWT | `401 unauthorized` |
| Authenticated but not authorized for tenant | `404 unknown tenant` (same body as unknown tenant) |
| Authorized for tenant but role lacks action | `403 forbidden` |

### `/stats` scoping

When RBAC is on: `GET /stats?ns=X` requires `stats` on `X` (else `404`).
`GET /stats` without `ns` aggregates only tenants the principal has `stats`
on (`*`-admin sees all; scoped admin sees its tenants; non-admin → `403`).

### Cluster defense-in-depth

- **Coordinator:** authenticate + authorize **before** routing; denied tenants
  return `401`/`403`/`404` with **no upstream contact**; forward the original JWT.
- **Client:** re-verify JWT and re-authorize in addition to `OwnedTenantGuard`.

### Kubernetes wiring

1. Mount a **ConfigMap** (or Vault Agent-rendered file) at e.g.
   `/etc/prism/rbac/policy.yaml` and set `AUTHZ_POLICY_FILE`.
2. Use a **projected ServiceAccount token** with audience bound to
   `OIDC_AUDIENCE` (matches your IdP / API server issuer config).
3. Set `OIDC_ISSUER` (or `OIDC_JWKS_URL` / `OIDC_JWKS_FILE`) and
   `OIDC_AUDIENCE`. The chart exposes optional values (default off).

### Vault wiring

1. Vault Agent renders the policy YAML and a short-lived JWT into a shared
   `emptyDir` mount.
2. Point `AUTHZ_POLICY_FILE` and configure OIDC env to trust Vault's JWKS /
   issuer for the rendered token.

---

## Ingest (`internal/store/ingest`)

Write entry point for contract-v1 Parquet windows. Two transports share one
validation chain and land via `engine.Ingest`.

### HTTP

`POST <ROUTE_PREFIX>/{tenant}/ingest/{artifact}` — raw Parquet body
(`application/octet-stream`). Empty body → `204` no-op.

### Arrow Flight

When `FLIGHT_ADDR` is set, a Flight server accepts `DoPut` streams. Incoming
Arrow IPC record batches are encoded to Parquet and ingested the same way as
HTTP. The `FlightDescriptor` path is `[tenant, artifact, startUnixNano, endUnixNano]`.

Flight is **not** covered by JWT/RBAC. When RBAC is enabled (`AUTHZ_POLICY_FILE`
set) and Flight is enabled, startup fails if `AUTH_MODE=none` — configure
`bearer`, `mtls`, or `trusted-header` for Flight, or disable `FLIGHT_ADDR`.
HTTP ingest under RBAC uses JWT; Flight keeps the operator `AUTH_MODE` unchanged.

### Validation chain (order → status)

1. Auth (see below) → `401` / tenant mismatch → `403`
2. Unknown/malformed tenant → `404 unknown tenant`
3. Unknown/malformed artifact → `404 unknown artifact type`
4. HTTP body over `MAX_BODY_BYTES` → `413 window too large`
5. Success → `204 No Content` (rows in `hot_current`)

### Auth modes (`AUTH_MODE`)

| Mode | Check | Tenant match |
|---|---|---|
| `none` | open | path tenant authoritative |
| `bearer` | `Authorization: Bearer <INGEST_TOKEN>` | path authoritative |
| `mtls` | verified TLS client cert | cert CN == path tenant |
| `trusted-header` | `X-Tenant` header | header == path tenant |

Flight bearer auth mirrors HTTP via gRPC metadata `authorization`.

---

## Configuration (environment)

| Variable | Default | Purpose |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | HTTP bind address |
| `FLIGHT_ADDR` | _(empty)_ | Flight `DoPut` bind address; empty disables Flight |
| `DATA_DIR` | `/data` | Shared data root for all tenants |
| `ALLOWED_ARTIFACTS` | `metrics-raw` | Comma-separated ingest artifact types |
| `MAX_BODY_BYTES` | `268435456` | Max HTTP ingest body (256 MiB) |
| `INGEST_TOKEN` | _(empty)_ | Static bearer token for `AUTH_MODE=bearer` |
| `AUTH_MODE` | `none` | `none` \| `bearer` \| `mtls` \| `trusted-header` |
| `ROUTE_PREFIX` | _(empty)_ | Optional ingest path prefix |
| `HOT_WINDOW_SECONDS` | _(unset)_ | Hot-window duration in seconds (overrides minutes when set) |
| `HOT_WINDOW_MINUTES` | `10` | Hot-window duration in minutes when seconds unset |
| `MaxOpenTenants` | `32` | Bounded LRU of open per-tenant DuckDB handles |
| `RowGroupSize` | `1000000` | Parquet row-group size for flush, snapshot, and merge COPY |
| `SEGMENTS_PER_TIER` | `6` | Lucene-style merge trigger: minimum live segments at a tier before compaction |
| `MAX_SEGMENT_BYTES` | `2147483648` | Seal threshold (2 GiB); segments at or above this size are never merge inputs |
| `RETENTION_DAYS` | `15` | Delete tier segments and rollups whose max timestamp is strictly before `now − N days` |
| `ROLLUP_STEPS` | `1m,5m,1h` | Comma-separated downsampling intervals materialized after L1+ merges |
| `MAX_TIER` | `8` | Highest tier directory scanned (`L0`…`L8`) |
| `HOT_SNAPSHOT_SECONDS` | `15` | Hot snapshot export ticker interval |
| `FLUSH_TICK_SECONDS` | `30` | Hot→L0 flush ticker interval |
| `MERGE_TICK_SECONDS` | `60` | Tier merge ticker interval (at most one merge action per tenant per tick) |
| `RETENTION_TICK_SECONDS` | _(unset)_ | Retention ticker in seconds; when unset, `RETENTION_TICK_HOURS` applies |
| `RETENTION_TICK_HOURS` | `1` | Retention ticker in hours when seconds unset |
| `E2E_EXPOSE_QUERY_SQL` | _(empty)_ | When `1`, query JSON includes generated SQL (e2e/regression only) |
| `QUERY_HOT_ONLY` | `false` | When `true`, HTTP query unions only `hot_current`/`hot_prev` (no tier or rollup Parquet reads). Grafana `print-view-sql` is unchanged. |
| `SQL_API_ENABLED` | `true` | When `false`, `POST /{ns}/sql` is not registered. |
| `SQL_API_MAX_ROWS` | `100000` | Maximum rows returned per SQL request (truncates with `"truncated": true`). |
| `SQL_API_TIMEOUT_SECONDS` | `30` | Per-query timeout for arbitrary SQL. |
| `DUCKDB_MEMORY_LIMIT` | _(empty)_ | DuckDB `memory_limit` for engine and SQL sandbox when set. |
| `RUN_JOBS` | `true` | When `false`, disables all background maintenance (hot snapshot, flush, merge, rollups, retention). Ingest and query still run; hot data will not flush or compact and retention will not delete. |
| `ADMIN_LISTEN_ADDR` | _(empty)_ | When set, binds admin/stats/query on a second HTTP server (see below) |
| `ADMIN_TOKEN` | _(empty)_ | Static bearer token for admin-plane routes when set |
| `MODE` | `standalone` | Deployment role: `standalone`, `client`, or `cluster` (see Role § above) |
| `CLIENT_TENANTS` | _(empty)_ | Owned tenants for `client` mode (comma-separated) |
| `CLUSTER_CLIENTS` | _(empty)_ | Tenant→client base URL map for `cluster` mode (`tenant=http://host:port,...`) |

See [`CONFIG.md`](CONFIG.md) §14 for the full `prism-store` env reference.

---

## Storage engine (`internal/store/engine`)

Per-tenant embedded DuckDB at `<DATA_DIR>/<tenant>/engine.duckdb`. The engine owns the hot ingest path, hot→L0 flush, near-real-time hot snapshots, a bounded tenant LRU, and a one-time legacy `metrics-raw` importer. Background compaction, rollups, retention, and tickers are described below.

### On-disk layout (per tenant)

All paths are relative to `DATA_DIR` (default `/data`):

```
DATA_DIR/
  <tenant>/
    engine.duckdb          # embedded DuckDB catalog (hot + view definitions)
    hot/                   # current hot-window Parquet snapshot
    tiers/
      L0/ … L7/            # immutable merged segments, coarsest at L7
    rollups/
      1m/  5m/  1h/        # time-bucket aggregate Parquet
    .metering.json         # on-disk usage / compaction metering (operator-facing)
```

Each tenant namespace must satisfy the validators in `internal/store/tenant`
(`^[a-z0-9][a-z0-9._-]{0,62}$`). Artifact types follow the output-contract
taxonomy (`metrics-raw` first).

### Hot catalog

Two tables, created idempotently on first open:

- `hot_current` — rows currently accepting ingest
- `hot_prev` — rolled window awaiting L0 export

Schema: `("__name__" VARCHAR, labels VARCHAR, value DOUBLE, timestamp_ms BIGINT, ts TIMESTAMP)`.

**Ingest** streams a contract-v1 parquet window into `hot_current`. Empty bodies are a no-op `(0, nil)`. Non-empty inserts use `ts = clock().UTC()` (ingest time, bound as a SQL parameter — not `timestamp_ms`). The first insert into an empty schedule sets flush at `now + HotWindow` (default 10 minutes).

### Hot → L0 flush

When `clock ≥ scheduled` (`FlushDue`, or `maybeFlushDue` on ingest past deadline):

1. `DROP hot_prev` → `RENAME hot_current → hot_prev` → recreate empty `hot_current`
2. If `hot_prev` is empty: drop it and clear the schedule (no L0 file)
3. Else: `COPY (SELECT * FROM hot_prev ORDER BY ts) TO tiers/L0/<unixNano>.parquet` via temp file + atomic rename, then drop `hot_prev` and clear the schedule

Multiple ingests within one hot window accumulate in `hot_current` and produce a **single** L0 segment on flush.

### Hot snapshot

`ExportHotSnapshots` atomically writes `<tenant>/hot/current.parquet` from `hot_current ORDER BY ts` (temp + rename). Reads see in-flight rows; a background ticker exports every `HOT_SNAPSHOT_SECONDS` (default 15s).

### Legacy `metrics-raw` import

On first tenant open (once per tenant, gated by `.legacy-import-done`):

- Scan `<tenant>/metrics-raw/*.parquet` (skip `_seed.parquet` and dotfiles)
- Copy each file to `tiers/L0/legacy-<nanos>.parquet` with `ts` parsed from the `metrics-raw-<nanos>-` filename prefix
- Skip targets that already exist; write the marker even when the directory is absent

### Tenant LRU

Open DuckDB handles are cached in a bounded LRU (default 32 tenants, `MaxOpenTenants`). Eviction closes the oldest connection. A single mutex guards the LRU and the per-tenant `flushAt` schedule map. `Close()` evicts all handles.

### On-disk permissions

Directories `0750`, files `0640`.

---

## Lifecycle (`internal/store/lifecycle`, `merge`, `rollup`, `stats`)

Background work runs in one goroutine with four independent tickers started from `cmd/prism-store serve` and stopped on shutdown. Tick errors are logged (`slog`) and never fatal.

| Ticker | Default | Action |
|---|---|---|
| Hot snapshot | 15s | `ExportHotSnapshots` |
| Flush | 30s | `FlushDue` (hot→L0) |
| Merge | 60s | One bounded merge per tenant (lowest tier first) |
| Retention | 1h | Delete expired tier segments and rollup files |

### Tiered compaction (`internal/store/merge`)

Lucene **TieredMergePolicy** analogue over immutable Parquet tiers:

- **Seal:** segments with `Bytes ≥ MAX_SEGMENT_BYTES` (default 2 GiB) are never merge inputs.
- **Trigger:** when a tier has ≥ `SEGMENTS_PER_TIER` (default 6) live segments, the planner groups by size level (floor-rounded log scale), picks the first time-adjacent contiguous run (gap ≤ one segment span), and shrinks the candidate set down to 1 if needed so summed bytes ≤ `MAX_SEGMENT_BYTES`.
- **One action per tick:** no cascade — at most one merge per tenant per merge tick, lowest tier first.
- **Promotion:** merged output lands in `L{dest}` with rows ordered by `ts`; source files are deleted only after the output is atomically renamed.

Path helpers live in `internal/store/layout` (`TierDir`, `RollupDir`, `ToSlash`).

### Rollups (`internal/store/rollup`)

After a merge promotes to **L1 or above**, the store materializes downsampled Parquet under `rollups/{step}/` for each step in `ROLLUP_STEPS` (default `1m,5m,1h`). Schema: `bucket`, `"__name__"`, `avg`, `min`, `max`, `count`, `sum` from `time_bucket(step, ts)` grouped by bucket and name. L0 merges do not build rollups (avoids rework on volatile data).

### Retention

Tier segments with `MaxTs` **strictly before** `now − RETENTION_DAYS` are deleted (default 15 days kept, 16 days deleted at the boundary). Rollup files whose max `bucket` is before the same cutoff are removed on the retention tick.

### Metering (`internal/store/stats`)

Per-tenant `.metering.json` tracks cumulative `compactionCpuSeconds` (JSON field name preserved for billing). Each merge tick adds the **wall-clock elapsed seconds** of the DuckDB COPY merge (single-threaded burst ≈ CPU for billing purposes). `TenantOnDiskBytes` sums `tiers/`, `rollups/`, `hot/`, and `engine.duckdb` (+ `.wal`); legacy `metrics-raw/` and dotfiles are excluded.

---

## Query (`internal/store/query`)

Read-only time-range queries over the unified hot + tier + rollup view. Queries
execute under `engine.WithRead` (shared read lock) so reads never observe the
hot table mid-flush.

### HTTP

`GET <ROUTE_PREFIX>/{tenant}/query?start=&end=&step=`

| Query param | Required | Format |
|---|---|---|
| `start` | yes | RFC3339 inclusive lower bound |
| `end` | yes | RFC3339 exclusive upper bound |
| `step` | no | Rollup step override (`1m`, `5m`, `1h`); when omitted, auto-selected from range width |

| Condition | Status |
|---|---|
| missing/invalid `start` or `end` | `400` |
| unknown/malformed tenant | `404 unknown tenant` |
| tenant root absent on disk | `400 bad query` |
| execution failure | `500 query failed` |
| success | `200 application/json` `{ "rows": […], "sql"? }` |

The optional `sql` field appears only when `E2E_EXPOSE_QUERY_SQL=1` (regression
guards; not for production).

When `QUERY_HOT_ONLY=true`, the union includes only parts (1)–(2) below; tier
and rollup Parquet reads are skipped for lower latency on freshness-only
queries. Default is the full union (hot + tiers + rollups).

### Union SQL shape

`Builder.BuildSQL` unions (each part filtered with bound `?` parameters):

1. `hot_current` — `ts >= ? AND ts < ?`
2. `hot_prev` — same
3. each present tier `L0`…`L7` — `read_parquet('<tenant>/tiers/L{n}/*.parquet')` filtered on `ts`
4. optional rollup — `read_parquet('<tenant>/rollups/<step>/*.parquet')` projected to the row shape, filtered on `bucket`

Wrapped as `SELECT * FROM (…) ORDER BY ts`. **Fixed schema only:** no
`union_by_name`, no `filename=true`.

### Rollup step auto-selection

When `step` is omitted:

| Range width | Rollup step |
|---|---|
| ≥ 7 days | `1h` |
| ≥ 24 hours | `5m` |
| ≥ 1 hour | `1m` |
| shorter | raw tiers only (no rollup part) |

Explicit `step` always wins when rollup files exist for that step.

### Grafana view SQL

`query.ViewSQL(dataDir, tenant)` emits `CREATE OR REPLACE VIEW prism_<tenant> AS …`
unioning the tenant hot snapshot (`hot/current.parquet`) and present tier globs,
path-scoped, same row projection, no `union_by_name`/`filename`.

CLI: `prism-store print-view-sql --tenant <ns> [--data-dir <dir>]` prints the
statement for DuckDB datasource `initSQL` wiring.

---

## Arbitrary SQL API (`internal/store/query`, `sql.go`)

Read-only **arbitrary SQL** over a single tenant's metrics, RBAC-guarded
(action **`query`**, same plane as structured query). Each request runs in a
**fresh in-memory DuckDB sandbox** (never shared across tenants or requests).

### HTTP

`POST <ROUTE_PREFIX>/{tenant}/sql`

Request JSON: `{"sql": "<single SELECT or WITH>", "max_rows": <optional int>}`.

Success `200 application/json`:

```json
{"columns":["…"],"rows":[[…],…],"row_count":N,"truncated":false}
```

| Condition | Status |
|---|---|
| malformed JSON / empty SQL / non-SELECT / multi-statement / exec error | `400 bad query` |
| unknown/malformed tenant / missing tenant root | `404 unknown tenant` |
| internal failure (snapshot, sandbox) | `500 query failed` |

When **`SQL_API_ENABLED=false`** (default `true`), the route is not registered
(`404`).

### Relation and schema

| Relation | Schema |
|---|---|
| `metrics` | `"__name__"`, `labels`, `value`, `timestamp_ms`, `ts` |

Built from the tenant's **`hot/current.parquet`** snapshot (exported per request)
plus present **`tiers/L*/*.parquet`** globs — same union shape as structured
query / Grafana view SQL. Visibility: committed hot (as of snapshot) + all tiers.

### Sandbox guarantees

Per request, on a dedicated `:memory:` DuckDB connection (settings applied in
order; **`lock_configuration=true` last**):

1. `SET memory_limit` — from `DUCKDB_MEMORY_LIMIT` when set
2. `SET max_temp_directory_size='0B'`
3. `LOAD parquet` (when needed)
4. `SET allowed_directories=['<abs tenantRoot>']` — best-effort (DuckDB ≥1.2)
5. Materialize `metrics` from tenant parquet under `tenantRoot`
6. `SET enable_external_access=false` + extension hardening knobs
7. `SET lock_configuration=true`

User SQL cannot re-enable external access or reach paths outside the tenant.
Cross-tenant `read_parquet`, `ATTACH`, `COPY`, host reads, and writes fail.
Defense-in-depth: only `SELECT`/`WITH` accepted upfront.

References: [DuckDB — Securing DuckDB](https://duckdb.org/docs/stable/operations_manual/securing_duckdb/overview);
OWASP API1 BOLA.

### Limits

| Env | Default | Effect |
|---|---|---|
| `SQL_API_MAX_ROWS` | `100000` | Server cap; `min(request.max_rows, cap)` |
| `SQL_API_TIMEOUT_SECONDS` | `30` | Query timeout (context cancel) |
| `SQL_API_ENABLED` | `true` | Register route when `true` |
| `DUCKDB_MEMORY_LIMIT` | _(empty)_ | Sandbox memory cap when set |

### Auth

Same as structured query: RBAC **`query`** when `AUTHZ_POLICY_FILE` is set; else
`ADMIN_TOKEN` on the admin plane. Cluster coordinators forward `POST /{ns}/sql`
to the owning client (authorize-before-proxy; client re-enforces).

---

## Admin provisioning (`internal/store/admin`, `internal/store/seed`)

Tenant lifecycle and billing metering endpoints ported from the homelab proxy.
Handlers live in `internal/store/admin`; zero-row Parquet seeds in
`internal/store/seed`.

### Control-plane bind

When `ADMIN_LISTEN_ADDR` is set, a **second** `http.Server` serves admin,
stats, and query routes; the public `LISTEN_ADDR` server keeps ingest plus
health probes only. When unset, all routes share the single mux (dev/back-compat).
Both planes serve `/healthz` and `/readyz`. Graceful shutdown stops both servers.

When `ADMIN_TOKEN` is set, admin-plane routes (`/admin/*`, `/stats`, and query
when on the admin plane) require `Authorization: Bearer <token>` (constant-time
compare). When unset, no auth — rely on network isolation (NetworkPolicy/gateway).

### `POST /admin/tenants/{ns}/ensure`

Idempotently seeds zero-row Parquet placeholders so DuckDB `read_parquet` globs
never error on empty tenants.

| Condition | Status |
|---|---|
| unknown/malformed tenant | `404 unknown tenant` |
| seed or engine failure | `500 ensure failed` |
| success (including repeat calls) | `204 No Content` |

Seeds written (all idempotent, atomic `.tmp` + rename):

- `<tenant>/metrics-raw/_seed.parquet` — embedded contract-v1 zero-row fixture
- `<tenant>/tiers/_seed.parquet`, `hot/current.parquet`
- `<tenant>/rollups/{1m,5m,1h}/_seed.parquet` — zero-row rollup schema

Reserved name: `_seed.parquet` (`seed.SeedName`).

### `GET /stats?ns=` — billing contract

**Frozen JSON shape** (field names, casing, and `omitempty` must not change —
consumers scrape this for credit metering):

```json
{
  "artifacts": {
    "<artifact>": {
      "windows": 0,
      "latestUnixNanos": 0
    }
  },
  "totalWindows": 0,
  "onDiskBytes": 0,
  "compactionCpuSeconds": 0.0
}
```

`onDiskBytes` and `compactionCpuSeconds` appear **only when `ns` is provided**
(`omitempty` omits zero values). Without `ns`, the response aggregates across
all tenant directories under `DATA_DIR`.

Per-tenant `windows` = hot row count + L0 segment count. `latestUnixNanos` =
max L0 file mtime. `onDiskBytes` from `stats.TenantOnDiskBytes` (excludes
legacy `metrics-raw/`). `compactionCpuSeconds` from `.metering.json`.

| Condition | Status |
|---|---|
| non-empty unknown `ns` | `404` |
| success | `200 application/json` |

---

## Health endpoints

| Route | Success | Failure |
|---|---|---|
| `GET /healthz` | `200` body `ok\n` | — |
| `GET /readyz` | `200` body `ready\n` after `MkdirAll(DATA_DIR)` | `503` when the data dir is not writable |

Graceful shutdown: `SIGINT` / `SIGTERM` → `Shutdown` with a 10s timeout.

---

## Sizing

Measured on prod node `sunset` (`metrics-raw`, zstd Parquet). Use these ratios — not
a flat “10 MB per 5k/s” rule (that understates DuckDB hot-window memory by ~100×).

| Signal | Measurement |
|--------|-------------|
| Idle | ~0.0004 cores, ~8 MiB RSS |
| Ingest @ 2,000 samples/s (1-min hot window) | ~0.026 cores, ~341 MiB RSS |
| Compaction (per L-merge) | ~3.5 CPU-seconds burst |
| Compacted size | ~95 bytes/sample |

**CPU (ingest):** ≈ **0.013 cores per 1,000 samples/s** sustained. Merges are bursty
(≤ ~1 core for a few seconds) — size the *limit*, not the request, for merge headroom.

**Memory:** dominated by the hot window + DuckDB arena:

`mem ≈ 128 MiB + (ingest_rate × HOT_WINDOW_seconds × ~0.4 KiB/row) × ~1.5`

Example: 5,000 samples/s @ 5-min hot window ≈ 128 MiB + 5000×300×0.4 KiB×1.5 ≈ **~1 GiB**.

**Storage:** ≈ `95 bytes/sample × rate × retention_days × 86,400 × ~1.3` (rollups +
overhead). Example: 1,000 samples/s × 15 d ≈ **~160 GiB**.

**Levers:** shrink `HOT_WINDOW` at high ingest rates; cap `MAX_SEGMENT_BYTES` (512 MiB–1 GiB
recommended at scale, down from the 2 GiB code default) to bound transient merge RAM.

**Recommended Helm defaults** (central pod aggregate rate; chart default = ≤1k/s row):

| Aggregate ingest | cpu req | cpu limit | mem req | mem limit | HOT_WINDOW | MAX_SEGMENT_BYTES | PVC (15d) |
|------------------|---------|-----------|---------|-----------|------------|-------------------|-----------|
| ≤ 1k samples/s | 250m | 2 | 512Mi | 2Gi | 10m | 1Gi | 32Gi |
| 1–5k/s | 500m | 3 | 1Gi | 3Gi | 5m | 1Gi | 128Gi |
| 5–15k/s | 1 | 4 | 2Gi | 6Gi | 3m | 512Mi | 512Gi |
| 15–40k/s | 2 | 6 | 4Gi | 10Gi | 2m | 512Mi | 1Ti+ |

Chart: `deploy/charts/prism-store/` (`values.yaml` comments mirror this table). Consumer
gateway/secret/Grafana overlays live under `deploy/charts/prism-store/examples/` — not
installed by the base chart.

---

## Related docs

- [`OUTPUT_CONTRACT.md`](OUTPUT_CONTRACT.md) — artifact taxonomy and Parquet schemas.
- [`DESIGN.md`](DESIGN.md) §15 — ADR (naming, monorepo layout, CGO decision).
- [`MIGRATION.md`](MIGRATION.md) — `prism-proxy` → `prism-store` cutover plan + env map (#30).
