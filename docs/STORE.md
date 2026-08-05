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

### Features

| Area | Capability |
|---|---|
| **Ingest** | HTTP `POST /{ns}/ingest/{artifact}` (Parquet windows) and optional Arrow Flight `DoPut` when `FLIGHT_ADDR` is set — shared validation chain, lands in `hot_current`. |
| **Hot window** | Time-bounded ingest buffer (`HOT_WINDOW_*`); rolled to `hot_prev`, flushed to L0 on schedule (`FLUSH_TICK_SECONDS`) or opportunistically on ingest. |
| **Hot snapshot** | Near-real-time export of `hot_current` to `hot/current.parquet` (`HOT_SNAPSHOT_SECONDS`). |
| **Tiered storage** | Immutable Parquet segments `L0`…`L{n}` (`MAX_TIER`); Lucene-style merge compaction when `SEGMENTS_PER_TIER` reached. |
| **Merges** | Background tier merges (`MERGE_TICK_SECONDS`); honors `DUCKDB_THREADS` / `DUCKDB_MEMORY_LIMIT`. |
| **Rollups** | Downsampled Parquet under `rollups/{step}/` after L1+ merges (`ROLLUP_STEPS`); same DuckDB caps as merges. |
| **Retention** | Deletes expired tier segments and rollups (`RETENTION_DAYS`, retention ticker). |
| **Structured query** | `GET /{ns}/query?start=&end=&step=` — union over hot + tiers + rollups; optional hot-only (`QUERY_HOT_ONLY`). |
| **Arbitrary SQL** | `POST /{ns}/sql` — read-only SQL in a per-request sandbox; JSON (default) or Arrow IPC stream on the **same route** via `Accept`. |
| **PromQL API** | `GET`/`POST /{ns}/api/v1/{query,query_range,series,labels,label/<name>/values}` — Prometheus-compatible read API over the tenant metrics view (`PROMQL_API_ENABLED`, default on). Metrics-only. |
| **Loki logs API** | `GET`/`POST /{ns}/loki/api/v1/{query_range,labels,label/<name>/values}` — Loki-compatible read API over the tenant `logs` relation, LogQL subset (`LOKI_API_ENABLED`, default on). Logs-only. |
| **SQL queue** | Optional in-flight limiter (`SQL_API_QUEUE_ENABLED`, default off) with `429` + `Retry-After` backpressure on data nodes. |
| **RBAC** | Optional JWT/OIDC + deny-by-default YAML policy (`AUTHZ_POLICY_FILE`); fixed roles `reader` / `writer` / `admin`. |
| **Cluster modes** | `MODE=standalone` (all-in-one), `client` (owned tenants + local engine), `cluster` (stateless coordinator proxy). |
| **Metering / stats** | `GET /stats?ns=` — frozen billing JSON + on-disk bytes and compaction CPU seconds. |
| **Reader/writer split** | `RUN_JOBS=false` disables background maintenance on a node (query/ingest only). |
| **Tenant engine LRU** | `MAX_OPEN_TENANTS` caps resident per-tenant DuckDB handles. |

Memory sizing: [`MEMORY.md`](MEMORY.md). Full env reference: [`CONFIG.md`](CONFIG.md) §14.

### Deployment modes (`MODE`)

Bootstrap **`MODE`** selects one of three roles (default **`standalone`**):

| Mode | Role | Data | Query routing |
|---|---|---|---|
| `standalone` | Self-contained node | Engine + ingest + jobs on `DATA_DIR` | Local engine |
| `client` | Data-holding leaf fronted by a coordinator | Same as standalone | Local engine, but only for tenants listed in `CLIENT_TENANTS`; other tenants → `404` before the engine runs |
| `cluster` | Stateless coordinator/router | **None** — no engine, ingest, or background jobs | Forwards `GET` and `POST` `<prefix>/{ns}/query`, `{ns}/sql`, `{ns}/api/v1/*`, and `{ns}/loki/api/v1/*` to the owning client URL from `CLUSTER_CLIENTS` |

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
leaves). See [RBAC](#rbac) below.

**Future / out of scope:** routing ingest, admin, `/stats`, or `/ensure`
through the coordinator; scatter-gather across clients; dynamic service
discovery; per-client mTLS; health-aware routing and failover.

---

## RBAC

Self-contained guide for operators and integrators. RBAC is **optional** — set
`AUTHZ_POLICY_FILE` to enable; leave unset for legacy static-token auth.

### When RBAC is on

HTTP query, ingest, ensure, stats, and `/sql` routes require:

1. A verified **JWT** in `Authorization: Bearer <token>`.
2. A **binding** in the mounted policy file granting the requested **action** on
   the path **tenant** (`ns`).

Static `ADMIN_TOKEN` / `INGEST_TOKEN` gates are **not** used on those HTTP
routes when RBAC is enabled. `AUTH_MODE` still governs **Arrow Flight** independently.

### Identity (JWT / OIDC)

Configuration (all required when RBAC is on except JWKS source):

| Env | Purpose |
|---|---|
| `OIDC_ISSUER` | Expected JWT `iss`; used for OIDC discovery when JWKS file/URL unset. |
| `OIDC_AUDIENCE` | Comma-separated accepted `aud` values (bind k8s SA tokens to this audience). |
| `OIDC_JWKS_URL` | Optional static JWKS URL instead of discovery. |
| `OIDC_JWKS_FILE` | Optional mounted JWKS JSON (air-gapped / Vault-rendered). |

Verification behavior:

- Signature checked against JWKS (discovery, URL, or file).
- Validates `iss`, `aud`, `exp` / `nbf` / `iat`.
- **`alg=none` and algorithm-confusion (e.g. HMAC vs RSA) tokens are rejected.**
- Principal = non-empty JWT `sub` claim.
- Identity headers (`X-Tenant`, `X-User`, …) are **ignored** — only the JWT counts.

Missing or invalid JWT → **`401 unauthorized`**.

### Roles and HTTP actions

Three fixed roles; custom roles are not supported.

| Role | HTTP actions |
|---|---|
| `reader` | `query` — `GET /{ns}/query`, `POST /{ns}/sql` |
| `writer` | `ingest` — `POST /{ns}/ingest/{artifact}` |
| `admin` | `query`, `ingest`, `ensure`, `stats` — all of the above plus `POST /admin/tenants/{ns}/ensure` and `GET /stats` |

A principal with `reader` on tenant A cannot ingest, ensure, or read stats for A.
A `writer` cannot query. Only `admin` covers provisioning and billing stats.

### Policy file format

Deny-by-default YAML mounted at `AUTHZ_POLICY_FILE`:

```yaml
bindings:
  - subject: "system:serviceaccount:team-a:ingest"
    role: writer
    tenants: ["team-a"]
  - subject: "alice@corp.example"
    role: reader
    tenants: ["team-a", "team-b"]
  - subject: "platform-admin"
    role: admin
    tenants: ["*"]
```

- **`subject`** — must match JWT `sub` exactly.
- **`role`** — `reader`, `writer`, or `admin` only.
- **`tenants`** — list of namespace strings, or `["*"]` for all tenants (high blast radius).

Invalid policy at **startup** → process exit. Invalid policy on **reload** → keep
last-good policy and log (never fail-open).

**Hot reload:** poll file mtime every `AUTHZ_RELOAD_SECONDS` (default `15`).

### HTTP status semantics (anti-enumeration)

| Condition | Status | Body |
|---|---|---|
| Missing / invalid JWT | `401` | `unauthorized` |
| Authenticated, no binding for tenant | `404` | `unknown tenant` |
| Bound for tenant, role lacks action | `403` | `forbidden` |

Unauthorized tenants return the **same** `404` body as a malformed or unknown
tenant (`unknown tenant`) — byte-identical across handlers, cluster router, and
client guard. This blocks tenant enumeration (OWASP BOLA).

### `/stats` scoping

- `GET /stats?ns=X` — requires `stats` action on `X`; else `404` or `403` as above.
- `GET /stats` (no `ns`) — aggregates only tenants the principal may `stats` on;
  `*`-admin sees all; principals with no `stats` scope → `403`.

### Arrow Flight (fail-fast)

RBAC middleware covers **HTTP only**. Flight `DoPut` keeps operator `AUTH_MODE`
(`bearer`, `mtls`, `trusted-header`).

If `AUTHZ_POLICY_FILE` is set **and** `FLIGHT_ADDR` is set **and**
`AUTH_MODE=none`, startup **fails** with an explicit error. Operators must either:

- Configure non-`none` Flight auth, or
- Disable Flight (`FLIGHT_ADDR` unset).

HTTP ingest under RBAC always uses JWT regardless of `AUTH_MODE`.

### Cluster defense-in-depth

| Layer | Behavior |
|---|---|
| **Coordinator** (`MODE=cluster`) | Authenticate + authorize **before** reverse-proxy; denied tenants get `401`/`403`/`404` with **no upstream contact**; forwards original JWT. No engine, ingest, jobs, or `/sql` queue on the coordinator. |
| **Client** (`MODE=client`) | Re-verify JWT and re-authorize; **`OwnedTenantGuard`** returns `404` for tenants not in `CLIENT_TENANTS` before the engine runs. |

### Kubernetes integration

1. Mount policy YAML (ConfigMap or Vault Agent template) and set
   `AUTHZ_POLICY_FILE=/etc/prism/rbac/policy.yaml`.
2. Issue **projected ServiceAccount tokens** with `audience` matching
   `OIDC_AUDIENCE` (align with API server `--service-account-issuer`).
3. Set `OIDC_ISSUER` to the cluster issuer URL, or mount JWKS via
   `OIDC_JWKS_FILE` when discovery is unavailable.
4. Bind each workload's SA `sub` (`system:serviceaccount:ns:name`) in the policy.

Example pod env fragment:

```yaml
env:
  - name: AUTHZ_POLICY_FILE
    value: /etc/prism/rbac/policy.yaml
  - name: OIDC_ISSUER
    value: https://kubernetes.default.svc.cluster.local
  - name: OIDC_AUDIENCE
    value: prism-store
volumeMounts:
  - name: rbac-policy
    mountPath: /etc/prism/rbac
    readOnly: true
```

### Vault / secrets integration

1. **Policy file** — Vault Agent (or CSI provider) renders `policy.yaml` into a
   shared volume; point `AUTHZ_POLICY_FILE` at the rendered path.
2. **JWKS** — render static JWKS to a file (`OIDC_JWKS_FILE`) or expose a
   Vault OIDC/JWT auth JWKS URL (`OIDC_JWKS_URL`).
3. **Client tokens** — workloads obtain short-lived JWTs from Vault (or k8s SA
   tokens) with `aud` matching `OIDC_AUDIENCE`; rotate by updating bindings or
   token TTL, not store restarts.
4. On policy rotation, rely on hot reload; on JWKS rotation, update the mounted
   file or URL — verifier picks up keys on next validation.

---

## Ingest (`internal/store/ingest`)

Write entry point for contract-v1 Parquet windows. Two transports share one
validation chain and land via `engine.Ingest`.

### HTTP

`POST <ROUTE_PREFIX>/{tenant}/ingest/{artifact}` — raw Parquet body
(`application/octet-stream`). Empty body → `204` no-op.

**Logs artifacts** (`logs-raw`/`logs-template`/`logs-summary`, opt-in via
`ALLOWED_ARTIFACTS`) skip the metrics hot catalog: each window is landed as an
immutable file under `<tenant>/logs/<artifact>/` and read back through the
`logs` relation on `/sql`. Metrics ingest is unchanged. Both HTTP and Flight
ingest land logs the same way.

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

All `prism-store` environment variables are documented in
[`CONFIG.md`](CONFIG.md) §14 (authoritative table). Memory model and sizing:
[`MEMORY.md`](MEMORY.md).

Notable groups:

- **Listen / routing:** `LISTEN_ADDR`, `ADMIN_LISTEN_ADDR`, `ROUTE_PREFIX`, `MODE`, `CLIENT_TENANTS`, `CLUSTER_CLIENTS`
- **Ingest / Flight:** `DATA_DIR`, `ALLOWED_ARTIFACTS`, `MAX_BODY_BYTES`, `FLIGHT_ADDR`, `AUTH_MODE`, `INGEST_TOKEN`
- **Hot / lifecycle:** `HOT_WINDOW_*`, tickers, `SEGMENTS_PER_TIER`, `MAX_SEGMENT_BYTES`, `RETENTION_*`, `ROLLUP_STEPS`, `MAX_TIER`, `RUN_JOBS`
- **Query / SQL:** `QUERY_HOT_ONLY`, `SQL_API_*`, `SQL_API_QUEUE_*`, `E2E_EXPOSE_QUERY_SQL`
- **DuckDB governance:** `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`, `MAX_OPEN_TENANTS`
- **RBAC:** `AUTHZ_POLICY_FILE`, `OIDC_*`, `AUTHZ_RELOAD_SECONDS`
- **Legacy admin token:** `ADMIN_TOKEN` (RBAC off only)

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

Open DuckDB handles are cached in a bounded LRU sized by `MAX_OPEN_TENANTS`
(default `32`). Eviction closes the oldest connection. A single mutex guards the
LRU and the per-tenant `flushAt` schedule map. `Close()` evicts all handles.
Each open tenant engine uses `SetMaxOpenConns(1)`.

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

Lucene **TieredMergePolicy** analogue over immutable Parquet tiers. Merge DuckDB
connections honor `DUCKDB_THREADS` and `DUCKDB_MEMORY_LIMIT` from the store config.

- **Seal:** segments with `Bytes ≥ MAX_SEGMENT_BYTES` (default 2 GiB) are never merge inputs.
- **Trigger:** when a tier has ≥ `SEGMENTS_PER_TIER` (default 6) live segments, the planner groups by size level (floor-rounded log scale), picks the first time-adjacent contiguous run (gap ≤ one segment span), and shrinks the candidate set down to 1 if needed so summed bytes ≤ `MAX_SEGMENT_BYTES`.
- **One action per tick:** no cascade — at most one merge per tenant per merge tick, lowest tier first.
- **Promotion:** merged output lands in `L{dest}` with rows ordered by `ts`; source files are deleted only after the output is atomically renamed.

Path helpers live in `internal/store/layout` (`TierDir`, `RollupDir`, `ToSlash`).

### Rollups (`internal/store/rollup`)

After a merge promotes to **L1 or above**, the store materializes downsampled
Parquet under `rollups/{step}/` for each step in `ROLLUP_STEPS` (default
`1m,5m,1h`). Schema: `bucket`, `"__name__"`, `avg`, `min`, `max`, `count`, `sum`
from `time_bucket(step, ts)` grouped by bucket and name. L0 merges do not build
rollups (avoids rework on volatile data). Rollup DuckDB workers apply the same
`DUCKDB_THREADS` / `DUCKDB_MEMORY_LIMIT` as the engine and merges.

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
queries. The same flag also constrains the **SQL API sandbox** (`POST /{tenant}/sql`):
the `metrics` view unions only `hot/current.parquet` and skips `tiers/L*/*.parquet`.
By default the structured query unions hot + tiers + rollups; the `/sql` sandbox
unions hot + tiers only (never rollups — see "When rollups help").

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

### When rollups help (and who reads them)

A rollup trades **label fidelity and raw resolution** for a large reduction in
rows scanned: it pre-aggregates each metric into `(bucket, "__name__")` buckets
holding `avg/min/max/count/sum`, so **all labels are dropped**. That shape
determines which readers can use them:

| Reader | hot + `tiers/L*` (raw, labeled) | `rollups/{step}` (downsampled, no labels) |
|---|---|---|
| Structured query `GET /{ns}/query` | ✅ | ✅ (auto-selected by range width, or explicit `step`) |
| Arbitrary SQL `POST /{ns}/sql` | ✅ | ❌ (sandbox `metrics` view unions hot + tiers only) |
| PromQL `GET`/`POST /{ns}/api/v1/*` | ✅ | ❌ (needs per-series labels) |

**Only the structured query endpoint reads rollups.** `/sql` and PromQL always
read the raw hot + tier union, so they never return silently-delabeled results.
Rollups are metrics-only — logs never take the rollup path.

**Worth using when:**

- Wide-range structured-query panels (hours → weeks): a 30-day overview reads
  pre-bucketed `1h` rows instead of millions of raw samples.
- Aggregate-only questions where per-metric `avg/min/max/count/sum` is enough
  (capacity trends, long-horizon SLO lines), not a per-label breakdown.
- High-cardinality metrics over long windows, where the bucket collapse to one
  row per `(bucket, "__name__")` is the point.

**Not worth using (raw is better) when:**

- You need per-label detail or filtering (`by (instance)`, `{job="api"}`) — the
  labels are gone.
- Short/recent ranges (< 1h): auto-selection returns raw tiers anyway.
- Alerting or exact values, where bucket aggregates lose precision.
- Consumption is primarily **PromQL** (e.g. Grafana's Prometheus datasource) or
  `/sql` — neither path reads rollups, so `ROLLUP_STEPS` is pure background
  compute + disk for those tenants. Disable rollups (empty `ROLLUP_STEPS`) if no
  structured-query dashboards rely on them.

### Grafana view SQL

`query.ViewSQL(dataDir, tenant)` emits `CREATE OR REPLACE VIEW prism_<tenant> AS …`
unioning the tenant hot snapshot (`hot/current.parquet`) and present tier globs,
path-scoped, same row projection, no `union_by_name`/`filename`.

CLI: `prism-store print-view-sql --tenant <ns> [--data-dir <dir>]` prints the
statement for DuckDB datasource `initSQL` wiring.

---

## Arbitrary SQL API

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

**Arrow IPC stream (optional):** send
`Accept: application/vnd.apache.arrow.stream` to receive a streaming Arrow IPC
response (`Content-Type: application/vnd.apache.arrow.stream`) instead of JSON.
JSON remains the default when `Accept` is absent, `*/*`, or `application/json`.
The stream carries the same sandboxed query result; row-cap truncation is
signaled by the HTTP trailer **`X-Prism-Truncated: true|false`** (declared in
the `Trailer` response header). Clients must read the full response body before
trailer fields are available. Mid-stream failures after `200` terminate the IPC
stream (status cannot change).

Arrow transport requires building **`prism-store`** with the **`duckdb_arrow`**
build tag (CGO enabled). Production `Makefile` / release targets include it;
`go build ./...` without the tag returns **`406 Not Acceptable`** for Arrow
`Accept` requests (stub). RBAC action **`query`** applies identically.

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
| `logs` | `message`, `format` (guaranteed) + per-format/`template`/`count` columns (varies) |

The **`logs`** relation unions the tenant's landed `logs-*/*.parquet` windows with
`union_by_name=true` (variable per-format schemas; missing columns are NULL). It
is present in every sandbox (empty — zero rows — when a tenant has no logs), is
tenant-scoped like `metrics`, and is unaffected by `QUERY_HOT_ONLY` (logs have no
hot/tier split). Logs are **file-backed**: they never enter the metrics hot
catalog, so they need no hot snapshot export and a **reader** node
(`RUN_JOBS=false`, read-only mount of the writer's `DATA_DIR`) serves them in
full — the same rows the writer sees, as soon as a window lands. `make loki-e2e`
runs exactly that topology. Typical use — total count per mined template:

```sql
SELECT template, CAST(sum(count) AS BIGINT) AS count FROM logs GROUP BY template ORDER BY count DESC
```

The `metrics` relation is built from the tenant's **`hot/current.parquet`** snapshot (exported per request)
plus present **`tiers/L*/*.parquet`** globs when `QUERY_HOT_ONLY` is off — same
union shape as structured query / Grafana view SQL. With `QUERY_HOT_ONLY=true`,
only the hot snapshot is included. Visibility: committed hot (as of snapshot) + all
tiers (tiers omitted in hot-only mode).

### Sandbox guarantees

Per request, on a dedicated `:memory:` DuckDB connection (settings applied in
order; **`lock_configuration=true` last**). Bundled DuckDB is **≥1.2** via
`go-duckdb/v2`.

1. `SET threads` / `SET memory_limit` — from config when set
2. `SET max_temp_directory_size='0B'`
3. `LOAD parquet`
4. `SET allowed_directories=['<abs tenantRoot>']` — required read boundary
5. `CREATE VIEW metrics AS …` — lazy union over hot snapshot + tier parquet
   (zero-copy; no materialization)
6. `SET enable_external_access=false` + extension hardening knobs
7. `SET lock_configuration=true`

User SQL cannot re-enable external access or reach paths outside the tenant.
Cross-tenant `read_parquet`, `ATTACH`, `COPY`, host reads, and writes fail.
Defense-in-depth: only `SELECT`/`WITH` accepted upfront; `SET`/`RESET`/`PRAGMA`
rejected before execution. Tenant directories that are symlinks or resolve outside
`DATA_DIR` are treated as unknown tenants (`404`).

**Residual (within-tenant only):** catalog introspection (`duckdb_views()`,
`information_schema`, etc.) may expose absolute parquet paths under the tenant
root; this does not cross tenant boundaries.

References: [DuckDB — Securing DuckDB](https://duckdb.org/docs/stable/operations_manual/securing_duckdb/overview);
OWASP API1 BOLA.

### Limits

| Env | Default | Effect |
|---|---|---|
| `SQL_API_MAX_ROWS` | `100000` | Server cap; `min(request.max_rows, cap)` |
| `SQL_API_TIMEOUT_SECONDS` | `30` | Query timeout (context cancel) |
| `SQL_API_MAX_BODY_BYTES` | `1048576` | Maximum JSON request body (1 MiB) |
| `SQL_API_ENABLED` | `true` | Register route when `true` |
| `DUCKDB_MEMORY_LIMIT` | _(empty)_ | Sandbox (and engine) memory cap when set |
| `DUCKDB_THREADS` | _(empty)_ | Sandbox (and engine) thread cap when `> 0` |
| `SQL_API_QUEUE_ENABLED` | `false` | Enable in-flight limiter (data nodes only) |
| `SQL_API_MAX_INFLIGHT` | `4` | Max concurrent `/sql` when queue on |
| `SQL_API_MAX_QUEUE` | `64` | Max waiters when queue on |
| `SQL_API_QUEUE_TIMEOUT_MS` | `5000` | Max wait for slot; then `429` |

#### In-flight queue (optional)

When `SQL_API_QUEUE_ENABLED=true` on a **data node** (standalone or client —
**not** the cluster coordinator), middleware order is:

**auth → `OwnedTenantGuard` → limiter → `SQLHandler`**

Cheap auth/guard rejections do not consume a slot. At most
`SQL_API_MAX_INFLIGHT` requests execute concurrently; additional requests wait up
to `SQL_API_QUEUE_TIMEOUT_MS` for a slot, with at most `SQL_API_MAX_QUEUE`
waiters. When the wait queue is full, wait times out, or the client cancels →
**`429 Too Many Requests`**, body `too many concurrent queries`, header
**`Retry-After: 1`**.

Default **off** — zero behavior change from prior releases. One shared limiter
serves public and admin HTTP planes on the same process.

Sizing: see [`MEMORY.md`](MEMORY.md) (`MAX_INFLIGHT × DUCKDB_THREADS ≈ cores`).

### Auth

Same as structured query: RBAC **`query`** when `AUTHZ_POLICY_FILE` is set; else
`ADMIN_TOKEN` on the admin plane. Cluster coordinators forward `POST /{ns}/sql`
to the owning client (authorize-before-proxy; client re-enforces).

---

## PromQL API

Prometheus-compatible **read** API over a single tenant's metrics, so any
Prometheus exporter (scraped by the `prism` agent) can be queried with PromQL and
consumed by Grafana's Prometheus datasource or any PromQL client. It embeds the
canonical `github.com/prometheus/prometheus/promql` engine over a DuckDB-backed
`storage.Queryable` that reads the **same per-request sandbox `metrics` view** as
`/sql` (identical tenant isolation, hot-only, and DuckDB caps). It is **read-only
and additive**: no ingest, agent, or output-contract change.

`cmd/prism-alert` is the in-repo consumer of this API: a per-tenant PromQL ruler
that evaluates alerting rules against `/{ns}/api/v1/query` and posts
Alertmanager v4 webhooks to the prism notifier. See [`ALERTING.md`](ALERTING.md).

### HTTP

| Method | Path | Purpose |
|---|---|---|
| `GET`/`POST` | `<prefix>/{ns}/api/v1/query` | Instant query at `time` (default now). |
| `GET`/`POST` | `<prefix>/{ns}/api/v1/query_range` | Range query over `start`..`end` at `step`. |
| `GET`/`POST` | `<prefix>/{ns}/api/v1/series` | Series matching one or more `match[]` selectors. |
| `GET`/`POST` | `<prefix>/{ns}/api/v1/labels` | Label names (optional `match[]`, `start`/`end`). |
| `GET` | `<prefix>/{ns}/api/v1/label/{name}/values` | Values of a label. |

Repeated `match[]` selectors **union** (OR), and `series`/`labels`/`label
values` honor the Prometheus `limit` result cap (early-stop, so the response
stays bounded by `limit` rather than tenant cardinality).

**`hot_only` extension** — any endpoint accepts an optional `hot_only` param
(`true`/`1`/`yes`/`on`). It restricts that single request to the hot snapshot,
skipping cold Parquet tiers. It can only **tighten** scope: on a store already
running with `QUERY_HOT_ONLY=true` the param is a no-op (a request can never
widen back to the tiers). `prism-alert` sets it on every evaluation so recurring
rules stay cheap and never touch cold storage (see [`ALERTING.md`](ALERTING.md)).

Responses use the exact Prometheus envelope: `{"status":"success","data":{"resultType":"vector|matrix|scalar|string","result":…}}`;
errors are `{"status":"error","errorType":"…","error":"…"}`.

| Condition | Status / errorType |
|---|---|
| missing/invalid param, malformed expression, `> PROMQL_MAX_POINTS` steps | `400` / `bad_data` |
| unknown/malformed tenant / missing tenant root | `404` (`unknown tenant`) |
| evaluation error, `> PROMQL_MAX_SAMPLES` samples | `422` / `execution` |
| query timeout / cancellation | `503` / `timeout`\|`canceled` |

### Semantics and memory

- **Time axis:** the stored ingest `ts` is the sample timestamp (the same axis as
  structured query / `/sql`).
- **Labels:** the opaque `labels` text is parsed into a Prometheus label set at
  query time — no schema or contract change, and it works on all existing Parquet.
- **Pushdown:** a `__name__` equality matcher and the time bounds are pushed into
  DuckDB with `ORDER BY "__name__", labels, ts`; the adapter streams the sorted
  cursor into series (peak memory is one series, not the whole result set) and
  applies the remaining matchers in Go. `series`/`labels` endpoints read only
  distinct `(name, labels)` pairs, so they stay bounded by series cardinality.
- **Bounds:** `PROMQL_MAX_SAMPLES` (default 50,000,000, mirrors Prometheus
  `--query.max-samples`) caps samples per query; `QUERY_HOT_ONLY` restricts to the
  hot snapshot; the optional `/sql` in-flight queue also gates PromQL. Rollup
  projections are intentionally excluded (they drop labels).

### Auth, cluster, gating

RBAC action **`query`** (same as structured query / `/sql`). Cluster coordinators
forward every `/{ns}/api/v1/*` pattern to the owning client (authorize-before-proxy;
client re-enforces via the owned-tenant guard). Registered when
`PROMQL_API_ENABLED=true` (default); the API is metrics-only, so logs-only
deployments simply never receive PromQL traffic.

---

## Loki logs API

Loki-compatible **read** API over a single tenant's `logs` relation, so a stock
Grafana **Loki datasource** can browse prism logs the same way the Prometheus
datasource browses prism metrics. It queries the **same per-request DuckDB
sandbox** `/sql` uses (identical tenant isolation and DuckDB caps) — there is no
second storage engine. Both surfaces share one list-based `read_parquet([...],
union_by_name=true)` relation builder (not a per-file `UNION`), so expression
depth stays O(1) as window count grows.

### Logs lifecycle (parity with metrics)

Landed windows live under `<tenant>/logs/<artifact>/*.parquet`. Background jobs
(`RUN_JOBS=true`) also compact logs:

| Path | Role |
|---|---|
| `logs/<artifact>/*.parquet` | Landing windows (agent seals) |
| `logs/<artifact>/tiers/L{n}/*.parquet` | Compacted segments (merge when ≥ `SEGMENTS_PER_TIER`) |
| `logs/<artifact>/_manifest.json` | Atomic file catalog for planners |
| `logs/.meta_generation` | Cache invalidation stamp |

Planners **time-prune** the open set using the window id in
`<unix_ns>-*.parquet` (else mtime) before opening Parquet. Label APIs omit the
`message` column and prefer an in-process cardinality index for
`format`/`template`/`stream`/`job`. Optional knobs: `MAX_LOG_FILES`,
`LOG_COALESCE_MAX_*`, `LOGS_RECENT_LOOKBACK_HOURS`, `QUERY_DUCKDB_THREADS`
(see [`CONFIG.md`](CONFIG.md)).

Reference: [Grafana Loki HTTP API](https://grafana.com/docs/loki/latest/reference/loki-http-api/).

### HTTP

| Method | Path | Purpose |
|---|---|---|
| `GET`/`POST` | `<prefix>/{ns}/loki/api/v1/query_range` | Log entries over `start`..`end`. |
| `GET`/`POST` | `<prefix>/{ns}/loki/api/v1/labels` | Stream label names present in the window. |
| `GET`/`POST` | `<prefix>/{ns}/loki/api/v1/label/{name}/values` | Values of one stream label. |

Success bodies use the Loki envelope
`{"status":"success","data":{"resultType":"streams","result":[{"stream":{…},"values":[["<ns>","<line>"],…]}],"stats":{}}}`;
the label endpoints put a JSON string array in `data`. Errors are
`{"status":"error","error":"…"}`.

| Parameter | Default | Notes |
|---|---|---|
| `query` | match-all | LogQL subset (below). Empty/missing selector matches every stream. |
| `start` / `end` | `end` = now; `start` = `end - 1h` on `query_range`; on label endpoints omitted `start` means “everything” unless `LOGS_RECENT_LOOKBACK_HOURS` raises the floor | Nanosecond Unix epoch, fractional-second epoch, or RFC3339. `start` inclusive, `end` exclusive. Explicit wide ranges still open cold files. |
| `limit` | `100` | Entries per query; capped by `SQL_API_MAX_ROWS`. |
| `direction` | `backward` | `backward` (newest first) or `forward`. |

| Condition | Status |
|---|---|
| unsupported LogQL, malformed query/params, invalid label name, `end` before `start` | `400` (`status: error`) |
| unknown/malformed tenant / missing tenant root | `404` (`unknown tenant`) |
| query timeout / cancellation | `503` |
| sandbox or scan failure | `500` (`query failed`) |

A provisioned tenant with **no landed logs** answers `200` with an empty
`result`, never a `500`.

### LogQL subset

Supported: a stream selector with `=`, `!=`, `=~`, `!~` label matchers (regex
matchers are fully anchored, as in LogQL) followed by any number of line filters
`|=`, `!=`, `|~`, `!~` (regex filters match a substring). Label predicates and
line filters are pushed into DuckDB, and `limit` bounds the scan, so a request
never materializes more entries than it returns.

```logql
{format="json"} |= "logged in" !~ "healthz"
```

**Not supported** (returns `400` with the reason): metric queries (`rate`,
`count_over_time`, aggregations), parser stages (`| json`, `| logfmt`), label
filters, and formatters. A clear rejection beats a half-answered query.

### Streams, lines, and timestamps

- **Timestamp:** logs Parquet carries **no event-time column** (see
  [`OUTPUT_CONTRACT.md`](OUTPUT_CONTRACT.md) §3.2 — storage stamps ingest time),
  so every row of a landed window is stamped with that window's **ingest time**
  in nanoseconds: prefer the leading `<unix_ns>` from the filename
  (`layout.SegmentName`), else the file mtime.
- **Line:** `message` when it has text, else the mined `template`, else empty.
  Label endpoints never project `message`.
- **Labels:** the remaining **text** columns (`format`, `template`, extracted
  fields) plus a synthetic **`job="prism"`** on every stream, so Grafana's default
  `{job="prism"}` query works out of the box. `count` (from a summary window) is
  exposed as a label string; other numeric columns are not labels. NULL/empty
  values and names that are not legal label names are omitted.
- **Grouping:** rows with an identical label set form one stream; streams are
  returned in a deterministic order.

### Auth, cluster, gating

RBAC action **`query`** (same as structured query / `/sql`), and the optional
`/sql` in-flight queue gates these reads too. Cluster coordinators forward every
`/{ns}/loki/api/v1/*` pattern to the owning client (authorize-before-proxy;
client re-enforces via the owned-tenant guard). Registered when
`LOKI_API_ENABLED=true` (default); the API is logs-only, so metrics-only
deployments simply never receive Loki traffic. Limits are shared with `/sql`:
`SQL_API_MAX_ROWS` caps entries per query, `SQL_API_TIMEOUT_SECONDS` bounds
execution, `DUCKDB_MEMORY_LIMIT` governs memory, and `QUERY_DUCKDB_THREADS`
(falling back to `DUCKDB_THREADS`) governs the query sandbox — merge workers
keep `DUCKDB_THREADS` separately so a raised query thread count does not reopen
merge OOM risk.

Because logs are file-backed, this API is **unaffected by `QUERY_HOT_ONLY`**.
With `RUN_JOBS=true`, logs compaction/retention run alongside metrics jobs; a
read-only reader replica still serves queries from the shared tree.

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

For DuckDB instance caps, `/sql` queue sizing, and the reader/writer split see
[`MEMORY.md`](MEMORY.md).

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

- [`MEMORY.md`](MEMORY.md) — memory model, DuckDB caps, `/sql` queue sizing.
- [`OUTPUT_CONTRACT.md`](OUTPUT_CONTRACT.md) — artifact taxonomy and Parquet schemas.
- [`CONFIG.md`](CONFIG.md) §14 — complete env reference.
- [`DESIGN.md`](DESIGN.md) §15 — ADR (naming, monorepo layout, CGO decision).
- [`MIGRATION.md`](MIGRATION.md) — `prism-proxy` → `prism-store` cutover plan + env map (#30).
