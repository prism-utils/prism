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

## Health endpoints

| Route | Success | Failure |
|---|---|---|
| `GET /healthz` | `200` body `ok\n` | — |
| `GET /readyz` | `200` body `ready\n` after `MkdirAll(DATA_DIR)` | `503` when the data dir is not writable |

Graceful shutdown: `SIGINT` / `SIGTERM` → `Shutdown` with a 10s timeout.

---

## Related docs

- [`OUTPUT_CONTRACT.md`](OUTPUT_CONTRACT.md) — artifact taxonomy and Parquet schemas.
- [`DESIGN.md`](DESIGN.md) §15 — ADR (naming, monorepo layout, CGO decision).
