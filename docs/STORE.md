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
segments, materializes rollups, and exposes read-only query endpoints (query
lands in a later sub-issue).

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
| `RowGroupSize` | `1000000` | Parquet row-group size for flush, snapshot, and legacy COPY |

Retention, rollup steps, and background tickers are documented when their
sub-issues merge.

---

## Storage engine (`internal/store/engine`)

Per-tenant embedded DuckDB at `<DATA_DIR>/<tenant>/engine.duckdb`. The engine owns the hot ingest path, hot→L0 flush, near-real-time hot snapshots, a bounded tenant LRU, and a one-time legacy `metrics-raw` importer. Compaction, rollups, and tickers land in #25.

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

`ExportHotSnapshots` atomically writes `<tenant>/hot/current.parquet` from `hot_current ORDER BY ts` (temp + rename). Reads see in-flight rows; a ~15s export ticker is wired in #25.

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
