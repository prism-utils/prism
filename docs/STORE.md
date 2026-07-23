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

`prism-store` receives HTTP-parquet windows from edge agents, lands them under
per-tenant partitions, maintains a DuckDB hot catalog plus tiered Parquet
segments, materializes rollups, and exposes read-only query endpoints. The
skeleton slice (`#22`) exposes health/readiness only; ingest, engine, and query
wire in #23–#28.

---

## On-disk layout (per tenant)

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

---

## Configuration (environment)

> **Stub.** Expanded per sub-issue as ingest, engine, lifecycle, and query land.

| Variable | Default | Purpose |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | HTTP bind address |
| `DATA_DIR` | `/data` | Shared data root for all tenants |

Additional variables (`INGEST_TOKEN`, hot-window tuning, retention, rollup steps,
etc.) are documented when their sub-issues merge.

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
