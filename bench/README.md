# prism-store vs ClickHouse benchmark

Reproducible harness comparing **prism-store** (embedded DuckDB over tiered
zstd Parquet) against **ClickHouse** on four workloads:

1. **ingest** — load metrics + logs into each system (wall-clock + rows/s)
2. **count** — `COUNT(*)` over the **full ingested metrics table** (no time predicate)
3. **aggregation** — per-series `avg` / `min` / `max` / `count` grouped by `__name__` over **all metrics rows**
4. **logs LIKE** — `COUNT(*) WHERE message LIKE '%deadline exceeded%'` over a shared dataset-`ts` window (correctness gate + timing)

## Prerequisites

- Go 1.25+ with CGO toolchain (DuckDB)
- Docker + Docker Compose
- ~4 GiB free RAM for the default profile (~2M rows)

## One-command reproduction

From the repository root:

```bash
make bench
```

Larger local run:

```bash
make bench BENCH_SCALE=2   # doubles row counts (~4M rows total)
```

Outputs:

- `bench/results.json` — machine-readable report (includes resource caps)
- `bench/results-timeseries.json` — downsampled dense sample series
- `bench/RESULTS.md` — rendered tables, charts, and interpretation
- `bench/charts/*.svg` — CPU, memory, and I/O timeline charts

Optional flags on `prism-bench`: `--cpus`, `--mem-mib` (default **2 vCPU / 1024 MiB**
per system), `--idle-seconds` (default **5** s quiet baseline before workloads).

Cleanup is automatic (`docker compose down -v` on exit). Ephemeral data lives under `bench/.work/` (gitignored).

Manual cleanup if a run was killed abruptly:

```bash
docker compose -f bench/docker-compose.bench.yml down -v
rm -rf bench/.work
```

## Fairness notes

- **Identical data**: seeded generator (`bench/internal/gen`) feeds both backends from the same rows.
- **Metrics count/aggregation**: both systems scan the **full ingested metrics table** — no `ts` range pruning — so count and aggregation compare the same N rows. The store unions hot + tier Parquet and excludes rollup tiers to avoid double-counting.
- **Logs LIKE**: both systems filter on the **same dataset-`ts` window** (`ds.LogsQueryRange()`).
- **ClickHouse tuning**: MergeTree, `ORDER BY (ts, …)` low→high cardinality, `LowCardinality(String)` for `__name__` / `level` / `service`, day partitioning, batched inserts (50k rows), `tokenbf_v1` skip index on `message`. Image pinned in `docker-compose.bench.yml`. ClickHouse wins logs LIKE on typical hosts with this tuning.
- **Store metrics path**: real HTTP Parquet ingest via `prism-store`, then flush + tier compaction, then a fixed-schema union query over hot + tiers.
- **Store logs path (engine-level)**: DuckDB `read_parquet` over a logs-shaped zstd Parquet tier written in the store on-disk layout. The shipping store has **no logs ingest API** today — this measures the engine approach, not a product claim.
- **Timing**: each query is warmed once, then run K=5 times; results report p50 / p95 / min (ms). Ingest reports wall-clock + rows/s.
- **Correctness gates**: benchmark **fails** if metrics full-table counts differ between systems or from the expected row count, or if logs LIKE counts differ.
- **Equal resource envelope**: both systems run under the same minimal cap (default **2 vCPU / 1 GiB**). The store binary gets `GOMAXPROCS`, `GOMEMLIMIT`, and DuckDB `threads` / `memory_limit` (`DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`). ClickHouse gets compose `cpus` / `mem_limit` plus mounted `config.d` / `users.d` snippets (`max_threads`, `max_server_memory_usage`, `max_memory_usage`). Caps are recorded in `results.json`.

## Resource usage measurement

The harness runs **continuous** samplers for the whole benchmark and records an
**idle baseline** before any workload:

| Phase | prism-store target | ClickHouse target |
|-------|-------------------|-------------------|
| idle (baseline) | `prism-store` binary (quiet window) | ClickHouse **container** |
| ingest | `prism-store` binary (HTTP ingest) | ClickHouse **container** |
| count / aggregation / logs LIKE | **benchmark process** (embedded DuckDB engine) | ClickHouse **container** |

- **Idle baseline**: after both systems are up, a **5 s** quiet window (override with `--idle-seconds`) samples both targets before ingest. Reported as the **idle (baseline)** row and shaded on charts.
- **Sampling resolution**: process targets ~**35 ms** (`gopsutil`); ClickHouse container ~**75 ms** via Docker Engine API `GET /containers/{id}/stats?stream=false` (stdlib HTTP over the Docker socket — no Docker SDK).
- **Container CPU**: derived by **diffing cumulative `cpu_stats.cpu_usage.total_usage`** (nanoseconds) between polls — not Docker’s pre-computed 1 s streamed percentage — so sub-second charts have real points.
- **Container I/O**: blkio cumulative counters are differenced the same way.
- **CPU**: cores = CPU-time delta / wall-clock delta; per-phase tables report mean and peak from the dense series.
- **Memory**: peak RSS (process tree or container `memory_stats.usage`).
- **Disk**: read+write MiB and MiB/s when counters exist; **IOPS** when op counts exist.
- **Why not node_exporter / host metrics?** Host-level sampling conflates both systems, the OS, and other processes. Per-process and per-container attribution is exact and needs no extra services.
- **Charts**: pure-Go SVG timelines under `bench/charts/` (via `gonum.org/v1/plot`); embedded in `RESULTS.md` and the root `README.md` benchmark section.
- **Caveats**:
  - Store queries run in an **embedded DuckDB engine inside `prism-bench`** — there is no separate query server; resource usage for count/aggregation/logs LIKE reflects that embedding (the store’s real architecture).
  - Per-process disk **IOPS** (and process-level I/O bytes) come from Linux `/proc/<pid>/io` only. On **macOS/Windows** the process sampler reports CPU/mem and marks process I/O/IOPS **`n/a`**. ClickHouse container I/O/IOPS use Docker cgroup blkio stats; **Docker Desktop (macOS/Windows) frequently reports blkio as 0**, so meaningful container I/O/IOPS needs a **native Linux Docker host**.
  - If the Docker socket is unreachable, the container sampler falls back to `docker stats --no-stream` for CPU/mem and marks IOPS **`n/a`**.

Results appear in `RESULTS.md`, `results.json`, `results-timeseries.json`, `bench/charts/`, and the root `README.md` benchmark section.

## Layout

```
bench/
  cmd/prism-bench/       orchestrator CLI
  internal/caps/         equal per-system resource budget helpers
  internal/gen/          deterministic dataset + Parquet writers
  internal/clickhouse/   DDL, batched insert, queries, bench config writer
  internal/store/        prism-store HTTP ingest + engine queries (CGO)
  internal/results/      JSON, timeseries, markdown, SVG charts
  internal/timing/       warm-and-repeat latency helpers
  internal/monitor/      continuous stream samplers + phase aggregation
  charts/                committed SVG artifacts from the last `make bench`
  docker-compose.bench.yml
```

The harness is opt-in: `make test` does **not** start ClickHouse.
