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

- `bench/results.json` — machine-readable report
- `bench/RESULTS.md` — rendered table + interpretation

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

## Resource usage measurement

While each workload runs, the harness samples **actual** CPU, memory, and disk I/O
(not the host’s allocated hardware):

| Workload | prism-store target | ClickHouse target |
|----------|-------------------|-------------------|
| ingest | `prism-store` binary process (HTTP ingest) | ClickHouse **container** (Docker cgroup) |
| count / aggregation / logs LIKE | **benchmark process** (embedded DuckDB engine) | ClickHouse **container** |

- **Interval**: ~75 ms polling (`ProcSampler` via gopsutil; `DockerSampler` via Docker Engine API `GET /containers/{id}/stats?stream=false` over the Docker socket — stdlib HTTP, no Docker SDK).
- **CPU**: cores = CPU-time delta / wall-clock delta; report mean and peak over the window.
- **Memory**: peak RSS (process tree or container `memory_stats.usage`).
- **Disk**: read+write MiB and MiB/s always when counters exist; **IOPS** (read+write ops/s) when the platform exposes op counts.
- **Why not node_exporter / host metrics?** Host-level sampling conflates both systems, the OS, and other processes. Per-process and per-container attribution is exact and needs no extra services.
- **Caveats**:
  - Store queries run in an **embedded DuckDB engine inside `prism-bench`** — there is no separate query server; resource usage for count/aggregation/logs LIKE reflects that embedding (the store’s real architecture).
  - Per-process disk **IOPS** (and process-level I/O bytes) come from Linux `/proc/<pid>/io` only. On **macOS/Windows** the process sampler reports CPU/mem and marks process I/O/IOPS **`n/a`**. ClickHouse container I/O/IOPS work on all Docker platforms via cgroup blkio stats.
  - If the Docker socket is unreachable, the container sampler falls back to `docker stats --no-stream` for CPU/mem and marks IOPS **`n/a`**.

Results appear in `RESULTS.md` (full table), `results.json`, and the root `README.md` benchmark section.

## Layout

```
bench/
  cmd/prism-bench/       orchestrator CLI
  internal/gen/          deterministic dataset + Parquet writers
  internal/clickhouse/   DDL, batched insert, queries
  internal/store/        prism-store HTTP ingest + engine queries (CGO)
  internal/results/      JSON + markdown renderer
  internal/timing/       warm-and-repeat latency helpers
  internal/monitor/      per-process and per-container resource samplers
  docker-compose.bench.yml
```

The harness is opt-in: `make test` does **not** start ClickHouse.
