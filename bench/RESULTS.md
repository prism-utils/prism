# Benchmark: prism-store vs ClickHouse

Measured on this host with `make bench` (default small profile).

## Environment

| Key | Value |
|-----|-------|
| OS / arch | darwin / arm64 |
| CPU | Apple M1 Pro |
| RAM | 16.0 GiB |
| ClickHouse | 24.8.14.39 |
| DuckDB | v1.1.3 |
| Dataset | 1000000 metrics + 1000000 logs rows (scale=1) |
| Resource cap (per system) | 2 vCPU / 1024 MiB RAM |
| DuckDB threads / memory_limit | 2 / 1024MB |
| Idle baseline window | 5.0 s before workloads |
| Git commit | `dac1523` |
| Measured | 2026-07-23T10:32:05Z |

## Correctness gates

Metrics `COUNT(*)` over the full ingested table: store **1,000,000**, ClickHouse **1,000,000** (must match; equals dataset metrics row count).

Logs `LIKE '%deadline exceeded%'` count: store **10,000**, ClickHouse **10,000** (must match).

## Results (p50 / p95 / min ms; ingest: wall + rows/s)

| Workload | prism-store | ClickHouse |
|----------|-------------|------------|
| ingest | 1.48s · 1349593 rows/s | 2.06s · 971603 rows/s |
| count | 0.7 / 1.4 / 0.5 | 1.6 / 2.1 / 1.4 |
| aggregation | 13.2 / 16.6 / 12.9 | 10.4 / 32.2 / 10.3 |
| logs LIKE | 72.8 / 254.5 / 70.5 | 46.8 / 55.6 / 42.9 |

## Resource usage (dense series; per-phase aggregates)

| Workload | System | CPU mean / peak (cores) | Peak RSS (MiB) | Read+write (MiB) | MiB/s | IOPS |
|----------|--------|-------------------------|----------------|------------------|-------|------|
| idle (baseline) | prism-store | 0.00 / 0.00 | 22.3 | n/a | n/a | n/a |
| idle (baseline) | ClickHouse | 0.08 / 1.46 | 248.6 | 0.2 | 0.0 | 0 |
| ingest | prism-store | 0.04 / 1.82 | 99.0 | n/a | n/a | n/a |
| ingest | ClickHouse | 0.10 / 1.00 | 398.2 | 0.6 | 0.0 | 0 |
| count | prism-store | 0.37 / 0.89 | 581.6 | n/a | n/a | n/a |
| count | ClickHouse | 0.11 / 0.47 | 398.5 | n/a | n/a | n/a |
| aggregation | prism-store | 0.80 / 1.94 | 585.4 | n/a | n/a | n/a |
| aggregation | ClickHouse | 0.50 / 1.47 | 402.0 | n/a | n/a | n/a |
| logs LIKE | prism-store | 1.12 / 3.25 | 604.2 | n/a | n/a | n/a |
| logs LIKE | ClickHouse | 0.59 / 1.99 | 401.5 | 0.3 | 0.4 | 0 |

Store **count**, **aggregation**, and **logs LIKE** sample the benchmark process (embedded DuckDB engine). Store **ingest** and **idle** sample the `prism-store` binary. ClickHouse samples the container cgroup via cumulative counter diffs (~75 ms).

## Resource charts

### cpu-cores

![cpu-cores.svg](charts/cpu-cores.svg)

### memory-rss

![memory-rss.svg](charts/memory-rss.svg)

### disk-io

![disk-io.svg](charts/disk-io.svg)


## Interpretation

Both systems ingested the same seeded dataset. Each query workload is warmed once before K=5 timed runs.

**Metrics count and aggregation** scan the **full ingested metrics table** on both sides — no `ts` range predicate — so both systems read the same N rows (`SELECT count(*)` and `GROUP BY __name__` over every row). Rollup tiers are excluded on the store path to avoid double-counting.

**Logs LIKE** uses the same dataset-`ts` window on both systems (`ds.LogsQueryRange()`). The store path is **engine-level**: DuckDB `read_parquet` over a logs-shaped zstd Parquet tier in the store on-disk layout — the shipping store has no logs ingest API yet.

The store metrics path uses real HTTP Parquet ingest, hot→L0 flush, tier compaction, and a fixed-schema union over hot + tier Parquet (no rollups for these workloads).

ClickHouse uses MergeTree with `LowCardinality` dimensions, day partitioning, batched inserts (50k rows), and a `tokenbf_v1` skip index on `message`.

- **ingest**: prism-store leads on ingest throughput (1349593 vs 971603 rows/s).
- **count**: prism-store p50 0.7 ms vs ClickHouse 1.6 ms.
- **aggregation**: ClickHouse p50 10.4 ms beats prism-store 13.2 ms on this host.
- **logs_like**: ClickHouse p50 46.8 ms beats prism-store 72.8 ms on this host.
## Reproduce

```bash
make bench        # default scale (2M rows total)
make bench BENCH_SCALE=2
```

See [`bench/README.md`](README.md) for prerequisites and cleanup.
