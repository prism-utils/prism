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
| Git commit | `fa7cb26` |
| Measured | 2026-07-23T10:25:56Z |

## Correctness gates

Metrics `COUNT(*)` over the full ingested table: store **1,000,000**, ClickHouse **1,000,000** (must match; equals dataset metrics row count).

Logs `LIKE '%deadline exceeded%'` count: store **10,000**, ClickHouse **10,000** (must match).

## Results (p50 / p95 / min ms; ingest: wall + rows/s)

| Workload | prism-store | ClickHouse |
|----------|-------------|------------|
| ingest | 1.49s · 1339588 rows/s | 2.03s · 984933 rows/s |
| count | 0.5 / 0.7 / 0.4 | 1.6 / 1.9 / 1.3 |
| aggregation | 12.9 / 14.9 / 12.0 | 10.3 / 31.8 / 8.7 |
| logs LIKE | 68.8 / 69.4 / 68.3 | 39.0 / 44.0 / 36.3 |

## Resource usage (dense series; per-phase aggregates)

| Workload | System | CPU mean / peak (cores) | Peak RSS (MiB) | Read+write (MiB) | MiB/s | IOPS |
|----------|--------|-------------------------|----------------|------------------|-------|------|
| idle (baseline) | prism-store | 0.00 / 0.00 | 22.4 | n/a | n/a | n/a |
| idle (baseline) | ClickHouse | 0.05 / 0.09 | 241.0 | 0.2 | 0.0 | 0 |
| ingest | prism-store | 0.10 / 2.00 | 92.1 | n/a | n/a | n/a |
| ingest | ClickHouse | 0.15 / 0.45 | 371.2 | 27.3 | 3.9 | 0 |
| count | prism-store | 0.10 / 0.59 | 583.2 | n/a | n/a | n/a |
| count | ClickHouse | 0.06 / 0.11 | 349.9 | n/a | n/a | n/a |
| aggregation | prism-store | 0.15 / 1.95 | 586.7 | n/a | n/a | n/a |
| aggregation | ClickHouse | 0.08 / 0.16 | 327.8 | n/a | n/a | n/a |
| logs LIKE | prism-store | 0.52 / 2.07 | 598.5 | n/a | n/a | n/a |
| logs LIKE | ClickHouse | 0.28 / 0.46 | 358.6 | 0.4 | 0.2 | 0 |

Store **count**, **aggregation**, and **logs LIKE** sample the benchmark process (embedded DuckDB engine). Store **ingest** and **idle** sample the `prism-store` binary. ClickHouse samples the container cgroup via cumulative counter diffs (~75 ms).

## Resource charts

### cpu-cores

![cpu-cores.svg](bench/charts/cpu-cores.svg)

### memory-rss

![memory-rss.svg](bench/charts/memory-rss.svg)

### disk-io

![disk-io.svg](bench/charts/disk-io.svg)


## Interpretation

Both systems ingested the same seeded dataset. Each query workload is warmed once before K=5 timed runs.

**Metrics count and aggregation** scan the **full ingested metrics table** on both sides — no `ts` range predicate — so both systems read the same N rows (`SELECT count(*)` and `GROUP BY __name__` over every row). Rollup tiers are excluded on the store path to avoid double-counting.

**Logs LIKE** uses the same dataset-`ts` window on both systems (`ds.LogsQueryRange()`). The store path is **engine-level**: DuckDB `read_parquet` over a logs-shaped zstd Parquet tier in the store on-disk layout — the shipping store has no logs ingest API yet.

The store metrics path uses real HTTP Parquet ingest, hot→L0 flush, tier compaction, and a fixed-schema union over hot + tier Parquet (no rollups for these workloads).

ClickHouse uses MergeTree with `LowCardinality` dimensions, day partitioning, batched inserts (50k rows), and a `tokenbf_v1` skip index on `message`.

- **ingest**: prism-store leads on ingest throughput (1339588 vs 984933 rows/s).
- **count**: prism-store p50 0.5 ms vs ClickHouse 1.6 ms.
- **aggregation**: ClickHouse p50 10.3 ms beats prism-store 12.9 ms on this host.
- **logs_like**: ClickHouse p50 39.0 ms beats prism-store 68.8 ms on this host.
## Reproduce

```bash
make bench        # default scale (2M rows total)
make bench BENCH_SCALE=2
```

See [`bench/README.md`](README.md) for prerequisites and cleanup.
