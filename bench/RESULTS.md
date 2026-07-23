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
| Git commit | `944a09b` |
| Measured | 2026-07-23T09:39:57Z |

## Correctness gates

Metrics `COUNT(*)` over the full ingested table: store **1,000,000**, ClickHouse **1,000,000** (must match; equals dataset metrics row count).

Logs `LIKE '%deadline exceeded%'` count: store **10,000**, ClickHouse **10,000** (must match).

## Results (p50 / p95 / min ms; ingest: wall + rows/s)

| Workload | prism-store | ClickHouse |
|----------|-------------|------------|
| ingest | 1.18s · 1692060 rows/s | 1.67s · 1195310 rows/s |
| count | 0.9 / 7.1 / 0.6 | 1.9 / 2.1 / 1.6 |
| aggregation | 8.2 / 14.6 / 5.2 | 6.2 / 25.9 / 5.8 |
| logs LIKE | 18.7 / 29.2 / 17.6 | 16.1 / 23.3 / 15.0 |

## Resource usage (sampled during timed window)

| Workload | System | CPU mean / peak (cores) | Peak RSS (MiB) | Read+write (MiB) | MiB/s | IOPS |
|----------|--------|-------------------------|----------------|------------------|-------|------|
| ingest | prism-store | 0.50 / 2.12 | 90.7 | n/a | n/a | n/a |
| ingest | ClickHouse | 0.25 / 0.45 | 311.1 | 0.0 | 0.0 | 0 |
| count | prism-store | 0.07 / 0.14 | 644.8 | n/a | n/a | n/a |
| count | ClickHouse | 0.05 / 0.05 | 372.0 | 0.0 | 0.0 | 0 |
| aggregation | prism-store | 0.86 / 1.72 | 659.6 | n/a | n/a | n/a |
| aggregation | ClickHouse | 0.04 / 0.04 | 330.5 | 0.0 | 0.0 | 0 |
| logs LIKE | prism-store | 1.95 / 3.91 | 682.4 | n/a | n/a | n/a |
| logs LIKE | ClickHouse | 0.05 / 0.05 | 362.0 | 0.0 | 0.0 | 0 |

Store **count**, **aggregation**, and **logs LIKE** sample the benchmark process (embedded DuckDB engine). Store **ingest** samples the `prism-store` binary. ClickHouse samples the container cgroup.

## Interpretation

Both systems ingested the same seeded dataset. Each query workload is warmed once before K=5 timed runs.

**Metrics count and aggregation** scan the **full ingested metrics table** on both sides — no `ts` range predicate — so both systems read the same N rows (`SELECT count(*)` and `GROUP BY __name__` over every row). Rollup tiers are excluded on the store path to avoid double-counting.

**Logs LIKE** uses the same dataset-`ts` window on both systems (`ds.LogsQueryRange()`). The store path is **engine-level**: DuckDB `read_parquet` over a logs-shaped zstd Parquet tier in the store on-disk layout — the shipping store has no logs ingest API yet.

The store metrics path uses real HTTP Parquet ingest, hot→L0 flush, tier compaction, and a fixed-schema union over hot + tier Parquet (no rollups for these workloads).

ClickHouse uses MergeTree with `LowCardinality` dimensions, day partitioning, batched inserts (50k rows), and a `tokenbf_v1` skip index on `message`.

- **ingest**: prism-store leads on ingest throughput (1692060 vs 1195310 rows/s).
- **count**: prism-store p50 0.9 ms vs ClickHouse 1.9 ms.
- **aggregation**: ClickHouse p50 6.2 ms beats prism-store 8.2 ms on this host.
- **logs_like**: ClickHouse p50 16.1 ms beats prism-store 18.7 ms on this host.
## Reproduce

```bash
make bench        # default scale (2M rows total)
make bench BENCH_SCALE=2
```

See [`bench/README.md`](README.md) for prerequisites and cleanup.
