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
| Git commit | `67be130` |
| Measured | 2026-07-23T09:32:05Z |

## Correctness gates

Metrics `COUNT(*)` over the full ingested table: store **1,000,000**, ClickHouse **1,000,000** (must match; equals dataset metrics row count).

Logs `LIKE '%deadline exceeded%'` count: store **10,000**, ClickHouse **10,000** (must match).

## Results (p50 / p95 / min ms; ingest: wall + rows/s)

| Workload | prism-store | ClickHouse |
|----------|-------------|------------|
| ingest | 1.14s · 1759572 rows/s | 1.62s · 1235686 rows/s |
| count | 0.9 / 5.3 / 0.5 | 1.4 / 1.7 / 1.3 |
| aggregation | 6.1 / 12.4 / 4.0 | 6.1 / 23.6 / 5.3 |
| logs LIKE | 17.2 / 29.5 / 16.8 | 15.3 / 23.3 / 14.3 |

## Resource usage (sampled during timed window)

| Workload | System | CPU mean / peak (cores) | Peak RSS (MiB) | Read+write (MiB) | MiB/s | IOPS |
|----------|--------|-------------------------|----------------|------------------|-------|------|
| ingest | prism-store | 0.48 / 2.27 | 96.5 | n/a | n/a | n/a |
| ingest | ClickHouse | 0.26 / 0.48 | 316.4 | 0.0 | 0.0 | 0 |
| count | prism-store | 0.09 / 0.18 | 650.4 | n/a | n/a | n/a |
| count | ClickHouse | 0.05 / 0.05 | 362.7 | 0.0 | 0.0 | 0 |
| aggregation | prism-store | 0.94 / 1.89 | 669.9 | n/a | n/a | n/a |
| aggregation | ClickHouse | 0.04 / 0.04 | 350.6 | 0.0 | 0.0 | 0 |
| logs LIKE | prism-store | 1.92 / 3.85 | 695.8 | n/a | n/a | n/a |
| logs LIKE | ClickHouse | 0.06 / 0.06 | 394.1 | 0.0 | 0.0 | 0 |

Store **count**, **aggregation**, and **logs LIKE** sample the benchmark process (embedded DuckDB engine). Store **ingest** samples the `prism-store` binary. ClickHouse samples the container cgroup.

## Interpretation

Both systems ingested the same seeded dataset. Each query workload is warmed once before K=5 timed runs.

**Metrics count and aggregation** scan the **full ingested metrics table** on both sides — no `ts` range predicate — so both systems read the same N rows (`SELECT count(*)` and `GROUP BY __name__` over every row). Rollup tiers are excluded on the store path to avoid double-counting.

**Logs LIKE** uses the same dataset-`ts` window on both systems (`ds.LogsQueryRange()`). The store path is **engine-level**: DuckDB `read_parquet` over a logs-shaped zstd Parquet tier in the store on-disk layout — the shipping store has no logs ingest API yet.

The store metrics path uses real HTTP Parquet ingest, hot→L0 flush, tier compaction, and a fixed-schema union over hot + tier Parquet (no rollups for these workloads).

ClickHouse uses MergeTree with `LowCardinality` dimensions, day partitioning, batched inserts (50k rows), and a `tokenbf_v1` skip index on `message`.

- **ingest**: prism-store leads on ingest throughput (1759572 vs 1235686 rows/s).
- **count**: prism-store p50 0.9 ms vs ClickHouse 1.4 ms.
- **aggregation**: ClickHouse p50 6.1 ms beats prism-store 6.1 ms on this host.
- **logs_like**: ClickHouse p50 15.3 ms beats prism-store 17.2 ms on this host.
## Reproduce

```bash
make bench        # default scale (2M rows total)
make bench BENCH_SCALE=2
```

See [`bench/README.md`](README.md) for prerequisites and cleanup.
