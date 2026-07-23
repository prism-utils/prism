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
| Git commit | `e6f007a` |
| Measured | 2026-07-23T03:45:17Z |

## Correctness gate

Logs `LIKE '%deadline exceeded%'` count: store **10,000**, ClickHouse **10,000** (must match).

## Results (p50 / p95 / min ms; ingest: wall + rows/s)

| Workload | prism-store | ClickHouse |
|----------|-------------|------------|
| ingest | 1.17s · 1714212 rows/s | 1.82s · 1100331 rows/s |
| count | 0.8 / 0.9 / 0.8 | 4.3 / 16.5 / 3.8 |
| aggregation | 6.4 / 6.6 / 5.3 | 11.8 / 26.1 / 11.1 |
| logs LIKE | 19.6 / 19.9 / 19.3 | 15.9 / 16.3 / 15.5 |

## Interpretation

Both systems ingested the same seeded dataset over an identical time range with warmed caches (one throwaway query before timing).

The store metrics path uses real HTTP Parquet ingest, hot→L0 flush, tier compaction, and the fixed-schema union query engine. The logs LIKE path is **engine-level**: DuckDB `read_parquet` over a zstd logs-shaped tier in the store on-disk layout — the shipping store has no logs ingest API yet.

ClickHouse uses MergeTree with `LowCardinality` dimensions, day partitioning, batched inserts (50k rows), and a `tokenbf_v1` skip index on `message`.

- **ingest**: prism-store leads on ingest throughput (1714212 vs 1100331 rows/s).
- **count**: prism-store p50 0.8 ms vs ClickHouse 4.3 ms.
  Store metrics queries filter on ingest-time `ts`; ClickHouse uses embedded sample timestamps.
- **aggregation**: prism-store p50 6.4 ms vs ClickHouse 11.8 ms.
- **logs_like**: ClickHouse p50 15.9 ms beats prism-store 19.6 ms on this host.
## Reproduce

```bash
make bench        # default scale (2M rows total)
make bench BENCH_SCALE=2
```

See [`bench/README.md`](README.md) for prerequisites and cleanup.
