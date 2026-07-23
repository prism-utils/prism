# Arrow transport profile (RBAC on) (hot cache only — sandbox reads the hot snapshot; parquet tiers skipped)

Measured on this host with `make bench-api-arrow-hot` — store queries over the RBAC-guarded HTTP SQL API with **Arrow IPC** transport for count/aggregation and a JSON-vs-Arrow scan comparison.

*prism-store count/aggregation use Arrow transport (`Accept: application/vnd.apache.arrow.stream`); scan phases compare JSON vs Arrow on the same SQL. ClickHouse uses its native protocol client; logs LIKE remains engine-level (no logs API). JWT/RBAC overhead applies to every store HTTP request.*

## Environment

| Key | Value |
|-----|-------|
| OS / arch | darwin / arm64 |
| CPU | Apple M1 Pro |
| RAM | 16.0 GiB |
| ClickHouse | 24.8.14.39 |
| DuckDB | v1.4.1 |
| Dataset | 1000000 metrics + 1000000 logs rows (scale=1) |
| Resource cap (per system) | 2 vCPU / 1024 MiB RAM |
| DuckDB threads / memory_limit | 2 / 1024MB |
| Idle baseline window | 5.0 s before workloads |
| Git commit | `93612b8` |
| Measured | 2026-07-23T18:19:44Z |

## Correctness gates

Metrics `COUNT(*)` over the full ingested table: store **1,000,000**, ClickHouse **1,000,000** (must match; equals dataset metrics row count).

Logs `LIKE '%deadline exceeded%'` count: store **10,000**, ClickHouse **10,000** (must match).

## Results (p50 / p95 / min ms; ingest: wall + rows/s)

| Workload | prism-store (Arrow) | ClickHouse |
|----------|---------------------|------------|
| ingest | 1.20s · 1669133 rows/s | 1.88s · 1066478 rows/s |
| count | 130.9 / 131.8 / 130.3 | 1.4 / 2.0 / 1.2 |
| aggregation | 143.5 / 144.2 / 140.5 | 10.0 / 22.3 / 8.6 |
| logs LIKE | 44.3 / 46.2 / 43.5 | 35.5 / 50.7 / 34.0 |

**Scan transport comparison** (same SQL, store only — not vs ClickHouse):

| Transport | p50 / p95 / min (ms) | rows returned |
|-----------|----------------------|---------------|
| JSON | 366.1 / 370.3 / 357.9 | 100,000 |
| Arrow | 172.0 / 176.6 / 170.7 | 100,000 |

## Resource usage (dense series; per-phase aggregates)

| Workload | System | CPU mean / peak (cores) | Peak RSS (MiB) | Read+write (MiB) | MiB/s | IOPS |
|----------|--------|-------------------------|----------------|------------------|-------|------|
| idle (baseline) | prism-store | 0.00 / 0.00 | 22.0 | n/a | n/a | n/a |
| idle (baseline) | ClickHouse | 0.04 / 0.15 | 270.0 | n/a | n/a | n/a |
| ingest | prism-store | 0.16 / 1.76 | 92.7 | n/a | n/a | n/a |
| ingest | ClickHouse | 0.34 / 1.47 | 417.2 | 0.6 | 0.2 | 0 |
| count | prism-store | 1.19 / 2.09 | 419.7 | n/a | n/a | n/a |
| count | ClickHouse | 0.09 / 0.76 | 422.3 | n/a | n/a | n/a |
| aggregation | prism-store | 0.96 / 1.85 | 447.8 | n/a | n/a | n/a |
| aggregation | ClickHouse | 0.15 / 1.39 | 422.1 | n/a | n/a | n/a |
| logs LIKE | prism-store | 1.07 / 3.72 | 1361.0 | n/a | n/a | n/a |
| logs LIKE | ClickHouse | 0.39 / 1.83 | 454.8 | n/a | n/a | n/a |
| scan (JSON transport) | prism-store | 0.81 / 2.11 | 568.8 | n/a | n/a | n/a |
| scan (Arrow transport) | prism-store | 1.08 / 2.04 | 574.4 | n/a | n/a | n/a |

**API-Arrow profile:** store **count**, **aggregation**, **scan_json**, and **scan_arrow** sample the `prism-store` binary (HTTP `/sql` with JWT/RBAC). Store **logs LIKE** samples the benchmark process (embedded engine; server stopped). Store **ingest** and **idle** sample the `prism-store` binary. ClickHouse samples the container cgroup via cumulative counter diffs (~75 ms). **Scan phases** isolate JSON-vs-Arrow transport memory/latency on the same build — not a ClickHouse comparison.

**Caveats:** JWT verification and RBAC policy checks run on every store HTTP request. Container I/O/IOPS on Docker Desktop (macOS/Windows) frequently report blkio as 0 — meaningful container I/O needs a native Linux Docker host.

## Resource charts

### cpu-cores

![cpu-cores.svg](charts-api-arrow-hot/cpu-cores.svg)

### memory-rss

![memory-rss.svg](charts-api-arrow-hot/memory-rss.svg)

### disk-io

![disk-io.svg](charts-api-arrow-hot/disk-io.svg)


## Interpretation

Both systems ingested the same seeded dataset with JWT auth on store ingest. Each query workload is warmed once before K=5 timed runs.

**Count and aggregation** use the **Arrow IPC** transport over HTTP `/sql` (lazy-view sandbox + streaming). Compare against the attached JSON `-api` profile (~280–300 ms, ~471–483 MiB peak RSS on the same host class) to see the combined lazy-view (#54) + Arrow transport (#55) impact.

**Scan JSON vs Arrow** runs the same `SELECT … LIMIT N` over both transports on one build — isolating transport memory/latency (JSON full-buffer vs Arrow streaming). This is **not** compared to ClickHouse.

- **ingest**: prism-store leads on ingest throughput (1669133 vs 1066478 rows/s).
- **count** (Arrow): ClickHouse p50 1.4 ms beats prism-store 130.9 ms.
- **aggregation** (Arrow): ClickHouse p50 10.0 ms beats prism-store 143.5 ms.
- **logs_like** (Arrow): ClickHouse p50 35.5 ms beats prism-store 44.3 ms.
- **scan**: JSON p50 366.1 ms (peak RSS 568.8 MiB) vs Arrow p50 172.0 ms (peak RSS 574.4 MiB).
## Reproduce

```bash
make bench-api-arrow-hot        # default scale (2M rows total)
make bench-api-arrow-hot BENCH_SCALE=2
```

See [`bench/README.md`](README.md) for prerequisites and cleanup.
