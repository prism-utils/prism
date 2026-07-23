# prism-store memory model

Standalone reference for sizing and limiting memory on `prism-store` data nodes
(standalone and `client` modes). Configuration knobs live in
[`CONFIG.md`](CONFIG.md) §14; operational features in [`STORE.md`](STORE.md).

---

## No global DuckDB pool

`prism-store` does **not** share one DuckDB memory pool across tenants or
requests. Each tenant gets a **separate on-disk DuckDB** at
`<DATA_DIR>/<tenant>/engine.duckdb`, opened on demand and cached in an LRU
bounded by `MAX_OPEN_TENANTS` (default `32`). Each engine connection uses
`SetMaxOpenConns(1)` — one DuckDB instance per open tenant handle.

Every `POST /{ns}/sql` request opens an additional **ephemeral in-memory**
DuckDB sandbox (`:memory:`) that is discarded when the request completes.

Each DuckDB instance (tenant engine, SQL sandbox, merge worker, rollup worker)
honors `DUCKDB_MEMORY_LIMIT` **independently** when set.

---

## Worst-case peak model

Treat soft caps as ceilings, not reservations — idle open tenants hold only
their buffered working set; hot data is disk-backed in `engine.duckdb`.

```
peak ≈ (active tenant engines × DUCKDB_MEMORY_LIMIT)
     + (concurrent /sql sandboxes × DUCKDB_MEMORY_LIMIT)
     + background merge/rollup DuckDB
     + Go heap
```

Multiply by **concurrently active** operations, not by `MAX_OPEN_TENANTS`.
An LRU-evicted tenant closes its engine and releases DuckDB memory.

With `RUN_JOBS=false`, the merge/rollup term drops to zero (see below).

---

## `DUCKDB_MEMORY_LIMIT` — meaning and scaling

When set, DuckDB applies `SET memory_limit='…'` on every governed instance
(tenant engine, `/sql` sandbox, merge, rollup). When **unset**, DuckDB defaults
to roughly **80% of host RAM** per instance — always set this in containers.

| Query shape | Memory growth |
|---|---|
| Scans, filters, `COUNT`, `LIKE` | ~constant (streamed row-groups) |
| `GROUP BY`, `DISTINCT`, `JOIN` | ~cardinality (hash tables) |
| `ORDER BY`, large result sets | ~rows (can spill on engine; sandbox cannot) |

Peak working set often scales with **`DUCKDB_THREADS`** (parallel operators).

The limit is a **soft cap**: supported operators may spill to a temp directory.
True overflow surfaces as a **catchable query error**, not a process crash.

---

## Sandbox vs tenant engine spill behavior

| Instance | Temp directory | On limit exceeded |
|---|---|---|
| `/sql` sandbox | `max_temp_directory_size='0B'` | **Fail-fast** — query errors immediately; bounded, predictable |
| Tenant engine (`/query`) | Default temp dir | **Can spill** to disk under the tenant root |

The sandbox sets `max_temp_directory_size='0B'` so ad-hoc SQL cannot grow
unbounded via spill. Structured query on the tenant engine keeps spill as a
safety valve.

---

## Hot-buffer growth (time-bounded only)

The hot ingest path (`hot_current` / `hot_prev`) has **no row or byte cap** —
only a **time window** (`HOT_WINDOW_MINUTES` or `HOT_WINDOW_SECONDS`), drained
by `FLUSH_TICK_SECONDS` and opportunistically on ingest when the deadline
passes.

High ingest within one window grows `hot_current` until flush. During L0 COPY,
transient peak includes both `hot_current` and `hot_prev`.

Sizing formula (from [`STORE.md`](STORE.md)):

```
mem ≈ 128 MiB + (ingest_rate × HOT_WINDOW_seconds × ~0.4 KiB/row) × ~1.5
```

Example: 5,000 samples/s with a 5-minute window ≈ **~1 GiB** hot-path memory.

---

## `RUN_JOBS=false` reader/writer split

When `RUN_JOBS=false`, background tickers (hot snapshot, flush, merge, rollups,
retention) are **not started**. This removes the merge/rollup DuckDB memory
term and makes reader nodes more predictable.

Ingest and query still run. Ingest can still trigger opportunistic hot flush.
Operators should treat such nodes as **read-only** — do not rely on them to
compact or enforce retention.

Reader pods over shared storage (`DATA_DIR` on a PVC or object-store FUSE) scale
horizontally for query load while a single writer pod runs `RUN_JOBS=true`.

---

## Arrow transport vs DuckDB memory

Arrow IPC is a **response encoding** of the same `POST /{ns}/sql` endpoint
(chosen via `Accept: application/vnd.apache.arrow.stream`). It is **not** a
separate route and did not replace `/sql`.

Arrow reduces **Go heap** and **wire size** (streaming batches vs a buffered
JSON array). It does **not** reduce the sandbox DuckDB working set.

JSON and Arrow requests count **equally** toward `/sql` concurrency when the
queue is enabled.

---

## `/sql` in-flight queue (v1.3+)

Without the queue, concurrent `/sql` requests are unbounded — worst-case memory
scales as `concurrent_requests × DUCKDB_MEMORY_LIMIT`.

Optional env (all default to prior behavior — queue **off**):

| Env | Default | Role |
|---|---|---|
| `SQL_API_QUEUE_ENABLED` | `false` | Master switch |
| `SQL_API_MAX_INFLIGHT` | `4` | Max concurrent `/sql` executions |
| `SQL_API_MAX_QUEUE` | `64` | Max requests waiting for a slot |
| `SQL_API_QUEUE_TIMEOUT_MS` | `5000` | Max wait before `429` |

When enabled on a **data node** (standalone or client — **not** the cluster
coordinator), middleware order is: auth → `OwnedTenantGuard` → limiter → handler.
Cheap rejections do not consume a slot.

Backpressure: `429 Too Many Requests`, body `too many concurrent queries`,
header `Retry-After: 1`.

Sizing rule: **`SQL_API_MAX_INFLIGHT × DUCKDB_THREADS ≈ CPU cores`** for the
pod.

---

## Setting limits — recipe

1. Choose container hard memory limit **`M`**.
2. Reserve headroom **`H`** (~25–30%) for Go GC, OS page cache, and spikes.
3. Set **`GOMEMLIMIT`** and **`GOMAXPROCS`** via pod env — honored by the Go
   runtime automatically; the binary does not read them.
4. Compute per-instance DuckDB cap:

   ```
   DUCKDB_MEMORY_LIMIT ≈ (M − H − GoHeap) / (MAX_INFLIGHT + active_ingest_engines)
   ```

5. Keep **`M` > Σ soft caps + headroom** so DuckDB spills (engine) or returns
   query errors (sandbox) before the kernel OOM-kills the pod.

See [`CONFIG.md`](CONFIG.md) §14 for every env var.

---

## Heterogeneous tenants

There is **no per-tenant memory limit map** in a single process today — one
global `DUCKDB_MEMORY_LIMIT` and `DUCKDB_THREADS` apply to all tenants on that
node.

**Recommended isolation:** cluster `MODE` with per-tenant or per-tenant-class
**client pods**, each with its own `DUCKDB_MEMORY_LIMIT`, `GOMEMLIMIT`, and
Kubernetes requests/limits. The pod limit **is** the per-tenant limit in
practice. Combine with RBAC bindings and Vault-issued JWTs (see
[`STORE.md`](STORE.md#rbac)).

`MAX_OPEN_TENANTS` bounds how many tenant **engines** stay resident simultaneously;
it does not cap concurrent `/sql` sandboxes — use the queue for that.

---

## Related docs

- [`CONFIG.md`](CONFIG.md) §14 — complete env reference
- [`STORE.md`](STORE.md) — features, SQL API, sizing table
- [`DESIGN.md`](DESIGN.md) §15 — store ADRs
