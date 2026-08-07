# Spec: SQL queue on by default + live queue snapshot for operators

Status: CHANGES_REQUESTED

- **Slug / branch:** `feat/sql-queue-default-on`
- **Ships as:** next patch after `v1.9.5` (expected `v1.9.6`) once merged + tagged.
- **Owner phase:** orchestrator → developer → reviewer → merge → tag/release → hand off to homelab for bump + `/admin` UI.

## 1. Task

The `/sql` (+ PromQL + Loki) in-flight limiter already exists (`internal/store/queue`, since v1.3) but defaults **off**. Shared multi-tenant writers (homelab `prism-proxy`) already opt in via env; a fresh or misconfigured deploy can still run unbounded concurrent DuckDB sandboxes and OOM under Grafana dashboard fan-out.

This task:

1. **Flip defaults** so the queue protects production-shaped workloads out of the box.
2. **Expose a live queue snapshot** (in-flight, waiting, caps, rejected total) on a new admin-plane route — **do not** mutate the frozen `GET /stats` billing contract.
3. Update docs (`CONFIG.md`, `STORE.md`, `MEMORY.md`, `MIGRATION.md`) to match.

Homelab `/admin` UI + image bump live in the sibling `feat/prism-queue-admin` work (blocked on this release).

## 2. Scope

- **In scope:**
  - Default env changes in `cmd/prism-store` + tests + helm chart values/golden.
  - `queue.Limiter` snapshot + rejected counter; `GET /admin/queue` (admin-plane, same `protectAdminRoute` / RBAC `admin`/`stats` posture as `/stats`).
  - Docs + MIGRATION note for the default flip.
- **Out of scope:**
  - Changing the frozen `/stats` JSON billing shape.
  - Per-tenant queues / fair scheduling.
  - Homelab site-main UI / gitops tag bump (sibling task).
  - Adding a full Prometheus `/metrics` scrape surface (snapshot JSON is enough for `/admin`; counters live in the snapshot).

## 3. Open questions — resolved in Phase 0

- [x] Q: Is default `SQL_API_MAX_INFLIGHT=10` safe? — **A: No.** Rejected.
  - Prod chart comment (2026-08-06): admin Grafana Prom+Loki **OOMKilled at 6Gi with inflight=16**.
  - Prod soak today: `sqlAPIMaxInFlight: 2`, `sqlAPIMaxQueue: 128`, `sqlAPIQueueTimeoutMs: 120000`, DuckDB `6500MB`, pod limit `10Gi`.
  - Prometheus 7d `max_over_time(container_memory_working_set_bytes{namespace="prism-proxy",container="proxy"})` peaked **~7.5 GiB** of 10 GiB under load; multiple historical restart spikes during higher-concurrency experiments.
  - Admin dashboards fan out hard: `clickhouse-overview` ≈174 targets, `cluster-overview` ≈116, `node-stats` ≈79 — concurrency must stay low; depth + long wait absorb the blast.
  - MEMORY model: each sandbox independently honors `DUCKDB_MEMORY_LIMIT` → `inflight × 6500MB` must fit under the cgroup with merge/Go headroom. **Default `MaxInFlight=2`.**
- [x] Q: Keep 5s queue timeout when enabling by default? — **A: No.** Raise default timeout to **120000 ms** and default max queue to **128**, matching soaked prod so Grafana panels wait instead of 429 under dashboard fan-out.
- [x] Q: Extend `/stats`? — **A: No.** Frozen billing contract (MIGRATION.md / STORE.md). New `GET /admin/queue`.

## 4. Decision log

- **Default queue ON with MaxInFlight=2, MaxQueue=128, Timeout=120s:**
  - ref: https://github.com/grafana/loki/blob/main/pkg/querier/querier.go — Loki `querier.max-concurrent` defaults to 4 and is sized ≈ CPU; our bottleneck is **per-sandbox DuckDB memory**, not CPU, so live OOM evidence wins over Loki's CPU default (and over the user's guess of 10).
  - ref: https://blog.stackademic.com/backpressure-strategies-for-high-throughput-go-services-a2335b0590e3 — semaphore + bounded wait + shed metrics is the standard Go backpressure pattern.
  - perf: caps peak sandbox term at `2 × DUCKDB_MEMORY_LIMIT`; long wait avoids 429 storms on 100+ panel dashboards.
  - product: shared writers are safe without operator env; single-tenant labs can raise `SQL_API_MAX_INFLIGHT` explicitly.

- **Live snapshot via `GET /admin/queue`, not `/stats` / not full Prometheus exporter:**
  - ref: https://www.dash0.com/knowledge/prometheus-metrics — gauges for in-flight + queue depth; prefer reading source-of-truth (`len(sem)`, `waiters`) via `Set`-style snapshot.
  - ref: https://pkg.go.dev/github.com/pior/loadshedder — running/waiting/limit/rejected is the minimal useful surface.
  - perf: atomic reads only; no scrape registry / cardinality.
  - product: site-main `/admin` can `fetch` the same admin plane as `/stats` without inventing Prom scrape wiring for one card.

## 5. Acceptance checklist  (developer checks these off)

### Defaults
- [x] `SQL_API_QUEUE_ENABLED` default **`true`**
- [x] `SQL_API_MAX_INFLIGHT` default **`2`**
- [x] `SQL_API_MAX_QUEUE` default **`128`**
- [x] `SQL_API_QUEUE_TIMEOUT_MS` default **`120000`**
- [x] Config unit tests updated for new defaults; env override still works
- [x] Helm chart `values.yaml` + golden regenerated to match

### Snapshot instrumentation
- [x] `Limiter` tracks `rejectedTotal` (atomic) on every 429 path
- [x] `Limiter.Snapshot()` returns `{Enabled, MaxInFlight, MaxQueue, TimeoutMs, InFlight, Waiting, RejectedTotal}` (InFlight = `len(sem)` / occupied slots)
- [x] `GET /admin/queue` registered on admin/combined mux only; protected like `/stats` (ADMIN_TOKEN / RBAC admin action)
- [x] Coordinator (`MODE=cluster`) does **not** host the limiter snapshot of a data node (no false zeros on the proxy) — either omit the route on coordinator or return `enabled:false` with zeros; document the choice
  - Chose **omit**: the coordinator mux registers no admin routes, so `GET /admin/queue` is `404` there; documented in `STORE.md` (`/admin/queue` section + cluster out-of-scope list), `MIGRATION.md`, and `DESIGN.md`, and pinned by `TestRouterOmitsQueueSnapshotRoute`.
- [ ] Unit tests: snapshot reflects in-flight + waiters under load; rejected increments on 429; route auth walls
  - Only the `ADMIN_TOKEN` wall is tested; with `AUTHZ_POLICY_FILE` set the route's RBAC wall has no test — add a `TestRBACStatsScoping`-shaped case asserting `403` for a principal with no `stats` binding and `200` for one that has it.

### Docs
- [x] `docs/CONFIG.md`, `docs/STORE.md`, `docs/MEMORY.md`, `docs/MIGRATION.md` updated (default-on + new route + sizing note that 10/16 OOMs shared writers)
  - Also `README.md` (said "default off") and a `DESIGN.md` decision entry, which the flip would otherwise contradict.
- [x] Tests written first (`test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if wiring touched)
  - `make lint` 0 issues; `make test` all packages ok (race, `-tags duckdb_arrow`); `make full-tests` → `full-tests: OK` (integration + e2e via docker); `deploy/charts/prism-store/scripts/check-golden.sh` → golden manifest OK.

## 6. Mandatory review gates  (reviewer owns)

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
  - The new `/admin/queue` route has no RBAC-on test, so its `403` failure path is unverified while `STORE.md` now states a specific RBAC contract for it.
- [x] **Gate 3 — Docs & comments match the task and the delivered code**
- [x] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes
  - Blocked by Gate 2 only: "Failure paths + `Validate()` rejection covered, not just happy path" is unmet for the new route's RBAC wall.

## 7. Reviewer notes

**Verdict: `CHANGES_REQUESTED`** — one gate (Gate 2) fails. Everything else holds.

Verified green by the reviewer, uncached:

- `make lint` → `0 issues`.
- `go test -count=1 -race -tags duckdb_arrow ./...` → all packages `ok`.
- `make integration e2e GOFLAGS=-count=1` → `integration 3.1s ok`, `e2e 148.0s ok`.
- `deploy/charts/prism-store/scripts/check-golden.sh` → `golden manifest OK`.
- `go mod tidy` → no diff.

Confirmed independently: defaults are `true / 2 / 128 / 120000` in `cmd/prism-store`,
chart values, and the golden manifest; `rejectedTotal` increments on all three shed
paths (full queue, expired wait, client cancel), each with its own test; the route is
registered under `serveAdmin` only and returns `404` on the public plane and on a
`MODE=cluster` coordinator; `internal/store/admin/stats.go` is untouched so the frozen
billing JSON is unchanged; `internal/store/cluster` stays a leaf in the production
dependency graph (the `admin` import is external-test-only); and history shows
`test:` (`03089a1`) before both implementation commits.

### The one blocking item

`GET /admin/queue` is wired with `rbac.wrapStats`, and `STORE.md` gained a
dedicated "`/admin/queue` scoping" section asserting that a principal with any
`stats` binding sees the snapshot. No test exercises that path — only the
`ADMIN_TOKEN` wall is covered. `TestRBACStatsScoping` in `cmd/prism-store` is the
established harness for exactly this at the same wiring layer.

This is worth a test rather than trusting construction, because the shared
`WrapStats` middleware branches on the `ns` query parameter before authorizing.
`/admin/queue` has no tenant dimension, but `GET /admin/queue?ns=<tenant outside
the principal's stats scope>` will take that branch and answer `403`/`404` while
the bare path answers `200`. That is safe (strictly more restrictive), but it is
undocumented and unpinned, and it makes the STORE.md sentence imprecise.

### Non-blocking observations for the orchestrator

- `internal/store/queue/snapshot_test.go` synchronizes with bounded `time.Sleep(5ms)`
  poll loops guarded by a one-second deadline. `TESTING.md` prefers
  `require.Eventually`, but the existing `limiter_test.go` in the same package already
  uses this shape, so this is consistent with the package rather than new drift.
  Worth converting the package as a whole some day, not in this PR.
- The docs are unusually good here: the MEMORY.md sizing rule, the MIGRATION.md
  upgrade callout, and the DESIGN.md decision entry all carry the OOM evidence, and
  the coordinator-`404` choice is justified in prose and pinned by a test.
- Once the RBAC test lands, nothing else needs re-verification beyond `make lint test`;
  the wiring, docs, chart, and golden manifest are all settled.
