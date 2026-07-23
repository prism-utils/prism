# Spec: prism-store — tenant provisioning (/ensure) + /stats metering

Status: CHANGES_REQUESTED

- **Slug / branch:** `feat/store-provisioning`
- **Owner phase:** orchestrator → developer
- **Issue:** elk-utilities/prism#27 (Epic #21) — depends on #24, #25, #26 (merged).

## 1. Task

Port the tenant lifecycle admin API and the per-tenant metering endpoint from
`homelab-apps` prism-proxy: `POST /admin/tenants/{ns}/ensure` (idempotent seeds
so empty-tenant Grafana `read_parquet` globs never error) and `GET /stats?ns=`
(the **billing contract** consumers scrape). Add a separate control-plane bind
so admin/stats/query are not publicly reachable.

## 2. Scope

- **In scope:**
  - **`internal/store/seed`** (port of reference `internal/seed`): embed a zero-row `metrics-raw` parquet; `EnsureMetricsRawSeedForTenant(dataDir, ns)` writes `<ns>/metrics-raw/_seed.parquet`; `EnsureTieredLayoutForTenant(dataDir, ns)` writes zero-row seeds `tiers/_seed.parquet`, `hot/current.parquet`, `rollups/{1m,5m,1h}/_seed.parquet`. Reserved `SeedName = "_seed.parquet"`. All writes idempotent (skip if present) and atomic (`.tmp`+rename). Copy the embedded fixture `metrics-raw-seed.parquet` from the reference package.
  - **`internal/store/admin`** — HTTP handlers + the stats response types/collectors, keeping `main.go` thin (mirrors `internal/store/{ingest,query}`):
    - `EnsureHandler`: `POST /admin/tenants/{ns}/ensure` → `TenantAllowed(ns)` false ⇒ `404 unknown tenant`; open tenant DuckDB (`eng.DB(ns)`); `EnsureMetricsRawSeedForTenant` + `EnsureTieredLayoutForTenant`; any failure ⇒ `500 ensure failed`; success ⇒ `204`. Idempotent (repeat calls stay `204`, no dup rows).
    - `StatsHandler`: `GET /stats?ns=` → non-empty `ns` not allowed ⇒ `404`. Response type **byte-for-byte compatible** with the reference:
      ```
      { "artifacts": { "<artifact>": { "windows": int, "latestUnixNanos": int } },
        "totalWindows": int, "onDiskBytes"?: int, "compactionCpuSeconds"?: float }
      ```
      `onDiskBytes`/`compactionCpuSeconds` use `omitempty` and are set **only when `ns` is provided**. Per-tenant: `windows` = `eng.HotRowCount(ns)` + `len(engine.ListL0(dataDir,ns))`; `latestUnixNanos` = max L0 file mtime. Aggregate (no `ns`): sum `windows` and max `latestUnixNanos` across tenant dirs; `totalWindows` = sum. Iterate `cfg.allowedArtifacts` for the `artifacts` map (today a single `metrics-raw` entry). `onDiskBytes` from `stats.TenantOnDiskBytes` (excludes legacy `metrics-raw/`), `compactionCpuSeconds` from `stats.CompactionCPUSeconds`.
  - **Separate control-plane bind in `cmd/prism-store`:**
    - `ADMIN_LISTEN_ADDR` (optional): when set, `/admin/*`, `/stats`, and the `/{ns}/query` route bind on a **second `http.Server`** on that address; the public `LISTEN_ADDR` server keeps only ingest + `/healthz` + `/readyz`. When **unset**, all routes stay on the single public mux (dev/back-compat).
    - `ADMIN_TOKEN` (optional): when set, admin+stats(+query on the admin plane) require `Authorization: Bearer <token>` (constant-time compare); missing/wrong ⇒ `401`. When unset, no auth (rely on network isolation — documented).
    - `/healthz` + `/readyz` served on **both** planes so each is independently probeable.
    - Graceful shutdown must stop **both** servers.
  - Reuse `internal/store/{engine,stats,tenant,query}`. Update `docs/STORE.md` (document the `/stats` JSON as a **stable billing contract**, `/admin/ensure`, seeds) and `docs/CONFIG.md` (`ADMIN_LISTEN_ADDR`, `ADMIN_TOKEN`).
- **Out of scope:** Helm (#28); release/image (#29); benchmark; the consumer control-plane that *calls* `/ensure`.

## 3. Open questions  (resolved before READY)

- [x] Single port with path allowlist, or a separate bind? → **Separate optional bind** (`ADMIN_LISTEN_ADDR`): project-agnostic — consumers lock it down with NetworkPolicy/gateway rather than depending on a mesh. Unset = single mux for dev.
- [x] Does the query route move to the control plane? → Yes when `ADMIN_LISTEN_ADDR` is set (query is read-plane per the issue); stays on the single mux otherwise.
- [x] Change the `/stats` JSON at all? → **No** — byte-for-byte compatible (billing continuity, EPIC_I); only additive `omitempty` fields already present.
- [x] Auth default? → Optional `ADMIN_TOKEN`; default no-auth relying on network isolation, documented as the deployment expectation.

## 4. Decision log  (Decision Protocol)

- **Separate control-plane listener over in-mux path allowlist.**
  - ref: Kubernetes NetworkPolicy binds policy to ports; https://kubernetes.io/docs/concepts/services-networking/network-policies/
  - perf: negligible (a second `http.Server`); product: consumers restrict the admin port at L3/L4 without app-level mesh assumptions — the core generalization lever for #27.
- **Idempotent zero-row parquet seeds on ensure.**
  - ref: DuckDB errors on an empty `read_parquet` glob — https://duckdb.org/docs/data/parquet/overview
  - perf: one-time tiny writes; product: freshly-provisioned tenants render in Grafana before the first window lands.
- **Frozen `/stats` contract.**
  - ref: consumer `credit-metering.ts` reads `onDiskBytes` + `compactionCpuSeconds`.
  - perf: n/a; product: billing continuity across the cutover — a schema change would silently break metering.

## 5. Acceptance checklist  (developer checks these off)

- [x] `seed.EnsureMetricsRawSeedForTenant` + `EnsureTieredLayoutForTenant` write all documented zero-row files, idempotently and atomically; embedded fixture present; `SeedName` reserved.
- [ ] `POST /admin/tenants/{ns}/ensure`: `404` unknown tenant, `500` on failure, `204` success; repeat call stays `204` and adds no rows (idempotent) — tested.
- [x] `GET /stats?ns=<t>`: returns the exact documented shape; `windows` = hot rows + L0 count; `latestUnixNanos` = max L0 mtime; `onDiskBytes` excludes legacy `metrics-raw/`; `compactionCpuSeconds` from metering file.
- [x] `GET /stats` (no `ns`): aggregates `windows`/`totalWindows` across tenant dirs; omits `onDiskBytes`/`compactionCpuSeconds`.
- [x] JSON field names/casing/omitempty match the reference byte-for-byte (golden-style assertion).
- [ ] `ADMIN_LISTEN_ADDR` set ⇒ admin/stats/query on the second server, ingest stays on public; unset ⇒ single mux. Both planes serve `/healthz`+`/readyz`. Shutdown stops both.
- [x] `ADMIN_TOKEN` set ⇒ `401` without/with wrong bearer, pass with correct (constant-time); unset ⇒ open.
- [x] `docs/STORE.md` (billing contract + ensure/seeds) and `docs/CONFIG.md` (`ADMIN_LISTEN_ADDR`, `ADMIN_TOKEN`) updated.
- [x] Tests written first (`test:` commit precedes implementation) — CONTRIBUTING.md §1.
- [x] `make lint test` green; `CGO_ENABLED=0 go build ./cmd/prism` passes; `go build ./cmd/prism-store` passes.

## 6. Mandatory review gates  (reviewer owns)

- [x] **Gate 1 — Guidelines:** seed pkg leaf/pure; handlers thin, slog at edges, errors wrapped; bearer compare constant-time; no globals; two-server wiring clean with a single shutdown path.
- [ ] **Gate 2 — Edge cases:** ensure on already-seeded tenant (idempotent, no dup rows); ensure on unknown tenant `404`; stats for tenant with no data (zero windows, not error); stats aggregate over empty dataDir; legacy `metrics-raw/` excluded from `onDiskBytes`; missing `.metering.json` ⇒ `0`; admin token unset vs set; port-collision/startup error surfaces.
  - Missing: `POST /ensure` → `500 ensure failed` when seed/layout write fails (no httptest); admin-plane `/{ns}/query` not asserted in split-plane tests; no test that `runServe` shuts down both HTTP servers; no port-collision/startup-error test.
- [x] **Gate 3 — Docs/comments match code:** STORE.md `/stats` contract matches the struct tags exactly; CONFIG.md flags match; no forward references.
- [ ] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
  - `internal/store/admin/stats.go:34` nolint cites `engine.ListL0` — drop the cross-symbol reference from the comment text.
- [ ] Full docs/REVIEW.md checklist; TESTING.md layering (seed unit + DuckDB integration for stats/ensure; `httptest` for handlers incl. two-plane routing + auth).
  - Split-plane httptest covers ingest/`/stats`/health only; query route and dual-server shutdown untested.

## 7. Reviewer notes

**Verdict: CHANGES_REQUESTED** (2026-07-22). Independent verification green: `make lint` 0 issues; `make test` (`-race`) all packages ok; `CGO_ENABLED=0 go build ./cmd/prism` ok; `go build ./cmd/prism-store` ok. TDD order ok (`1359153 test(store-provisioning):…` before `0f11e82 feat(store-provisioning):…`). Billing JSON struct tags and golden strings match reference `artifactStats`/`statsResponse` in prism-proxy `main.go` (~273–316); `ingest.BearerEquals` uses `crypto/subtle.ConstantTimeCompare`. No committed binaries (`.parquet` fixture only).

**Fix before re-review:**

1. **§5 ensure `500`:** add an httptest (e.g. read-only `DATA_DIR` or injected seed failure) asserting `500` + body `ensure failed`.
2. **§5 split plane:** extend `cmd/prism-store/routing_test.go` — admin mux serves `GET /{ns}/query` (non-404), public mux does not when split; add a test that cancelling `runServe` ctx calls `Shutdown` on both servers (or equivalent httptest of the shutdown path).
3. **Gate 2 port-collision:** assert startup returns/surfaces an error when `ADMIN_LISTEN_ADDR` (or public addr) is already bound.
4. **Gate 4:** reword the `//nolint:gosec` on `stats.go:34` so it does not name `engine.ListL0`.
