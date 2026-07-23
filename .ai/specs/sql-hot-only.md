# Spec: honor QUERY_HOT_ONLY in the /sql sandbox + api-arrow-hot benchmark

Status: READY

- **Slug / branch:** `feat/sql-hot-only`
- **Owner phase:** orchestrator → developer → reviewer + security-review; then orchestrator RUNS the benchmark and commits results to a directory.
- **Security-sensitive** (touches the arbitrary-SQL sandbox parquet-source selection). The change only **narrows** the source set (drops tier globs) — it cannot broaden access. Isolation/RBAC unchanged.
- **Why:** `QUERY_HOT_ONLY` today only affects the legacy `GET /{ns}/query` range endpoint. The Arrow/JSON **transport** lives on `POST /{ns}/sql`, whose sandbox always unions the hot snapshot + parquet tiers — so there is no "transport over hot-cache-only" path to benchmark. Wire hot-only into the `/sql` sandbox, then benchmark the Arrow transport in hot-only mode and place the results in `bench/`.

## 1. Task
1. Make the `POST /{ns}/sql` sandbox honor `QUERY_HOT_ONLY`: when hot-only, the `metrics` view reads **only** the hot snapshot (`hot/current.parquet`) and **skips** the tier globs (`tiers/L*/*.parquet`). Wire the existing `QUERY_HOT_ONLY` config into `SQLConfig`.
2. Add a benchmark hot-only variant (`--hot-only`, with `--api`; combined with `--arrow` → profile `api-arrow-hot`) that sets `QUERY_HOT_ONLY=true`, drives the Arrow-transport count/aggregation + JSON-vs-Arrow scan, and writes results to `bench/RESULTS-api-arrow-hot.md` + `bench/charts-api-arrow-hot/` (+ `bench/results-api-arrow-hot.*`). Attach; do not touch other profiles.

## 2. Design (resolved)

### Sandbox hot-only (`internal/store/query/sql.go`)
- Add `HotOnly bool` to `SQLConfig`.
- Thread it down: `SQLHandler` → `prepareSandboxConn(..., hotOnly)` → `sandboxMetricsUnionSQL(tenantRoot, hotOnly)` → `collectSafeParquetPaths(absRoot, tenantRoot, hotOnly)`.
- In `collectSafeParquetPaths`: always include the vetted hot snapshot (`hot/current.parquet`); **when `hotOnly==true`, skip the tier glob loop entirely.** All existing symlink/containment vetting (`safeTenantParquetFile`, `AssertNoUnionByName`) is unchanged.
- If the hot snapshot is absent under hot-only → `errNoParquetSources` → 400 (same as today). (In practice `ExportHotSnapshot` runs first, so it exists.)
- Config wiring (`cmd/prism-store/main.go`): pass `HotOnly: cfg.queryHotOnly` into the `query.SQLConfig{...}` literal (~line 323), mirroring the existing `HotOnly: cfg.queryHotOnly` on the `/query` `query.Config`.
- Both JSON and Arrow encoders inherit this automatically (they share `prepareSandboxConn`).

### Benchmark (`bench/`)
- Driver: add `HotOnly bool` to `benchstore.Config`; when set, `serverEnv()` adds `QUERY_HOT_ONLY=true`.
- Orchestrator: add `--hot-only` flag (requires `--api`; error otherwise). Profile suffix gains `-hot`: `--api --arrow --hot-only` → `api-arrow-hot`. Reuse the existing `api-arrow` query path (count/agg Arrow + scan_json/scan_arrow) and count gate (all bench data is resident in `hot_current`, so the hot snapshot holds the full ~1M rows → the count gate still equals `MetricsRows`).
- Artifacts via `results.ArtifactPaths(repoRoot, "api-arrow-hot")` → `bench/results-api-arrow-hot.*`, `bench/RESULTS-api-arrow-hot.md`, `bench/charts-api-arrow-hot/`.
- Render: treat `api-arrow-hot` like `api-arrow` with a title/notes suffix "(hot cache only — sandbox reads the hot snapshot, tiers skipped)".
- `Makefile`: `bench-api-arrow-hot` target = `CGO_ENABLED=1 go run -tags duckdb_arrow ./bench/cmd/prism-bench --api --arrow --hot-only --scale $(BENCH_SCALE)`.
- Docs: `bench/README.md` short section; `docs/STORE.md` note that `QUERY_HOT_ONLY` now also constrains the SQL API sandbox to the hot snapshot.

### Numbers produced by the run
- Developer lands code + tests only (no committed `*-api-arrow-hot*` artifacts). Orchestrator runs `make bench-api-arrow-hot` and commits the generated results/charts + a root README note.

### Out of scope
- Changing other profiles/results. RBAC/isolation logic. `/query` endpoint. Flushing bench data to tiers to force hot-misses (documented as a possible future contrast; here all data is hot so hot-only == full-count, which is the desired comparable baseline).

## 3. Open questions (resolved)
- [x] Does the transport honor hot-only today → **no** (`/sql` ignores it); this spec wires it in.
- [x] Will hot-only return the full dataset in the bench → **yes** (all ~1M rows live in `hot_current`; the hot snapshot contains them), so the count gate holds and results are comparable to `api-arrow`.
- [x] Attach location → `bench/` with an `api-arrow-hot` suffix (dedicated charts dir).

## 4. Decision log (Decision Protocol)
- **Extend `QUERY_HOT_ONLY` to the `/sql` sandbox by dropping tier globs; benchmark the Arrow transport in that mode.**
  - ref: existing hot-only design in `internal/store/query/query.go` (tier/rollup loop gated on `!HotOnly`) and `docs/STORE.md` hot-window semantics.
  - perf/product: hot-only serves purely from the recent hot snapshot (no tier parquet fan-out), the intended low-latency path; benchmarking the transport there shows its best-case latency with RBAC on. Security: strictly narrows the sandbox source set, so isolation is unaffected.

## 5. Acceptance checklist (developer)
- [ ] `SQLConfig.HotOnly` added; wired from `QUERY_HOT_ONLY` in `main.go`; threaded to `collectSafeParquetPaths`.
- [ ] Hot-only sandbox SQL includes ONLY `hot/current.parquet`; NO `tiers/L*` paths; non-hot-only unchanged (snapshot + tiers).
- [ ] Tests: (a) hot-only union SQL omits tier paths / includes snapshot; (b) with data in BOTH hot snapshot and a tier, hot-only COUNT returns only hot rows while full returns all (proves tiers skipped); (c) isolation tests still pass under hot-only (cross-tenant/host-fs → 400); (d) Arrow + JSON both honor hot-only (shared path). TDD tests-first.
- [ ] Bench: `--hot-only` requires `--api` (error path tested); `api-arrow-hot` profile + artifacts; `QUERY_HOT_ONLY=true` in `serverEnv`; count gate passes; render supports `api-arrow-hot`; `make bench-api-arrow-hot`; `bench/README.md` + `docs/STORE.md` notes.
- [ ] `make lint test` green (with `duckdb_arrow`); `go build -tags duckdb_arrow ./bench/...` ok; `CGO_ENABLED=0 go build ./cmd/prism` ok; `make tidy` clean; `git status` clean.
- [ ] No `*-api-arrow-hot*` result artifacts committed (orchestrator generates them).

## 6. Mandatory review gates (reviewer) — SECURITY-SENSITIVE
- [ ] Gate 1 — Guidelines: minimal threading of HotOnly; shared sandbox path for JSON+Arrow; wrapped errors; atomic comments §3.8.
- [ ] Gate 2 — Edge cases: hot-only with missing snapshot → 400; hot-only skips ALL tiers; JSON and Arrow both affected; bench `--hot-only` requires `--api`; count gate holds.
- [ ] Gate 3 — Docs match code (STORE.md + bench/README.md).
- [ ] Gate 4 — Atomic comments.
- [ ] **SECURITY AUDIT:** confirm the change only narrows the source set (drops tier globs) — hot snapshot still vetted by `safeTenantParquetFile`; `allowed_directories` + external-access-off + lock unchanged; no cross-tenant/host-fs escape; RBAC unchanged.
- [ ] Full `docs/REVIEW.md`; TESTING layering; TDD (`git log`).

## 7. Reviewer notes
_(empty until first review)_
