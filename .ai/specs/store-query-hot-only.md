# Spec: prism-store — "hot only" query mode flag

Status: ALL_OK

- **Slug / branch:** `feat/store-query-hot-only`
- **Owner phase:** orchestrator → developer
- **Feature 1 of 3** in the prism-store roles epic (independent; ship as its own PR).

## 1. Task

Add a bootstrap flag/config **`QUERY_HOT_ONLY`** (default **false**). When enabled,
the HTTP query API answers **only from the hot cache** (the in-memory DuckDB
`hot_current` + `hot_prev` tables) and **never reads Parquet** (tier segments or
rollups). Default behavior (unified hot + Parquet view) is unchanged.

## 2. Scope

- **In scope:**
  - `internal/store/query`: add a `HotOnly bool` to the query `Config` and the `Builder`. In `buildSQL`, when `HotOnly` is true, emit ONLY the `hot_current` (+ `hot_prev` when present) SELECTs and **skip the tier `read_parquet` globs and the rollup branch entirely**. Args accounting stays correct (two args per emitted part). The `ORDER BY ts` shape and `AggregateSQL` compatibility are preserved.
  - `cmd/prism-store/main.go`: read `QUERY_HOT_ONLY` (bool env, default false) into `serverConfig`, pass into `query.Config`; log the effective value at startup. Follow the existing env-parsing style (add an `envBool` helper if none exists).
  - Threading: the `query.Handler`/`Config` already construct the Builder — wire `HotOnly` through so the served handler honors it.
- **Out of scope:**
  - The Grafana `ViewSQL` path (that is inherently a Parquet-snapshot view; hot-only does not apply and it stays unchanged). Note this in docs.
  - The `run-jobs` flag and `mode` feature (separate specs/PRs).
  - Any change to ingest, flush, or the engine.

## 3. Open questions  (resolved before READY)

- [x] Does "hot only" include `hot_prev`? → **Yes.** Both `hot_current` and `hot_prev` are the in-memory hot cache (not Parquet); including `hot_prev` avoids a data gap during a flush. Only Parquet (tiers + rollups) is excluded.
- [x] Env name + default? → **`QUERY_HOT_ONLY`**, default **false** (opt-in), matching the existing `UPPER_SNAKE` env convention.
- [x] Apply to the Grafana view SQL too? → **No** — that path is Parquet-snapshot by design; out of scope, documented.

## 4. Decision log  (Decision Protocol)

- **Add a hot-only read mode as an opt-in query flag (default off).**
  - ref: compute/compute separation — a low-latency serving pool that reads only the freshest data, separate from heavy/cold scans (ObsessionDB on decoupled ClickHouse: https://obsessiondb.com/blog/building-on-decoupled-clickhouse ).
  - perf: skipping tier/rollup `read_parquet` removes file globbing + Parquet scan from the hot path → lower, more predictable latency for freshness-only queries.
  - product: enables a "hot serving" role (pairs with the later `run-jobs`/`mode` work) without changing default semantics; strictly opt-in so existing deployments are unaffected.

## 5. Acceptance checklist  (developer checks these off)

- [x] `Builder`/`Config` gain `HotOnly`; `buildSQL` emits only hot-table SELECTs (no `read_parquet`, no rollup branch) when set; unit test asserts the generated SQL contains `hot_current`/`hot_prev` and contains **no** `read_parquet` and no `rollups` path when `HotOnly=true`, and still includes tiers/rollups when false.
- [x] Integration test (CGO/DuckDB): a tenant with BOTH hot rows and sealed tier Parquet returns ONLY the hot rows under hot-only, and the full union when disabled; args count matches emitted parts (no `sql: expected N args` errors).
- [x] `cmd/prism-store` reads `QUERY_HOT_ONLY` (default false), passes it to `query.Config`, logs the effective value; `serve` unchanged when unset.
- [x] `AggregateSQL` still works on hot-only output (shape preserved); a test covers aggregate over hot-only SQL.
- [x] Docs updated: `docs/STORE.md` (and `--help`/usage comment in `main.go`) document `QUERY_HOT_ONLY`, its default, that it excludes tiers+rollups, and that the Grafana view path is unaffected.
- [x] `make lint test` (`-race`) green; `go build ./cmd/prism-store` ok; `CGO_ENABLED=0 go build ./cmd/prism` ok; `make tidy` clean; no committed blobs.

## 6. Mandatory review gates  (reviewer owns)

- [x] **Gate 1 — Guidelines:** minimal change, env parsed in the existing style, wrapped errors, no globals, comments self-contained; default-off verified (no behavior change when unset).
- [x] **Gate 2 — Edge cases:** hot-only when `hot_prev` absent (single part, one arg pair); hot-only when tiers exist but must be ignored; hot-only with a rollup-eligible wide range still ignores rollups; args/placeholders balanced; empty result returns `[]` not error.
- [x] **Gate 3 — Docs/comments match code:** the documented default + exclusions match the builder; Grafana-view caveat accurate.
- [x] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [x] Full `docs/REVIEW.md` checklist; TDD verified via `git log` (test-first).

## 7. Reviewer notes

**APPROVE** (2026-07-23). TDD order confirmed (`7517d7a` test → `7e41cf8` feat). `make lint`/`make test -race`/builds/`make tidy`/clean tree all green. Default-off verified: `HotOnly` zero-value false; tier+rollup loop gated on `!b.HotOnly`; `TestBuildSQLHotOnlyFalseIncludesTiersAndRollups` + existing union tests unchanged. Hot-only SQL/args/integration/aggregate covered; `hot_prev`-absent single-part path verified ad-hoc (2 args, `[]` on empty). Docs (`STORE.md`, `main.go` usage) match wiring; `view.go` untouched.
