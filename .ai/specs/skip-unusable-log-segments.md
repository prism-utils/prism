# Spec: skip unusable log segments so refresh and Loki survive crash leftovers

Status: IN_REVIEW

- **Slug / branch:** `cursor/skip-unusable-log-segments-b37a`
- **Owner phase:** reviewer
- **PLAN phase(s):** store logs lifecycle / query visibility

## 1. Task

Prod admin canary (`user-fknjdouh-apps`) filed `grafana-issue` canary-empty +
query-broken after 0-byte crash leftovers (landing `.duckdb` and L1 `.parquet`)
wedged log refresh: oldest-first attach failed every tick, landing grew to
~50k files, Loki last-1h opened no L0. Skip unusable segments on scan and
query, pack a newest landing refresh first so last-1h becomes searchable
without draining the whole backlog, and size log packs off a 1 MiB floor so
tiny landings fill the seal budget.

## 2. Scope

- **In scope:**
  - `segformat.TooSmall` — bytes below a parquet/duckdb header are unusable
  - Query catalog skips unusable paths (`filterExistingLogFiles`, dir walk)
  - Merge landing/tier scan skips unusable files (do not fail the tenant)
  - Promote listing skips unusable files so a 0-byte L1 does not fail the tick
  - Log refresh: when the live set exceeds one pack and the action budget is
    ≥2, plan **one newest-first** landing→L0 action first, then oldest-first
    drain with the remaining budget
  - Log `packLiveLogs`: when the planner floor is larger than 1 MiB, derive
    merge-at-once from a 1 MiB floor (capped) so 536 KiB landings pack toward
    the seal budget
  - Tests first; `make lint test`
  - STORE.md / CONFIG.md one-line notes if operator-visible
- **Out of scope:**
  - Metrics merge OOM (separate)
  - Making landing searchable (refresh contract stays)
  - Quarantine/rename on the RO query plane
  - Homelab gitops image pin (follow-up after release)

## 3. Open questions

- [x] Q: Skip vs quarantine on query? — A: **Skip only.** Shared Grafana
  proxy mounts data read-only; rename would fail. Writer scan skip is enough
  to stop attaching poison files. Leftovers age out via retention.
- [x] Q: Oldest-first drain vs last-1h canary? — A: **One newest pack first**
  when budget ≥2 and backlog exceeds one pack. Remaining actions stay
  oldest-first so a stall still drains.
- [x] Q: Pack size? — A: Metrics 24h floor (~360 MiB) yields merge-at-once ≈6
  for 536 KiB landings. Logs derive from **1 MiB floor**, cap **64** files per
  action to stay inside the 3 GiB DuckDB merge limit.

## 4. Decision log

- **Skip empty files instead of failing the relation/tick.**
  - ref: https://duckdb.org/docs/stable/data/parquet/overview.html — parquet
    needs a header+footer; a 0-byte file makes `read_parquet([…])` fail the
    whole list.
  - perf: one `Stat` size check per cached path; cheaper than a failed sandbox.
  - product: one leftover cannot hide every other log line.

- **Newest-first first refresh action after a stall.**
  - ref: https://www.elastic.co/guide/en/elasticsearch/reference/8.19/near-real-time.html —
    refresh exists so *recent* ops become searchable; draining a week of
    buffer oldest-first leaves last-1h empty.
  - perf: one extra pack per tick; disjoint sources.
  - product: Grafana last-1h canary recovers on the next merge tick.

- **1 MiB floor for log pack width, cap 64.**
  - ref: existing `derivedMaxMergeAtOnce` + DuckDB memory errors at 3 GiB on
    this tenant's metrics merge.
  - perf: 64×536 KiB ≈ 34 MiB attach per action; 8 actions still beat ingest.
  - product: backlog drains in minutes, not days, without stampeding merge RAM.

## 5. Acceptance checklist

- [x] `segformat.TooSmall` treats 0 and `<8` bytes as unusable; 8+ is not
- [x] Loki/`/sql` open set skips a 0-byte tier parquet and still reads a valid sibling
- [x] `ScanLogLanding` omits a 0-byte landing file and still returns valid neighbors
- [x] Promote listing omits a 0-byte L1 parquet
- [x] Refresh with budget ≥2 and backlog > one pack plans a newest-first action first
- [x] Oldest-first drain still uses the remaining budget (disjoint sources)
- [x] Log pack-at-once uses the 1 MiB floor (capped) when the planner floor is larger
- [x] Tests written first (a `test:` commit precedes implementation)
- [x] `make lint test` green locally

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
