# Spec: logs refresh interval (ES-like searchable lag)

Status: IN_REVIEW

- **Slug / branch:** `fix/logs-refresh-interval`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** store logs lifecycle / query visibility

## 1. Task

Prod accumulates hundreds of hot landing `.duckdb` files while Grafana DuckDB
charts (parquet-only under tiers) lag ~8–10 minutes. Desired model (Elasticsearch
refresh analogue): small windows land into a **non-searchable buffer**; a
configurable **refresh** packs buffered segments into searchable L0; operators
tune lag (~1m default) vs segment count. Ship the store fix, then homelab bumps
the released image and verifies admin logging + Grafana.

## 2. Scope

- **In scope:**
  - Landing planner: refresh when **oldest live landing ≥ `LOGS_REFRESH_INTERVAL`**
    OR **live count ≥ `SEGMENTS_PER_TIER`** (whichever first).
  - Default `LOGS_REFRESH_INTERVAL` = **60s** (1m). `0` disables the age trigger
    (count-only, today’s behavior for age).
  - Query visibility: Loki + `/sql` logs catalog **exclude landing** — only
    `tiers/L*` (post-refresh) are searchable. Matches “hot buffer not returned
    on search.”
  - Per-artifact merge drain: apply up to **N** landing→L0 refresh actions per
    `TickMerge` (config `LOGS_REFRESH_MAX_ACTIONS` default **8**) so backlog
    cannot outrun agents.
  - Wire `FlushLogCoalesce` into `TickFlush` when coalesce is enabled (close
    `.pending` visibility hole).
  - Docs: `CONFIG.md` + `STORE.md` refresh semantics; chart values + statefulset
    env for new knobs; golden chart test.
  - Regression tests: age trigger, count trigger, landing excluded from catalog,
    multi-action drain, coalesce flush on tick.
- **Out of scope:**
  - Changing agent buffer `max_age` (stays agent-side).
  - Rewriting Grafana `initSQL` in homelab-apps (parquet tiers already correct
    once refresh is ≤1m); homelab only bumps image + env after release.
  - Metrics hot-window / PromQL snapshot path.
  - Explicit HTTP `_refresh` API (follow-up); background tick is enough.

## 3. Open questions  (resolved before READY)

- [x] Q: Should Loki/SQL keep reading landing (today) or wait for refresh? —
  A: **Wait for refresh** (user: hot buffer must not return on search).
- [x] Q: Acceptable lag? — A: **~1m default**; more than a few minutes is not OK.
- [x] Q: Refresh vs compaction? — A: Refresh = landing→L0 pack (searchable).
  Existing tier→tier merges remain compaction; prefer landing refresh when both
  are due so the searchable lag stays bounded.
- [x] Q: Format mismatch (.duckdb landing → parquet L0)? — A: Keep
  `MERGE_SEGMENT_FORMAT` (parquet default); refresh output stays parquet so
  Grafana globs work without initSQL changes.

## 4. Decision log  (Decision Protocol)

- **ES-like refresh: buffer not searchable until refresh opens L0.**
  - ref: https://www.elastic.co/guide/en/elasticsearch/reference/8.19/near-real-time.html —
    refresh makes recent ops visible to search; separate from merge compaction.
  - perf: query opens far fewer files (tiers only); ingest stays append-only
    landings; refresh cost amortized on the merge ticker.
  - product: operators get a single lag knob (~1m) and stop seeing
    multi-minute Grafana blind spots / hundreds of hot files.

- **Age OR count trigger (not count-only).**
  - ref: ES `index.refresh_interval` (default 1s stack / 5s serverless) —
    time-based visibility; our 60s default is a deliberate coarser trade for
    DuckDB `COPY` cost vs Lucene refresh.
  - perf: low-volume tenants still refresh within 1m instead of waiting forever
    below `SEGMENTS_PER_TIER`.
  - product: matches the user’s “enough segments **or** short period” control.

- **Bounded multi-action drain per tick.**
  - ref: ES refresh is cheap per shard; our refresh is a full segment rewrite —
    cap actions/tick to bound CPU while still beating agent fill rate
    (~2 files/min/artifact/replica).
  - perf: default 8 actions × net shrink prevents unbounded landing growth
    without stampeding DuckDB connectors.
  - product: avoids the current arithmetic failure (drain −5/min vs multi-agent
    fill).

- **Wire coalesce flush on `TickFlush`.**
  - ref: same near-real-time docs — buffered docs must eventually become
    visible; an unwired age seal leaves `.pending` invisible forever.
  - perf: reuses `FLUSH_TICK_SECONDS`; no new ticker.
  - product: coalesce remains opt-in but safe when enabled.

## 5. Acceptance checklist  (developer checks these off)

- [x] `LOGS_REFRESH_INTERVAL` (seconds, default 60) age-triggers landing→L0 when
      oldest live landing file exceeds the interval (even if count &lt; segments-per-tier).
- [x] Count trigger (`SEGMENTS_PER_TIER`) still refreshes immediately when enough
      live files accumulate.
- [x] Loki + `/sql` logs relation **omit landing**; only tier segments are scanned
      (landing excluded by design).
- [x] `TickMerge` can apply multiple landing refreshes per artifact per tick
      (`LOGS_REFRESH_MAX_ACTIONS`, default 8).
- [x] `TickFlush` calls `FlushLogCoalesce` when coalesce is configured.
- [x] Chart/values + CONFIG.md + STORE.md document the knobs and semantics.
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
