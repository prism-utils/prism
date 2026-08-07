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
      (landing excluded by design). — the label index now describes the same
      searchable set: it is built from tier entries only, a land contributes no
      values (it just carries the index stamp across its generation bump), and a
      refresh folds its output in.
- [x] `TickMerge` can apply multiple landing refreshes per artifact per tick
      (`LOGS_REFRESH_MAX_ACTIONS`, default 8).
- [x] `TickFlush` calls `FlushLogCoalesce` when coalesce is configured.
- [x] Chart/values + CONFIG.md + STORE.md document the knobs and semantics.
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [x] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

### Review 1 — CHANGES_REQUESTED

**Verified green (re-run by the reviewer, not trusted from the branch):**
`make lint test` (`golangci-lint`: 0 issues; `go test -count=1 -race -tags
duckdb_arrow ./...`: all packages ok) and `make full-tests GOFLAGS=-count=1`
(lint + race unit + docker integration + e2e, 226s, `full-tests: OK`). Both were
run with `-count=1` because the developer's run was fully cached.

**TDD contract holds.** History is `test:` before every implementation commit,
three times over (planner/catalog/drain, env+chart loading, duckdb tier
cataloguing). Conventional Commit subjects with correct package scopes.

**Blocking — the searchable-set change stops at the logs relation.**
`RebuildManifest` still catalogs landing windows, `EnsureLabelIndex` indexes
every `.parquet` entry in that manifest, and the Loki handler answers
`label/<name>/values` from the index for indexed labels instead of scanning the
relation. Net effect: a label value that only exists in a buffered landing
window is returned by the label API while `query_range` returns no lines for it,
for up to a refresh interval. Reproduced against this branch: with one landing
window (`format=buffered-format`) and one tier segment
(`format=refreshed-format`), `logmeta.LabelValues(..., "format", ...)` returns
both. This also makes the delivered STORE.md sentence "neither Loki nor `/sql`
opens landing files" untrue. Either scope the label index to tier segments (and
keep the docs as written), or narrow the docs and accept the skew explicitly —
whichever way, it needs a test that seeds a landing-only buffer and asserts the
label API result.

**Non-blocking observations (no action required to merge):**

- With `LOGS_REFRESH_INTERVAL=0` on a low-volume artifact, landing can now age
  past `RETENTION_DAYS` / `MAX_LOG_FILES` and be deleted without ever having
  been searchable. CONFIG.md warns the artifact "can then stay unsearchable
  indefinitely", which covers the visibility half but not the deletion half.
- `envIntAllowZero` silently falls back to the default on unparsable input with
  no log line, matching the existing `envInt` house pattern. Consistent, but an
  operator who typos `LOGS_REFRESH_INTERVAL` gets no signal.

**Checked and clean:** planner drain terminates (sealed segments are filtered
before packing, so a single-segment pack always fits the seal budget and the
loop always consumes at least one candidate); action sources are disjoint and
oldest-first; the age arm reads the ingest window id, not event time; landing
refresh is planned ahead of the cold-tier pack without starving it;
`FlushLogCoalesce` is a no-op when coalesce is unconfigured and both branches
are covered; chart values, statefulset env, and the golden fixture agree on
`60` / `8`; no new dependency, no new goroutine, no global mutable state.

### Developer response — review 1

Took the first option: the label index now describes the searchable set, so the
STORE.md sentence stands as written.

- `EnsureLabelIndex` indexes tier entries only, so a rebuild can no longer pick
  up a buffered window.
- A land contributes no values. It still bumps the generation, so it carries the
  index stamp forward (`CarryLabelIndex`) instead of stranding it one generation
  behind and making the next label query rescan every tier segment. An index
  that was already stale is left alone, so the rebuild still happens.
- The refresh folds its own output into the index, which is where buffered
  values legitimately become searchable.
- Tests land first and fail on the old code at all three levels: the index
  rebuild (`logmeta`), the land path (`engine`), and the HTTP label API
  (`query`), each with a landing-only value and a different tier value.
- STORE.md now says outright that the index is built from tier segments only.

The two non-blocking observations are left as-is.
