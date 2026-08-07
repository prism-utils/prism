# Spec: Logs merge packs toward MAX_SEGMENT_BYTES

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `fix/logs-merge-pack-max`
- **Owner phase:** orchestrator
- **PLAN phase(s):** Phase — Store lifecycle / merge (bugfix; issue #99)
- **Issue:** https://github.com/elk-utilities/prism/issues/99

## 1. Task

Prod with `MAX_SEGMENT_BYTES=3GiB` and `SEGMENTS_PER_TIER=6` leaves hundreds of
tiny log landing files because each merge tick only packs **6→1**. The planner
must keep using `SEGMENTS_PER_TIER` as the **trigger** (≥N unsealed), then pack
as many time-ordered unsealed segments as fit under `MAX_SEGMENT_BYTES`, leaving
sealed segments (`Bytes ≥ MAX_SEGMENT_BYTES`) alone until retention. Apply the
same pack-to-max ceiling for log tiers and metrics where `MaxMergeAtOnce=6`
blocks fill-to-max.

## 2. Scope

- **In scope:**
  - `internal/store/merge` — `findLogLandingMerge` candidate window; planner
    `MaxMergeAtOnce` default/derivation; metrics/log-tier packing via the same
    knob
  - `internal/store/lifecycle` — `TickMerge` must not hard-wire
    `MaxMergeAtOnce = SegmentsPerTier`
  - Docs: `docs/CONFIG.md`, `docs/STORE.md` — pack fills toward
    `MAX_SEGMENT_BYTES`; `SEGMENTS_PER_TIER` is trigger only
  - Tests for pack-to-max, seal exclusion, shrink-when-over, trigger threshold
- **Out of scope:** coalesce/pending orphans; retention policy changes; new
  operator env unless derivation alone is insufficient; Grafana/query changes

## 3. Open questions  (must be empty/answered before `Status: READY`)

All resolved by the user before this loop (Phase 0).

- [x] Q: Pack limit N=6 or fill toward max bytes? — A: Pack until approaching
      `MAX_SEGMENT_BYTES`.
- [x] Q: What about sealed segments? — A: Ignored until retention (keep current).
- [x] Q: Role of `SEGMENTS_PER_TIER`? — A: Trigger only (≥N to start), not pack
      limit.
- [x] Q: Safety cap on how many files one action may merge? — A: High
      `MaxMergeAtOnce` (configurable or derived) so tiny-file catch-up can fill
      the byte budget; shrink subset if sum would exceed max.
- [x] Q: Logs landing only, or metrics/log tiers too? — A: Prefer landing first;
      apply same pack-to-max wherever `MaxMergeAtOnce=6` blocks fill-to-max;
      document in this decision log.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Pack toward `MAX_SEGMENT_BYTES` once triggered (not cap at N=6):** take up to
  `MaxMergeAtOnce` time-ordered unsealed candidates, then shrink from the end
  until summed bytes ≤ `MAX_SEGMENT_BYTES` (same shrink-to-fit as today).
  - ref: https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/index/TieredMergePolicy.html
    — Lucene separates `segmentsPerTier` (budget/trigger) from `maxMergeAtOnce`,
    and shrinks merges that would exceed `maxMergedSegmentMB`. We need the
    **inverse for tiny files**: raise `maxMergeAtOnce` so one action can fill
    toward the byte budget (our prior `MaxMergeAtOnce = SegmentsPerTier` was too
    low for KB-sized landings under a multi-GiB seal).
  - perf: fewer merge ticks and fewer tiny L0 outputs → less DuckDB scan fan-out
    and lower write amplification vs 6→1 forever; one larger COPY is still
    bounded by `MAX_SEGMENT_BYTES` and existing DuckDB thread/memory limits.
  - product: matches documented seal semantics and the prod catch-up need (#99).

- **`MaxMergeAtOnce` derived when unset:**
  `max(SegmentsPerTier, MaxSegmentBytes/FloorBytes)` (FloorBytes defaulted as
  today). `TickMerge` passes `0` (or omits) so derivation applies — do **not**
  hard-wire `MaxMergeAtOnce = SegmentsPerTier`. Explicit test configs may still
  set a small `MaxMergeAtOnce`.
  - ref: same TieredMergePolicy page — `setMaxMergeAtOnce` is independent of
    `setSegmentsPerTier`; default Lucene maxMergeAtOnce is 10, but our seal
    budget / floor ratio is the natural upper bound for “how many floor-sized
    pieces fit in one sealed segment.”
  - perf: O(candidates) planning over paths already scanned; no extra I/O in the
    planner. Merge I/O bounded by byte budget, not by an artificial N=6.
  - product: no new required env for the common case; operators already set
    `MAX_SEGMENT_BYTES` / hot window (floor).

- **Same packing for metrics tiers and log tiers:** raise the shared
  `MaxMergeAtOnce` used by `findMergeForTier` / `pickTimeAdjacent` so size-level
  time-adjacent runs can also fill toward max (not only logs landing).
  - ref: TieredMergePolicy — one policy knobs for normal merging across the
    index; prism already shares `Planner` for metrics and log tiers.
  - perf: same as landing — fewer mid-size segments stuck below seal.
  - product: consistent seal/trigger semantics across planes; avoids a second
    “only landing packs” surprise.

- **Sealed segments remain non-inputs until retention:** unchanged.
  - ref: TieredMergePolicy `maxMergedSegmentMB` — do not keep growing past the
    configured max segment size during normal merging.
  - perf: avoids rewriting multi-GiB sealed files.
  - product: retention owns sealed lifetime (`RETENTION_DAYS` / `MAX_LOG_FILES`).

## 5. Acceptance checklist  (developer checks these off)

Concrete, verifiable deliverables for this task. Add as many as needed.

- [x] Logs landing: ≥ `SEGMENTS_PER_TIER` tiny unsealed files → one action whose
      sources sum as close as possible to (≤) `MAX_SEGMENT_BYTES`, and source
      count may exceed `SEGMENTS_PER_TIER` when `MaxMergeAtOnce` allows
- [x] Logs landing: sealed (`Bytes ≥ MAX`) never selected; all-sealed → no action
- [x] Logs landing: candidate set shrinks when sum would exceed max
- [x] Trigger unchanged: fewer than `SEGMENTS_PER_TIER` unsealed → no action
- [x] Metrics / log-tier planning uses derived (or high) `MaxMergeAtOnce` so the
      same fill-toward-max applies when the old N=6 cap would block it
- [x] `TickMerge` no longer forces `MaxMergeAtOnce = SegmentsPerTier`
- [x] `docs/CONFIG.md` + `docs/STORE.md` state: trigger = `SEGMENTS_PER_TIER`;
      pack fills toward `MAX_SEGMENT_BYTES` under `MaxMergeAtOnce`
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
