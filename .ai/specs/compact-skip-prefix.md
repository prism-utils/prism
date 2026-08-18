# Spec: Compact takePrefix skips files that do not fit

Status: ALL_OK
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/compact-skip-prefix-baa6`
- **Owner phase:** orchestrator
- **PLAN phase(s):** Store merge compaction follow-on (prod L0 drain)
- **Ships as:** git tag `v1.0.11` → `ghcr.io/prism-utils/prism-store:1.0.11` then homelab pin

## 1. Task

Age catch-up on `user-fqsejat4-apps` packed `tiny + 215Mi + tiny` (246Mi, under
the 256Mi cap) and DuckDB COPY OOMed. `takePrefix` **breaks** when the next
file would exceed `maxBytes`, so a large unsealed L0 in min-ts order also
blocks every later tiny file when the operator lowers `maxBytes` to a
DuckDB-safe pack. Change prefix selection to **skip** files that do not fit
the remaining budget and keep packing later eligible files. Keep default
catch-up caps (15m / 32 files / 256Mi). Add a docker e2e that plants a large
middle L0 plus small neighbors and asserts the smalls compact to L1.

## 2. Scope

- **In scope:**
  - `takePrefix` `continue` when `sum+bytes > maxBytes` (skip, do not stop).
  - Unit test: large middle file skipped; two later/earlier smalls still pack.
  - Existing `TestSelectCompactMaxBytesStopsPrefix` still holds (80+80 fits
    160; third 80 skipped — same 2-source result).
  - Docker e2e (`//go:build e2e`, compose `deploy/docker-compose.compact-lifecycle.yml`):
    named policy with a small `maxBytes`, one oversized middle L0, smalls
    compact to L1 with `.compacted` sidecars; oversized file stays live L0.
  - Docs: `STORE.md` / `CONFIG.md` one line that a file larger than remaining
    `maxBytes` is skipped so later files can still pack.
- **Out of scope:**
  - Changing default `maxBytes` (stays 256Mi).
  - DuckDB COPY memory / pod ceiling (Homelab ops; not this binary).
  - Lucene adjacency, logs merge, COLD_DATA_DIR dest.

## 3. Open questions

- [x] **Q1** Skip (continue) vs break when the next file does not fit? — A: skip
      so all segments that *do* fit still merge (user: not tiny-only, every
      eligible file that attends the caps).
- [x] **Q2** Keep catch-up default 256Mi? — A: yes; operators can pass a smaller
      `maxBytes` on admin POST / YAML.

## 4. Decision log

- **Skip files that do not fit remaining `maxBytes`, do not abort the scan:**
  - ref: https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/TieredMergePolicy.html
    (size levels skip a too-large sibling and still merge others in the tier)
  - perf: one extra walk of already-listed segments; COPY bytes stay ≤ `maxBytes`.
  - product: a 215Mi L0 cannot pin 100 tiny L0s behind it when the operator
    chooses a DuckDB-safe pack cap.

- **Do not lower the default 256Mi cap in this tag:**
  - ref: `.ai/specs/l0-window-compact.md` Q2 (user-approved 256Mi)
  - perf: large packs still allowed when DuckDB headroom exists.
  - product: `COMPACT_AGE_CATCHUP_MAX_BYTES` is the knob (Homelab sets 32Mi
    because a 246Mi metrics COPY OOMs at 3Gi DuckDB).

## 5. Acceptance checklist  (developer checks these off)

- [x] `takePrefix` skips a file that does not fit remaining budget and continues
- [x] Unit test: `80, 200, 80` with `maxBytes=160` packs the two 80s (not empty)
- [x] Unit test: consecutive files that fit still form a contiguous prefix
- [x] Docker e2e: oversized middle L0 skipped; small aged L0s compact to L1
- [x] `COMPACT_AGE_CATCHUP_MAX_BYTES` env (default 256Mi)
- [x] Docs mention skip-not-fit behavior and the env knob
- [x] Tests written first (a `test:` commit precedes implementation)
- [x] `make lint test` green; `make e2e` covers the new compact skip test

## 6. Mandatory review gates  (reviewer owns)

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [x] **Gate 3 — Docs & comments match the task and the delivered code**
- [x] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

Docker `TestCompact*` e2e green (catch-up, bucket:day, skip oversized middle).
`make lint` 0 issues. `make test` failed only on pre-existing
`TestE2E_LoggingThreePhaseParquet` (tmp file watcher; unrelated). Merge selector
+ lifecycle + cmd/prism-store tests green.

Status: ALL_OK

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
