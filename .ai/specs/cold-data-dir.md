# Spec: Cold data dir (`COLD_DATA_DIR`) — promote L1+ off the hot disk

Status: DRAFT
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->
<!-- Phase 0: open questions live in the homelab-apps orchestrator spec. Do not implement until both specs are READY. -->

- **Slug / branch:** `cursor/prism-cold-tier-baa6`
- **Owner phase:** orchestrator
- **PLAN phase(s):** store lifecycle / layout
- **Canonical product spec (Helm, Grafana, host paths, open questions):**
  [`../../../homelab-apps/.ai/specs/prism-cold-tier.md`](../../../homelab-apps/.ai/specs/prism-cold-tier.md)

## 1. Task

Add an optional second data root so compacted **L1+** parquet can live on a
slower device (spinning disk, later network) while **hot + L0 stay on
`DATA_DIR`**. Unmerged L0 stays hot even if older than 12h. Copy must be
crash-safe: tmp on the dest FS, fsync, checksum, atomic rename, retry forever,
GC stale temps, never delete the hot source until the cold dest is verified.

## 2. Scope

- **In scope:** `COLD_DATA_DIR` / `COLD_AFTER` env; layout helpers; promote job
  on `RUN_JOBS=true`; crash GC of `*.promote.tmp`; dual-root `ScanAllTiers` /
  logs scan / metrics+logs catalogs so query, PromQL, Loki, `/sql` still see
  promoted files; tests for kill-mid-copy, checksum mismatch, L0 exclusion;
  `docs/CONFIG.md` + `docs/STORE.md`.
- **Out of scope:** force-merge L0→L1 after N periods; Helm/charts (homelab-apps);
  object storage; changing merge N / retention / memory caps; GC of pre-existing
  merge `*.tmp` leftovers (not `*.promote.tmp`).

## 3. Open questions  (must be empty/answered before `Status: READY`)

Mirrored from the homelab spec — answer there; copy the A: line here.

- [ ] Q1: prism `COLD_DATA_DIR` in scope (vs Helm-only L1 bind-mount)? — A: PENDING
- [ ] Q2: 12h clock = catalog `max_ts_ns`? — A: PENDING
- [ ] Q3: merge still writes L1+ on hot, promote copies after? — A: PENDING
- [ ] Q4: logs L1+ cold; rollups/mats/manifest stay hot? — A: PENDING
- [ ] Q5: prod host path (apps/gitops, not this repo) — A: N/A here
- [ ] Q6: migrate existing hot L1+ via promote? — A: PENDING
- [ ] Q7: Grafana dual glob vs per-tier bind-mount — A: PENDING
  (“worse promote” on bind-mount = merge COPY writes L1 straight to HDD;
  no SSD canonical file to checksum/retry-copy from. Dual glob keeps two
  roots so promote can copy SSD→HDD. Engine HTTP query still unions both
  roots either way.)
- [ ] Q8: SHA-256 + parquet open before unlink source? — A: PENDING
- [ ] Q9: two `t.TempDir()`s in tests; no k3d requirement — A: PENDING
- [ ] Q10: POSIX local now; network FS = same loop later — A: PENDING

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Two roots + promote, not `os.Rename` across devices:
  - ref: https://alexwlchan.net/2019/atomic-cross-filesystem-moves-in-python/
    — rename is atomic only on one filesystem; cross-device copy uses unique
    tmp **in the destination directory**, then rename, then unlink source.
  - perf: DuckDB merge COPY stays on SSD; HDD only sequential copy of compacted
    segments.
  - product: SSD remains canonical until dest checksum+rename succeed; retries
    never give up; crash GC cannot publish truncated files.

## 5. Acceptance checklist  (developer checks these off)

- [ ] `COLD_DATA_DIR` empty/unset: identical layout and query behavior to today
- [ ] L0 never promoted, including when `max_ts` is older than `COLD_AFTER`
- [ ] Eligible L1+ copied to cold with unique `*.promote.tmp`, fsync, checksum,
      same-FS rename; hot unlinked only after dest verifies (plus existing delete
      grace if still required for globbing clients)
- [ ] Kill mid-copy: tmp GC’d; no truncated canonical dest; source intact; retry
- [ ] Checksum mismatch / short write: dest not published; source kept
- [ ] `ScanAllTiers` / logs scan / manifests union hot + cold
- [ ] `RUN_JOBS=false` still **reads** cold (proxy); only jobs promote
- [ ] Metrics: promote attempts, successes, retries, bytes, in-flight tmp count
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)
- [ ] CONFIG.md documents `COLD_DATA_DIR` and `COLD_AFTER`

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
