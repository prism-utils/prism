# Spec: Bounded L0 window compact (age catch-up + named policies)

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/l0-window-compact-baa6`
- **Owner phase:** developer
- **PLAN phase(s):** Store merge compaction (follow-on to prod soak vs Lucene adjacency)
- **Ships as:** git tag `v1.0.10` → `ghcr.io/prism-utils/prism-store:1.0.10` then homelab pin
- **Depends on:** `main` includes `COLD_DATA_DIR` (#147). Merge still writes L1 on
  `DATA_DIR`; promote copies aged L1+ to cold. Compact must not write L1 onto
  cold directly.

## 1. Task

Prod tenant `user-fqsejat4-apps` accumulated 122 L0 files because the metrics
planner requires Lucene **size-level + time-adjacency**. Tiny flushes hours or
days apart never pack; a 215Mi singleton sits in its own size level. Logs
already pack without adjacency. This task adds **option 3 + 4**: an automatic
**age catch-up** that packs fully-aged L0s without adjacency under a byte/file
budget, plus **named window policies** (YAML + admin `POST`) for rolling and
calendar (one dest per UTC day) compact. Same merge executor (`ORDER BY ts`,
dest `L{n+1}` on the hot root, delete grace, materialize). Not an unbounded
“merge all”. Then pin the new store image on Homelab writers and verify prod
L0 count drops.

## 2. Scope

- **In scope (prism):**
  - Compact **selector** (tier, fully-aged window, optional UTC `bucket`,
    `maxSources`, `maxBytes`) in `internal/store/merge`.
  - Built-in **age catch-up** ON by default (`olderThan=15m`, `maxSources=32`,
    `maxBytes=256Mi`, no `newerThan`). Disable with `COMPACT_AGE_CATCHUP=false`.
  - Optional `COMPACT_FILE` YAML named policies (`every`, `bucket: day|hour|none`).
  - `POST /admin/tenants/{ns}/compact` (`dryRun` sync plan; else **202** enqueue;
    `RUN_JOBS=false` is 204). RBAC = `ActionEnsure`.
  - Eligibility: `max_ts <= now - olderThan` (fully aged). Include **any**
    unsealed segment that meets the window + caps (including a 215Mi file if it
    fits `maxBytes`).
  - Tick order per tenant, **one** merge action: purge compacted → Lucene
    `FindMerges` → else catch-up → else first due named policy (oldest UTC
    bucket first).
  - Docker e2e (`//go:build e2e`, `requireDocker`, compose like
    `test/e2e/format_matrix_e2e_test.go`) proving aged small L0s compact to L1
    with `.compacted` sidecars and a second case for `bucket: day`.
  - Metrics for compact plan/execute (policy name, source count, bytes).
  - Docs: `STORE.md`, `CONFIG.md`, admin route table.
- **In scope (homelab-apps + gitops, after prism tag):**
  - prism-cache ConfigMap `compact.yaml` (`recent` + `daily` policies) +
    `COMPACT_FILE` / catch-up env (default on).
  - Reconciler helm params if image/env is passed that way.
  - gitops pin `ghcr.io/prism-utils/prism-store:1.0.10` on writers that run
    jobs (prism-cache / live-demo). Proxy stays `RUN_JOBS=false` (no compact).
  - Prod verify: `user-fqsejat4-apps` L0 count falls and compacted markers appear.
- **Out of scope:**
  - Changing Lucene adjacency for **young** L0s.
  - Unbounded merge-all.
  - Logs plane (already packs without adjacency).
  - CPU limit bump, DuckDB cwd `/tmp`, serialize merge/materialize/rollup,
    leftover `hot/*.tmp` GC, footer StatSegment, lowering `MAX_SEGMENT_BYTES`,
    `hot_current` byte cap (later specs).
  - MODE=cluster scatter-gather.

## 3. Open questions

- [x] **Q1** Ship prism **and** Homelab (not prism-only). Compact 3+4 this wave;
      other soak follow-ups stay later. — A: both
- [x] **Q2** Catch-up ON by default, `olderThan=15m`, `maxSources=32`,
      `maxBytes=256Mi`. — A: yes
- [x] **Q3** Engine + Homelab named `recent` / `daily` YAML. — A: both
- [x] **Q4** `dryRun` sync; real run **202** enqueue. — A: yes
- [x] **Q5** Eligible iff fully aged (`max_ts`). — A: yes
- [x] **Q6** Not tiny-only: every unsealed segment that meets the selector
      (so daily bucket can make one segment per day, including large files
      that fit `maxBytes`). — A: all matching
- [x] **Q7** `bucket: day|hour` is UTC only in v1. — A: UTC
- [x] **Q8** RBAC = `ActionEnsure`. — A: yes

## 4. Decision log

- **Bounded window compact, not merge-all:**
  - ref: https://www.elastic.co/guide/en/elasticsearch/reference/8.19/indices-forcemerge.html
  - ref: https://www.elastic.co/docs/reference/elasticsearch/index-lifecycle-actions/ilm-forcemerge
  - perf: COPY capped by `maxSources` and `maxBytes` (256Mi catch-up).
  - product: “drain aged L0s” and “one file per UTC day” without rewriting retention.

- **Keep Lucene for young L0s; catch-up only fully-aged files:**
  - ref: https://lucene.apache.org/core/9_11_1/core/org/apache/lucene/index/TieredMergePolicy.html
  - ref: prism `docs/STORE.md` logs landing pack (no adjacency)
  - perf: 3s stat ticks stay; extra COPY only when ≥2 fully-aged unsealed L0s fit.
  - product: recent packs stay tight for prune; yesterday’s tiny files drain over ticks.

- **YAML file + admin POST, same as materializations:**
  - ref: https://github.com/prism-utils/prism/blob/main/docs/STORE.md
  - perf: parse once at start.
  - product: gitops ships `recent` / `daily` without a binary change.

- **One action per tenant per merge tick; HTTP does not COPY:**
  - ref: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge
  - perf: merge DuckDB stays exclusive.
  - product: `dryRun` previews; 202 means queued for the tick.

- **L1 dest stays on DATA_DIR (hot):** COLD_DATA_DIR (#147) copies L1+ later.
  Compact must use the same `ExecuteMerge` dest as Lucene (hot root).
  - ref: `.ai/specs/cold-data-dir.md` Q3
  - perf: DuckDB COPY stays on SSD.
  - product: promote still checksums SSD→HDD.

## 5. Acceptance checklist  (developer checks these off)

### Prism

- [x] Selector unit tests: fully-aged vs overlap; `maxSources` / `maxBytes`
      shrink; sealed files excluded; `<2` sources ⇒ no action; `bucket: day`
      emits one UTC day per call (oldest eligible day first); large + small
      files in one pack when sum ≤ `maxBytes`.
- [x] Lucene `FindMerges` unchanged for young files (existing planner tests pass).
- [x] Catch-up on merge tick when Lucene returns empty (Lucene first, else
      catch-up, else due named policy). Default ON.
- [x] `COMPACT_FILE` load/validate (name regex, duration parse, required caps,
      duplicate names fail start). Empty path = no named policies.
- [x] `POST /admin/tenants/{ns}/compact` dry-run JSON; non-dry-run 202 enqueue;
      `RUN_JOBS=false` 204; unknown tenant 404; bearer/RBAC same as ensure.
- [x] Enqueue is per-tenant, consumed on the next merge tick as the single
      action (takes priority over catch-up that tick).
- [x] Executor path identical to Lucene packs (dest tier+1 on **hot** data dir,
      grace, materialize hook already on `ExecuteMerge`).
- [x] Docker e2e: aged L0s → L1 + compacted sidecars; `bucket: day` isolates
      days. `requireDocker` + compose (follow `format_matrix_e2e_test.go`).
- [x] Tests written first (a `test:` commit precedes implementation)
- [x] `make lint test` green; `make e2e` covers the new compact tests
- [x] `STORE.md` + `CONFIG.md` document catch-up knobs, YAML schema, admin route

### Homelab (after `v1.0.10` image exists — may be a follow-up PR on the same
branch name in homelab-apps / homelab-gitops)

- [ ] prism-cache chart: ConfigMap compact policies `recent` + `daily`,
      `COMPACT_FILE`, catch-up left at default ON
- [ ] Chart unit tests for the new env/volume
- [ ] gitops pin `1.0.10` on prod (and dev) prism-cache / live-demo writers
- [ ] Prod `user-fqsejat4-apps`: after Argo Healthy, L0 count drops vs pre-pin
      and `.compacted` markers appear (or compact dry-run lists them)

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_

## 8. Behavior

Tick order per tenant, **one** merge action:

1. Purge compacted (existing).
2. If an admin enqueue exists for that tenant → that action.
3. Else Lucene `FindMerges`.
4. Else built-in age catch-up (if enabled).
5. Else first due named policy (`every` elapsed, oldest UTC bucket first).

Selector:

| Field | Catch-up default | Named policy / admin |
|---|---|---|
| `tier` | 0 | default 0 |
| `olderThan` | 15m | optional duration |
| `newerThan` | unset | optional (rolling window) |
| `from`/`to` | n/a | admin inline RFC3339 |
| `bucket` | none | `none` \| `hour` \| `day` (UTC) |
| `maxSources` | 32 | required ≥ 2 for named |
| `maxBytes` | 256Mi | required, ≤ `MAX_SEGMENT_BYTES` |

Inline admin without `policy` and without `from`/`to`/`olderThan` is **400**.

Homelab `recent` / `daily` file (prism-cache):

```yaml
compact:
  policies:
    - name: recent
      tier: 0
      olderThan: 15m
      newerThan: 1h
      maxSources: 32
      maxBytes: 256Mi
      every: 45m
    - name: daily
      tier: 0
      bucket: day
      olderThan: 1d
      newerThan: 3d
      maxSources: 64
      maxBytes: 512Mi
      every: 1h
```
