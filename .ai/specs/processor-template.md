# Spec: processor/template — log-template mining

Status: READY

- **Slug / branch:** `feat/processor-template`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 6 (template only; no ml)

## 1. Task

Implement the `template` processor: normalize semi-structured log lines into a
stable **template key** (invariant skeleton with variable tokens masked, e.g.
`user <*> logged in from <*>`) and add it as a column, so summaries can group by
log shape. Wrap `github.com/air-gapped/lessence` (pure-Go, in-process) when it
provides this; otherwise implement a **decent Drain-style** streaming template
miner in-tree (fixed-depth parse tree clustering) — not a toy. `enabled: false`
is a proven identity no-op. Per DESIGN.md §8.

## 2. Scope

- **In scope:** `internal/processor/template`; config (`field`, `target`,
  `enabled`, miner params like depth/similarity threshold); the lessence wrap OR
  the in-tree Drain miner; adds a `template` column; deterministic golden cases.
- **Out of scope:** `ml`; `script`; summary aggregation.

## 3. Open questions  (resolved)

- [x] Q: lessence available/pure-Go? — A: exists (v0.4.5); developer verifies it
      is pure-Go and provides template mining; if not, implement Drain in-tree.
- [x] Q: minimal or decent? — A: decent, Drain-quality, with online clustering.

## 4. Decision log

- Template mining via lessence, Drain-style fallback.
  - ref: https://github.com/air-gapped/lessence — Go logging-normalization lib.
  - ref: https://github.com/logpai/logparser (Drain: fixed-depth tree online
    parsing; He et al., ICWS 2017) — algorithm for the fallback.
  - perf: online, per-line O(depth); no per-record heap growth on hot path
    (bounded cluster table).
  - product: grouping by template is the accepted way to summarize high-cardinality
    logs; used across observability tooling.

## 5. Acceptance checklist

- [ ] Developer verifies lessence is pure-Go and fit; records outcome in this
      spec's decision log. Uses it, or the Drain fallback if unfit.
- [ ] Adds a `template` column; original fields preserved; never drops rows.
- [ ] Golden cases: varied lines cluster to the expected templates deterministically.
- [ ] `enabled: false` → identity (batch out == batch in), proven.
- [ ] Bounded cluster table (no unbounded growth on unique lines); documented cap.
- [ ] No per-record heap alloc on the hot path (benchmark).
- [ ] `Validate()` for params; path-named errors.
- [ ] Tests written first; `make lint test` green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (unique lines, empty field, disabled)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
