# Spec: processor/summary — windowed group-by aggregates

Status: ALL_OK

- **Slug / branch:** `feat/processor-summary`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 6 (summary only)

## 1. Task

Implement the `summary` processor: over the Arrow columns of one flushed window
(the buffer already did the windowing), compute group-by aggregates —
`count`, `sum(col)`, `avg(col)`, `min(col)`, `max(col)`, `pXX(col)` — grouped by
configured columns, producing a small `RecordBatch` of aggregate rows. Paired
with the `json` encoder this becomes the `[{…}]` summary the sink stores. prism
does **no SQL**. Per DESIGN.md §8.

## 2. Scope

- **In scope:** `internal/processor/summary`; config `group_by []string`,
  `aggregates []string` (`count`, `avg(value)`, `p95(value)`, …); a small
  aggregate-expression parser for `fn(col)`; vectorized aggregation over Arrow
  columns; deterministic output schema/order.
- **Out of scope:** windowing (buffer spec); encoding (json encoder spec); ml.

## 3. Open questions  (resolved)

- [x] Q: SQL engine? — A: none; declarative aggregates over Arrow columns.
- [x] Q: percentiles? — A: exact per-window (bounded window) unless huge.

## 4. Decision log

- Declarative aggregate spec over Arrow columns (no embedded SQL).
  - ref: https://arrow.apache.org/docs/format/Columnar.html — columnar layout
    makes grouped aggregation cache-friendly.
  - perf: single pass per window over columns; no per-record allocs.
  - product: summaries emitted as JSON; storage/query is a server-side (SQLite)
    concern per the product decision — the agent stays pure-Go and lean.
- Percentiles computed exactly within the bounded window.
  - ref: https://en.wikipedia.org/wiki/Percentile#Nearest-rank_method — defined,
    deterministic method.
  - perf: window is bounded (buffer max_bytes/rows), so exact is affordable.
  - product: exact within window avoids sketch-approximation surprises.

## 5. Acceptance checklist

- [ ] Config `group_by` + `aggregates`; `Validate()` rejects unknown fn / bad
      expr / missing column — path-named.
- [ ] Deterministic aggregates for fixture windows: count/sum/avg/min/max/p95.
- [ ] Grouping over multiple keys; stable output row order (documented).
- [ ] Empty window → empty summary (valid); single-group correctness.
- [ ] Output is a small aggregate `RecordBatch`; input released; allocator balanced.
- [ ] No per-record heap alloc on the aggregation path (benchmark).
- [ ] Tests written first; `make lint test` green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (empty, single group, missing col)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
