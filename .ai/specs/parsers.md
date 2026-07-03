# Spec: parsers — json / logfmt / regex / prometheus + auto-discovery

Status: READY

- **Slug / branch:** `feat/parsers`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 4

## 1. Task

Implement the parsers that turn `RawBatch` bytes into Arrow `RecordBatch`es:
`parser/json`, `parser/logfmt`, `parser/regex` (for logs) and
`parser/prometheus` (exposition text → sample columns). Add schema
**auto-discovery** (infer columns/types from first N records, evolve safely with
deterministic type precedence). Fuzz the log parsers: never panic; malformed
input → routed error. Per DESIGN.md §4, §5.

## 2. Scope

- **In scope:** the four parser packages; row→Arrow conversion; auto-discovery
  with documented type precedence; per-format golden fixtures; fuzz targets.
- **Out of scope:** processors; encoders. (May be split into per-parser PRs if
  the reviewer prefers; kept as one spec for the phase.)

## 3. Open questions  (resolved)

- [x] Q: prometheus parse lib? — A: `prometheus/common/expfmt`.
- [x] Q: type conflict precedence? — A: documented widening (int→float→string).

## 4. Decision log

- Prometheus decode via **prometheus/common/expfmt**.
  - ref: https://pkg.go.dev/github.com/prometheus/common/expfmt — canonical
    exposition parser, pure-Go.
  - perf: streaming decode; no bespoke text parser to maintain.
  - product: correct for all metric types (counter/gauge/histogram/summary).
- Auto-discovery: infer from first N, evolve; conflicts widen to a common type.
  - ref: https://arrow.apache.org/docs/format/Columnar.html — schema/typing model.
  - perf: bounded sample window; no reparse.
  - product: schema evolution without dropping late fields (DESIGN §5).

## 5. Acceptance checklist

- [ ] `parser/json`: golden raw→Arrow (schema + values), nested handled/documented.
- [ ] `parser/logfmt`: golden key=val → columns.
- [ ] `parser/regex`: named-group config → columns; bad pattern → Validate error.
- [ ] `parser/prometheus`: exposition text → columns (`__name__`, labels, value,
      timestamp) via expfmt; counter/gauge/histogram covered.
- [ ] Auto-discovery: mixed/late fields evolve schema without data loss; type
      conflict resolves per documented precedence (test).
- [ ] Fuzz `json`/`logfmt`/`regex`: never panic; malformed → error (routed).
- [ ] Allocator-balanced output batches.
- [ ] Tests written first; `make lint test` green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (empty, malformed, mixed schema, fuzz)
- [ ] **Gate 3 — Docs & comments match** (type precedence documented)
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
