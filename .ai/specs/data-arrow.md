# Spec: data — Arrow-backed RecordBatch

Status: ALL_OK

- **Slug / branch:** `feat/data-arrow`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 2

## 1. Task

Replace the interim row-oriented `RecordBatch` (`[][]byte`) with an Apache
Arrow-backed columnar representation (schema + column arrays + poolable
buffers) per DESIGN.md §5, **without changing the `component` interfaces**.
Establish linear buffer ownership and an allocator-balance test helper so leaks
fail CI.

## 2. Scope

- **In scope:** `internal/data` — `RecordBatch` wrapping an `arrow.Record` (+
  schema, provenance); constructors (bounded), `Release()` returns buffers to a
  `memory.Allocator`; a retain/slice helper for fan-out; test helper asserting
  allocator balance (allocated == released). Add `apache/arrow-go/v18`.
- **Out of scope:** parsers producing Arrow (later specs); encoders; pipeline.

## 3. Open questions  (resolved)

- [x] Q: Which Arrow module? — A: `github.com/apache/arrow-go/v18` (pure-Go).
- [x] Q: Ownership model? — A: linear; receiver releases; fan-out uses Retain.

## 4. Decision log

- Columnar model: **apache/arrow-go/v18**.
  - ref: https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow — native
    Arrow↔Parquet, poolable buffers, pure-Go (preserves CGO_ENABLED=0).
  - perf: contiguous columns, reusable allocator buffers; vectorized ops; near
    free Parquet encoding.
  - product: industry-standard columnar interchange; future-proofs encoders.
- Allocator: expose a checked allocator in tests (`memory.NewCheckedAllocator`).
  - ref: https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow/memory —
    CheckedAllocator asserts balance.
  - perf: test-only.
  - product: enforces DESIGN §5/§11 "leak == fail".

## 5. Acceptance checklist

- [ ] `RecordBatch` carries an Arrow schema + columnar arrays + `Source`
      provenance; `Len()` = row count; bounded constructor(s).
- [ ] `Release()` is idempotent and returns all buffers to the allocator.
- [ ] Fan-out helper produces independently-owned views (Retain/slice) so two
      branches can each Release without double-free.
- [ ] `Host` gains a buffer `Allocator()` capability (or Settings) so components
      get the allocator without globals.
- [ ] Test helper asserts allocator balance; a deliberate leak test fails.
- [ ] `EncodedBlock` unchanged or minimally extended (schema fingerprint ok).
- [ ] Tests written first; `make lint test` (with `-race`) green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (empty batch, double-Release, leak)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
