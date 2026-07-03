# Spec: encoders — parquet + json

Status: ALL_OK

- **Slug / branch:** `feat/encoders`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 5

## 1. Task

Implement `encoder/parquet` (Arrow→Parquet via `apache/arrow-go`, configurable
compression + row-group sizing) and `encoder/json` (serialize a `RecordBatch`
as a JSON array `[{col: val, …}, …]`, one object per row) for the summary
branch. Encoders own their buffering and release the batch's buffers. Per
DESIGN.md §9.

## 2. Scope

- **In scope:** `internal/encoder/parquet` + `internal/encoder/json`; parquet
  round-trip test (encode → read back with arrow reader → assert schema/values/
  compression); json shape test; buffer release assertions.
- **Out of scope:** outputs; summary computation.

## 3. Open questions  (resolved)

- [x] Q: parquet writer? — A: arrow-go's pqarrow.
- [x] Q: json shape? — A: array of row objects `[{…}]`.

## 4. Decision log

- Parquet via arrow-go `pqarrow`.
  - ref: https://pkg.go.dev/github.com/apache/arrow-go/v18/parquet/pqarrow —
    native Arrow→Parquet, pure-Go, compression (zstd/snappy).
  - perf: columnar → Parquet is near free; row-group sizing configurable.
  - product: Parquet is the standard columnar file for downstream analytics.
- JSON summary as `[{…}]`.
  - ref: https://pkg.go.dev/encoding/json — stdlib, deterministic.
  - perf: summaries are small (aggregated); cost negligible.
  - product: trivially ingestible + storable in SQLite server-side.

## 5. Acceptance checklist

- [ ] `encoder/parquet`: encode a known batch → read back → schema + values +
      compression asserted; buffers released (allocator balanced).
- [ ] Config: compression (`zstd` default?) + `row_group_rows`; `Validate()`.
- [ ] `encoder/json`: batch → `[{…}]`; types mapped correctly; empty batch → `[]`.
- [ ] `EncodedBlock` metadata (format, rows, size) populated.
- [ ] Tests written first; `make lint test` green (+ `full-tests` if wired).

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (empty batch, round-trip, release)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
