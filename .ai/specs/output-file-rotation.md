# Spec: output/file — rotation + atomic rename

Status: ALL_OK

- **Slug / branch:** `feat/output-file-rotation`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 5

## 1. Task

Extend `output/file` from append-only to a rotating writer: write each
`EncodedBlock` (a complete Parquet file or a JSON blob) to a directory with
**size/time rotation** and **atomic rename** so no partial file is ever visible
to a reader. Per DESIGN.md §9.

## 2. Scope

- **In scope:** `internal/output/file` rotation policy (`max_bytes`,
  `max_age`/interval), write-to-temp + `os.Rename` atomic publish, filename
  scheme (timestamp/sequence + extension from block format), directory create;
  `Validate()`.
- **Out of scope:** http output; encoders.

## 3. Open questions  (resolved)

- [x] Q: how atomic? — A: write temp in same dir, fsync, `os.Rename`.
- [x] Q: one block per file? — A: each self-contained block → its own file.

## 4. Decision log

- Atomic publish via temp file + rename in the same directory.
  - ref: https://pkg.go.dev/os#Rename — rename is atomic within a filesystem.
  - perf: one extra rename per file; negligible.
  - product: readers/tailer sinks never see a half-written Parquet file.
- One EncodedBlock → one file (Parquet blocks are self-contained).
  - ref: https://parquet.apache.org/docs/file-format/ — a Parquet file is a
    complete, self-describing unit.
  - perf: no cross-file state; simple rotation.
  - product: each file is independently readable downstream.

## 5. Acceptance checklist

- [ ] Rotation by size and by time/interval (config); `Validate()` path-named.
- [ ] Atomic rename: a concurrent reader never observes a partial file (test via
      temp-then-rename; assert no `*.tmp` visible as final).
- [ ] Filename carries a monotonic/timestamped component + correct extension per
      block format (`.parquet`, `.json`).
- [ ] Directory auto-created; permission errors returned (not panicked).
- [ ] Shutdown flushes/closes the current file cleanly.
- [ ] Tests written first; `make lint test` green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (rotation, partial-file, perms, shutdown)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
