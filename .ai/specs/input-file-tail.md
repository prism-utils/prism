# Spec: input/file — tail mode

Status: READY

- **Slug / branch:** `feat/input-file-tail`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 3

## 1. Task

Add `mode: tail` to `input/file`: follow a file, emit bounded `RawBatch`es as
lines arrive, and survive **rotation** (rename+recreate / truncate) without
dropping or duplicating lines, at constant memory. Uses
`github.com/nxadm/tail`. (`mode: batch` already exists.)

## 2. Scope

- **In scope:** tail follow + rotation handling in `internal/input/file`; config
  `mode: tail`; bounded batching of lines; ctx-cancel stops cleanly + closes the
  channel; constant-memory streaming benchmark.
- **Out of scope:** parsing; batch mode changes beyond shared plumbing.

## 3. Open questions  (resolved)

- [x] Q: tail library? — A: `nxadm/tail` (rotation-aware, pure-Go).
- [x] Q: rotation semantics? — A: no drop/dup across rename+recreate.

## 4. Decision log

- File tailing: **nxadm/tail**.
  - ref: https://github.com/nxadm/tail — maintained fork, rotation/reopen, pure-Go.
  - perf: constant memory; line-by-line; no whole-file read.
  - product: the standard Go tailing lib; battle-tested rotation handling.

## 5. Acceptance checklist

- [ ] `mode: tail` follows appends; lines observed in order as bounded RawBatches.
- [ ] Rotation (rename + recreate) drops/dups zero lines (fixture test).
- [ ] ctx-cancel stops the follower and closes the channel; `goleak` clean.
- [ ] Constant-memory benchmark over a large synthetic append (flat allocs).
- [ ] Bounded batch size honored (config), backpressure via channel.
- [ ] Tests written first; `make lint test` green; benchmark present.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (rotation, empty file, cancel, EOF)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
