# Spec: runtime — multi-pipeline, per-input worker, fan-out branches

Status: ALL_OK

- **Slug / branch:** `feat/runtime-multiworker`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 2 (+ Revision 2026-07 topology)

## 1. Task

Rebuild `internal/pipeline` from a single linear chain into a runtime that
builds and runs **one pipeline per input concurrently** (one worker per input),
with **per-stage bounded channels**, a **fan-out** after the buffer into
multiple branches (each branch owns its `RecordBatch` and runs
`[processors]→encoder→output`), a **configurable failure policy** (`drop|block`),
and **per-pipeline isolation** (one input's failure does not stop the others).
Per DESIGN.md §6.

## 2. Scope

- **In scope:** `pipeline.Build([]config.Pipeline, registry, settings)` → set of
  built pipelines; `Run(ctx, host)` runs all under a parent errgroup; per-input
  sub-errgroup; bounded channels between input→parser→pre→buffer→branch stages;
  fan-out dispatch giving each branch an owned batch; failure policy hook;
  graceful drain (flush buffer on EOF/cancel); goleak + backpressure tests.
- **Out of scope:** the buffer's internal windowing logic (separate spec — this
  spec integrates the buffer component/stage seam); concrete components.

## 3. Open questions  (resolved)

- [x] Q: worker model? — A: one errgroup-owned worker per input pipeline.
- [x] Q: fan-out ownership? — A: each branch gets its own Retained batch.
- [x] Q: failure policy set? — A: `drop | block` now; `dead_letter` deferred.
- [x] Q: isolation? — A: a pipeline error is logged + stops that pipeline only.

## 4. Decision log

- Concurrency: parent errgroup with one child group per pipeline.
  - ref: https://pkg.go.dev/golang.org/x/sync/errgroup — owned goroutines, ctx.
  - perf: N inputs → N workers; no shared-queue contention.
  - product: matches OTel Collector's per-pipeline model
    (https://opentelemetry.io/docs/collector/architecture/).
- Backpressure via bounded channels only (no growing queues).
  - ref: https://www.benthos.dev/docs/guides/getting_started — config-first
    streaming with backpressure discipline.
  - perf: bounded memory; slow output slows source.
  - product: predictable steady-state memory (DESIGN §11).

## 5. Acceptance checklist

- [ ] Builds N pipelines from `[]config.Pipeline`; unknown type/opts → path error.
- [ ] Each pipeline runs in its own worker; all under a parent errgroup.
- [ ] Per-stage bounded channels (capacity configurable, sane default).
- [ ] Fan-out: each branch receives an independently-owned batch; both branches
      run concurrently; a slow branch applies backpressure (bounded-channel proof).
- [ ] Failure policy `drop` (count + continue) and `block` (backpressure) both
      covered by tests; malformed data never crashes the run.
- [ ] Per-pipeline isolation: a fatal error in one fake pipeline is logged and
      stops only it; the other keeps running (test).
- [ ] Graceful drain: EOF and ctx-cancel both flush the buffer and drain within
      the shutdown grace; `goleak` asserts no leaked goroutines.
- [ ] Allocator balance asserted across a full run (no buffer leak).
- [ ] Tests written first; `make lint test` (with `-race`) green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (cancel, EOF, slow branch, drop/block)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
