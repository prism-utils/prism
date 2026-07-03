# Spec: buffer — accumulation / windowing

Status: ALL_OK

- **Slug / branch:** `feat/buffer-window`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Revision 2026-07 (§6.1)

## 1. Task

Implement the accumulation buffer that sits between the parser/pre-processors
and the fan-out. It accumulates parsed records and **flushes a bounded
`RecordBatch` on the first of**: `max_age` (default `30s`), `max_rows`
(default `0` = no cap), `max_bytes` (default `12MiB`). It flushes any partial
window on EOF/shutdown. This is the component that guarantees flat steady-state
memory for windowed pipelines (DESIGN.md §6.1, §11).

## 2. Scope

- **In scope:** `internal/buffer` windowing accumulator with its typed config +
  `Validate()` + defaults; a timer that respects ctx; concatenation of buffered
  rows into one Arrow `RecordBatch`; deterministic, testable flush triggers.
- **Out of scope:** the fan-out dispatch (runtime spec); summary aggregation.

## 3. Open questions  (resolved)

- [x] Q: flush semantics? — A: first bound hit wins (age|rows|bytes).
- [x] Q: defaults? — A: 30s / 0 / 12MiB.
- [x] Q: EOF behavior? — A: flush partial window, never drop.

## 4. Decision log

- Time+size+count triggered micro-batching.
  - ref: https://opentelemetry.io/docs/collector/configuration/ (batch
    processor: `timeout` + `send_batch_size`) — proven edge pattern.
  - perf: bounded memory (max_bytes hard cap); amortizes per-window processing.
  - product: standard collector batching; predictable latency vs. throughput.
- `max_bytes` measured on accumulated Arrow buffer size (not raw bytes).
  - ref: https://pkg.go.dev/github.com/apache/arrow-go/v18/arrow/memory — buffer
    sizes are queryable.
  - perf: the real memory ceiling, so the cap means what it says.
  - product: "agent mem queue" cap is honest.

## 5. Acceptance checklist

- [ ] Config `max_age`/`max_rows`/`max_bytes` with defaults 30s/0/12MiB; `Validate`
      rejects negative/all-zero (must have at least one active bound).
- [ ] Flush on age: fake clock / injected timer → window flushes at max_age.
- [ ] Flush on rows: N+1th row triggers a flush of N (or N+1) deterministically.
- [ ] Flush on bytes: exceeding max_bytes triggers a flush.
- [ ] Partial window flushed on ctx-cancel and on upstream close (EOF); no loss.
- [ ] No `time.Sleep` in tests (injected clock/ticker); deterministic.
- [ ] Emitted batches are bounded and allocator-balanced.
- [ ] Tests written first; `make lint test` (with `-race`) green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (each trigger, partial flush, empty)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
