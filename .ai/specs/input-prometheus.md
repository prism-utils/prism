# Spec: input/prometheus — scrape

Status: READY

- **Slug / branch:** `feat/input-prometheus`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 3 (pulled from Deferred)

## 1. Task

Add a `prometheus` input that **scrapes** one or more `/metrics` exposition
endpoints on a configurable interval and emits the scraped payloads as
`RawBatch`es (the `prometheus` parser turns them into columnar samples). Pull
model only (no remote_write). Per DESIGN.md §1 (in scope), §7.

## 2. Scope

- **In scope:** `internal/input/prometheus` — HTTP GET each target on `interval`;
  emit bytes + provenance (target, scrape timestamp) as RawBatch; ctx-aware
  ticker; bounded per-scrape body read; config `targets []string`, `interval`,
  `timeout`; `Validate()`.
- **Out of scope:** remote_write receiver; PromQL query API; the parse step
  (see `parser/prometheus` in the parsers spec).

## 3. Open questions  (resolved)

- [x] Q: pull vs push? — A: scrape (pull) `/metrics`.
- [x] Q: parse in input or parser? — A: input fetches bytes; parser decodes.

## 4. Decision log

- Scrape model with net/http + ticker.
  - ref: https://prometheus.io/docs/instrumenting/exposition_formats/ — the
    exposition format prism consumes.
  - perf: bounded body read per scrape; constant memory; interval-driven.
  - product: standard Prometheus scrape semantics; interoperates with any
    exporter.
- Provenance carries target + scrape time for downstream windowing/summary.
  - ref: https://prometheus.io/docs/concepts/data_model/ — samples are
    (labels, value, timestamp).
  - perf: negligible metadata.
  - product: summaries can group by `instance`/target correctly.

## 5. Acceptance checklist

- [ ] Scrapes each target on `interval`; emits a RawBatch per scrape with target
      + timestamp provenance (httptest server fixture).
- [ ] `timeout` bounds each scrape; a failing/500 target is logged + skipped (or
      routed per policy), never crashes the input.
- [ ] ctx-cancel stops the ticker and closes the channel; `goleak` clean.
- [ ] Body read is bounded (configurable max); no unbounded slurp.
- [ ] `Validate()` rejects empty targets, non-positive interval — path-named.
- [ ] Tests written first; `make lint test` green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (target down, timeout, cancel, empty body)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
