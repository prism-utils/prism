# Spec: e2e — metrics path

Status: READY

- **Slug / branch:** `feat/e2e-metrics`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 8

## 1. Task

End-to-end test proving the metrics path: `prometheus(scrape) → buffer →
{ parquet→file, summary→json→file }`. A fake `/metrics` httptest server serves
exposition text; prism scrapes it, windows the samples, and writes both a
parquet file (reads back with expected samples) and a summary JSON (expected
grouped aggregates, e.g. count/avg per `__name__`/`instance`).

## 2. Scope

- **In scope:** e2e test driving the real assembled runtime from a YAML config
  against an httptest `/metrics` server; assertions on both sinks; a sample
  metrics config committed.
- **Out of scope:** docker-compose; logging path.

## 3. Open questions  (resolved)

- [x] Q: scrape target in test? — A: httptest server serving exposition text.
- [x] Q: output sink? — A: file (parquet + summary json).

## 4. Decision log

- httptest exposition server as the scrape target.
  - ref: https://pkg.go.dev/net/http/httptest — hermetic HTTP server for tests.
  - perf: in-process, fast.
  - product: exercises the real scrape+parse without a live exporter.

## 5. Acceptance checklist

- [ ] Sample metrics config committed and referenced by the test.
- [ ] httptest server serves counter/gauge exposition; prism scrapes on interval.
- [ ] Parquet file lands and reads back with expected sample rows/columns.
- [ ] Summary JSON contains expected grouped aggregates over the window.
- [ ] ctx-cancel / short run flushes the partial window; no leak.
- [ ] Tests written first; `make lint test` green; runs under `full-tests`.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (target down mid-run, empty scrape)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
