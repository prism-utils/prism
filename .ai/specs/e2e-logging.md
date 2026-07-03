# Spec: e2e — logging path

Status: ALL_OK

- **Slug / branch:** `feat/e2e-logging`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 8

## 1. Task

End-to-end test proving the logging path: `file(tail) → logfmt → template →
buffer → { parquet→file, summary→json→file }`. Fixture log lines flow through a
real pipeline built from a config string; the parquet files read back with the
expected rows and the summary JSON contains the expected grouped aggregates.

## 2. Scope

- **In scope:** an e2e test (Go, in-repo; temp dirs) driving the real assembled
  runtime from a YAML config; assertions on both sinks; a sample config committed
  under `deploy/`/`examples/`.
- **Out of scope:** docker-compose (separate integration spec); metrics path.

## 3. Open questions  (resolved)

- [x] Q: output sink? — A: file (parquet dir + summary json dir).
- [x] Q: docker needed? — A: no; file-based, hermetic temp dirs.

## 4. Decision log

- File-based hermetic e2e (temp dirs), no external services.
  - ref: https://pkg.go.dev/testing — standard Go e2e in-process.
  - perf: fast, deterministic.
  - product: proves the real wiring without infra flakiness.

## 5. Acceptance checklist

- [ ] Sample logging config committed and referenced by the test.
- [ ] Fixture lines → tail input → parsed → templated → windowed → fan-out.
- [ ] Parquet files land, read back (arrow reader) with expected rows/columns
      including the `template` column.
- [ ] Summary JSON files contain expected grouped aggregates (count per template/
      level/service).
- [ ] Clean shutdown flushes the partial window; no goroutine/buffer leak.
- [ ] Tests written first; `make lint test` green; runs under `full-tests`.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (empty input, shutdown mid-window)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
