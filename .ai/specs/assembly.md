# Spec: assembly — Default() + cmd/prism for multi-pipeline

Status: READY

- **Slug / branch:** `feat/assembly-multipipeline`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 8 (assembly slice)

## 1. Task

Wire the new built-ins into `internal/components.Default()` (inputs: stdin,
file, prometheus; parsers: raw, json, logfmt, regex, prometheus; processors:
template, summary; encoders: raw, parquet, json; outputs: stdout, file) and
update `cmd/prism` so `run` executes a **multi-pipeline** config and `validate`
rejects a broken config with a path-accurate message. Per DESIGN.md §4, §14.

## 2. Scope

- **In scope:** `components.Default()` registration; `cmd/prism` `run`/`validate`/
  `version` reading `pipelines: []`, building via the runtime, running to EOF/
  signal; exit codes; CLI test for `validate`.
- **Out of scope:** new components; container/integration (separate spec).

## 3. Open questions  (resolved)

- [x] Q: cobra now? — A: keep stdlib `flag`; cobra is an optional later swap.
- [x] Q: registration? — A: explicit assembler, no mandatory init().

## 4. Decision log

- Explicit assembler registration (no init magic).
  - ref: https://opentelemetry.io/docs/collector/architecture/ — factories +
    explicit registration.
  - perf: none.
  - product: hermetic tests; the assembler is the single source of truth
    (CONTRIBUTING §5).

## 5. Acceptance checklist

- [ ] `Default()` registers all in-scope built-ins; duplicate/unknown paths error.
- [ ] `prism run -config x.yaml` builds + runs a multi-pipeline config to EOF/
      signal; non-zero exit on failure.
- [ ] `prism validate -config x.yaml` prints a path-accurate error for a broken
      config and exits non-zero (CLI test); valid config → exit 0.
- [ ] `prism version` prints version.
- [ ] Tests written first; `make lint test` green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (bad config, unknown type, missing file)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
