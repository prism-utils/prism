# Spec: config — YAML + ${ENV} + multi-pipeline schema

Status: READY

- **Slug / branch:** `feat/config-multipipeline`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 1 (finish) + Revision 2026-07 (topology)

## 1. Task

Reshape `internal/config` from a single linear pipeline into a **list of
pipelines**, each with `input`, `parser`, optional pre-buffer `processors`, a
`buffer`, and one or more `branches` (each branch: optional `processors`,
`encoder`, `output`) — per DESIGN.md §7. Add YAML loading and `${ENV}`
interpolation via koanf so one struct serves YAML and JSON. Validation stays
total and path-accurate.

## 2. Scope

- **In scope:** `internal/config` types (`Config{ Pipelines []Pipeline }`,
  `Pipeline`, `Buffer`, `Branch`, reuse `Stage`); YAML+JSON+`${ENV}` loader via
  `github.com/knadh/koanf/v2`; `Validate()` for the new tree with path-named
  errors (`pipelines[0].branches[1].encoder.type`).
- **Out of scope:** building/wiring components (pipeline package), the buffer
  runtime, any component options schemas (factories own those).

## 3. Open questions  (resolved)

- [x] Q: One struct for YAML+JSON? — A: Yes, koanf yaml→json→struct; DESIGN §7.
- [x] Q: Env interpolation syntax? — A: `${VAR}` in any string value.
- [x] Q: Multi-pipeline shape? — A: `pipelines: []` with fan-out `branches`.

## 4. Decision log

- Loader library: **koanf/v2** (yaml + env providers, json struct tags).
  - ref: https://github.com/knadh/koanf — layered providers, pure-Go, one struct.
  - perf: load-time only; negligible; no hot-path cost.
  - product: single source of truth for both encodings; no divergent code paths.
- `${ENV}` interpolation at load: expand only in string leaves after decode.
  - ref: https://pkg.go.dev/os#Expand — standard, predictable expansion.
  - perf: one pass at load; none at runtime.
  - product: secrets stay out of committed config (DESIGN §12).

## 5. Acceptance checklist

- [ ] `Config` = `{ Pipelines []Pipeline }`; `Pipeline` = name + input + parser +
      pre `[]Stage` + `Buffer` + `[]Branch`; `Branch` = name + `[]Stage` +
      encoder + output. All `json` tags.
- [ ] `Load(io.Reader, format)` (or `LoadYAML`/`LoadJSON`) via koanf; YAML result
      == equivalent JSON result (table test).
- [ ] `${ENV}` interpolation in string values; missing var → path-named error or
      documented empty per decision.
- [ ] `Validate()` total: empty `pipelines`, duplicate pipeline names, empty
      `input.type`, zero `branches`, empty branch `encoder`/`output`, bad
      `buffer` bounds → each returns a path-accurate error.
- [ ] Unknown top-level/stage keys rejected (`DisallowUnknownFields` equivalent).
- [ ] `Buffer` fields: `max_age`, `max_rows`, `max_bytes` with defaults 30s/0/12MiB
      surfaced (defaults may live where the buffer factory reads them; config
      validates ranges).
- [ ] Tests written first (`test:` commit precedes implementation).
- [ ] `make lint test` green locally.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases**
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
