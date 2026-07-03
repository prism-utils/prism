# Spec: <task title>

<!--
  This file IS the loop state (see .ai/workflows/feature-loop.md). Copy it to
  .ai/specs/<kebab-slug>.md, fill it in, and drive it through the loop.
  Keep it concise. Do NOT restate rules — link to the guideline docs.
-->

Status: DRAFT
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `feat/<slug>`
- **Owner phase:** orchestrator
- **PLAN phase(s):** <e.g. Phase 4 — Parsers>  (see docs/PLAN.md)

## 1. Task

One paragraph: what and why. Restated from the user's request.

## 2. Scope

- **In scope:** <files / packages / behavior this change touches>
- **Out of scope:** <explicitly what this change will NOT do>

## 3. Open questions  (must be empty/answered before `Status: READY`)

All human interaction happens here, in Phase 0. List design questions that
affect the solution; do not proceed to `READY` until each is resolved.

- [ ] Q: <question> — A: <answer, or "PENDING — ask user">

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

Every non-trivial decision, with ≥1 researched reference, plus performance and
product-quality rationale.

- <decision>: <choice>
  - ref: <url> — <what it establishes>
  - perf: <cost/benefit vs alternatives>
  - product: <why this is the right call for a shipped product>

## 5. Acceptance checklist  (developer checks these off)

Concrete, verifiable deliverables for this task. Add as many as needed.

- [ ] <deliverable 1>
- [ ] <deliverable 2>
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

<!-- Reviewer appends one actionable line under any gate it unchecks. Set
     Status: ALL_OK only when every box above is checked; otherwise
     Status: CHANGES_REQUESTED. -->

_(empty until first review)_
