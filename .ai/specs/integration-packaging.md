# Spec: integration & packaging — full-tests + non-root container

Status: READY

- **Slug / branch:** `feat/integration-packaging`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 8

## 1. Task

Make `make full-tests` green in CI (the integration/e2e layer), and prove the
container runs **non-root** and processes a sample end-to-end. Ensure `validate`
catches bad configs at the CLI level. Per DESIGN.md §12, PLAN Phase 8.

## 2. Scope

- **In scope:** the `full-tests` target running the two e2e specs (+ any compose
  bits actually needed); Dockerfile check (non-root, static, CGO_ENABLED=0);
  a container smoke test running a sample config against file sinks in a temp
  volume; CI wiring for the full gate.
- **Out of scope:** new components; http/MinIO sink (file sink is the target).

## 3. Open questions  (resolved)

- [x] Q: compose or in-process? — A: prefer in-process/file e2e; add compose only
      if a component genuinely needs an external service (none in this cut).
- [x] Q: container user? — A: non-root, read-only-rootfs friendly.

## 4. Decision log

- Keep integration hermetic (file sinks) — compose only if unavoidable.
  - ref: https://docs.docker.com/build/building/multi-stage/ — static non-root
    final image.
  - perf: fast CI; no service startup flake.
  - product: reproducible; the agent's real target is file/objects at the edge.

## 5. Acceptance checklist

- [ ] `make full-tests` runs both e2e paths and passes locally + in CI.
- [ ] Container builds `CGO_ENABLED=0`, runs as non-root, processes a sample
      config to file sinks (smoke test).
- [ ] `validate` CLI test: broken config → path-accurate error + non-zero exit.
- [ ] CI full gate wired; `make tidy` clean; cross-compile unaffected.
- [ ] Tests written first where applicable; `make lint test` green.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases**
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
