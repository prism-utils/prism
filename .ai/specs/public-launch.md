# Spec: Public launch readiness (opening plan)

Status: READY

- **Slug / branch:** `cursor/public-launch-checklist-8a90` (plan docs); execute on `feat/public-launch-execute` (or `cursor/public-launch-execute-8a90`)
- **Owner phase:** orchestrator → developer (execution)
- **PLAN phase(s):** public launch runbook

## 1. Task

Produce an agent-executable public-opening runbook with every owner decision
locked (including BSL parameters and maintainer-only CI), so a follow-on agent
can implement LICENSE → CLA → CI gate → rename → doc move → scrub → `v1.0.0` →
visibility flip without further questions.

Canonical runbook: [`docs/PUBLIC_LAUNCH.md`](../../docs/PUBLIC_LAUNCH.md).

## 2. Scope

- **In scope (this docs PR):** complete runbook + README + SECURITY/CoC already
  present; all decisions recorded.
- **Out of scope here (next execution PR):** creating `LICENSE`, CLA bot wiring,
  `ci.yml` authorize gate, rename, `prism-implementation` move, scrub, tag, flip.

## 3. Open questions

All resolved — see runbook §0.

### BSL / ops (2026-08-12)

- [x] Licensor — **Sys Ramos IT LLC**
- [x] Additional Use Grant — **A** (production OK except Competing Service)
- [x] Change Date — **4 years**
- [x] Change License — **Apache-2.0**
- [x] CLA — **GitHub CLA Assistant / cla-bot**
- [x] CI — **only maintainers run CI on PRs** (fork approval + `ci:run` label gate)

## 4. Decision log

- Maintainer-only CI: combine GitHub “require approval for all outside
  collaborators” with workflow authorize job + `ci:run` label; never build via
  `pull_request_target`.
  - ref: GitHub Actions docs on fork PR approval; OTel/collector hardening
    guidance against running untrusted workflows with secrets.
  - product: strangers cannot burn CI or exfil via PR code until a maintainer
    labels `ci:run`.
- BSL Competing Service grant (Cockroach/Sentry-style) with Apache-2.0 Change
  License and 4-year Change Date; licensor Sys Ramos IT LLC.
  - ref: https://mariadb.com/bsl11/ ; Sentry/Cockroach BSL adoptions.
  - product: self-host production OK; competing hosted telemetry store not OK
    without commercial license.

## 5. Acceptance checklist

- [x] `docs/PUBLIC_LAUNCH.md` is a full phased runbook with checkboxes + exact
      LICENSE parameters + CI gate recipe + exit criteria + attestations table
- [x] All L1–L5 and D1–D12 decisions locked in the runbook
- [x] README motivation + source-available framing present
- [x] Spec `Status: READY` for execution handoff
- [x] Tests N/A (docs-only)

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests** — N/A docs-only
- [ ] **Gate 3 — Docs match the task**
- [ ] **Gate 4 — Comments atomic** — N/A
- [ ] Full docs/REVIEW.md checklist (docs slice)

## 7. Reviewer notes

_(empty until first review)_

## Execution handoff

Next agent: open `feat/public-launch-execute` from `main`, follow
`docs/PUBLIC_LAUNCH.md` Phases 1→11 in order, check boxes + fill Attestations,
merge, tag `v1.0.0`, flip visibility when Exit criteria are green.
