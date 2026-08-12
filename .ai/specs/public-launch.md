# Spec: Public launch readiness (checklist + security docs)

Status: DRAFT

- **Slug / branch:** `cursor/public-launch-checklist-8a90` (docs track; follow-up impl branches use `feat/public-launch-*`)
- **Owner phase:** orchestrator
- **PLAN phase(s):** packaging / release hygiene (post store track); not a PLAN.md build phase

## 1. Task

Prepare `prism` to be opened to the public safely: capture a concrete launch
checklist, ship baseline `SECURITY.md` + Code of Conduct, and leave explicit
open questions (license, org/module path, auth default hardening) for owners
before visibility is flipped. This docs PR does **not** flip the repo public
and does **not** add a `LICENSE` until owners confirm SPDX.

## 2. Scope

- **In scope:**
  - `docs/PUBLIC_LAUNCH.md` — ordered hard/soft gates for public beta and `v1.0.0`
  - Root `SECURITY.md` (reporting + threat-model summary)
  - Root `CODE_OF_CONDUCT.md` (Contributor Covenant 2.1)
  - README + `TASKS.md` pointers to the launch track
  - Spec open questions + recommended decisions (Decision Protocol)
- **Out of scope (follow-up PRs after questions resolve):**
  - Adding `LICENSE` / `NOTICE`
  - Changing `AUTH_MODE` default or adding `ALLOW_INSECURE`
  - History scrub / credential rotation execution
  - Issue/PR templates, CODEOWNERS, `govulncheck` CI job
  - Flipping GitHub `visibility: public`
  - Module-path / org rename

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [ ] Q: SPDX license? — A: **PENDING — ask owners.** Recommendation: **Apache-2.0** (OTel Collector / Arrow peer norm; patent grant). Do not add `LICENSE` until confirmed.
- [ ] Q: Finish Go module cutover to `github.com/prism-utils/prism` before public? — A: **PENDING — ask owners.** GitHub already redirects to `prism-utils/prism`; `go.mod` still imports `elk-utilities/prism`. Recommendation: **yes, rename module before first public tag.**
- [ ] Q: Auth default for public binary — fail-closed (`ALLOW_INSECURE`) vs chart-only bearer default? — A: **PENDING — ask owners.** Recommendation: **fail-closed serve** unless `ALLOW_INSECURE=true` (OTel-style secure defaults; see PUBLIC_LAUNCH §2.2 option A).
- [ ] Q: First public tag shape? — A: **Recommended default (owners may override):** `v0.x` / `v1.0.0-beta.N` until auth defaults land; then `v1.0.0`.
- [ ] Q: Keep provisional name `prism` through beta? — A: **Recommended default: yes**; freeze before `v1.0.0`.

## 4. Decision log  (Decision Protocol)

- Launch docs first, code hardening second:
  - ref: https://opentelemetry.io/docs/security/config-best-practices/ — public projects document threat model + secure config before strangers deploy.
  - perf: zero runtime cost for docs; unblocks parallel owner decisions.
  - product: prevents a “public but license-less / auth-none default” footgun.
- Recommend Apache-2.0 (not applied in this PR):
  - ref: https://github.com/open-telemetry/opentelemetry-collector (Apache-2.0) — closest mental-model peer called out in DESIGN.md.
  - perf: N/A (legal).
  - product: easiest downstream adoption in CNCF-adjacent stacks; matches goreleaser `LICENSE*` packaging already configured.
- Recommend fail-closed auth for public serve (follow-up PR):
  - ref: https://opentelemetry.io/blog/2024/hardening-the-collector-one/ — Collector moved defaults toward localhost / safer binds after audit; insecure convenience is opt-in.
  - perf: one startup check; no per-request cost.
  - product: strangers cloning the README cannot accidentally expose unauthenticated ingest/query on `:8080`.
- Threat-model summary lives in `SECURITY.md` (short), detail stays in `STORE.md`:
  - ref: GitHub docs on SECURITY.md + private vulnerability reporting.
  - perf: N/A.
  - product: reporters know the channel; operators know what is guaranteed.

## 5. Acceptance checklist  (developer checks these off)

- [x] `docs/PUBLIC_LAUNCH.md` exists with hard blockers, soft items, and exit criteria
- [x] Root `SECURITY.md` with reporting channel + threat-model summary
- [x] Root `CODE_OF_CONDUCT.md`
- [x] README links the public-launch checklist
- [x] `TASKS.md` lists the public-launch track
- [x] Spec records open questions; no `LICENSE` added without owner confirmation
- [x] Tests written first — N/A (docs-only; no code under test)
- [x] `make lint test` — N/A for docs-only; not required to claim this docs slice done

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md) — docs-only; no guideline violations
- [ ] **Gate 2 — Tests cover edge cases** — N/A (docs-only); reviewer confirms no code change slipped in
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — N/A (no code comments)
- [ ] Full docs/REVIEW.md checklist passes (docs slice)

## 7. Reviewer notes

_(empty until first review)_

## Follow-ups after owners answer §3

1. `feat/public-launch-license` — add `LICENSE` (+ `NOTICE` if needed).
2. `feat/public-launch-auth-defaults` — implement §2.2 fail-closed policy + chart NOTES.
3. `chore/public-launch-templates` — issue/PR templates, optional CODEOWNERS, `govulncheck` CI.
4. `chore/public-launch-scrub` — gitleaks full-history + rotation attestation in the checklist.
5. Owner action: flip GitHub visibility when PUBLIC_LAUNCH exit criteria are green.
