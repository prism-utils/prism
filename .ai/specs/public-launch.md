# Spec: Public launch readiness (opening plan)

Status: DRAFT

- **Slug / branch:** `cursor/public-launch-checklist-8a90`
- **Owner phase:** orchestrator
- **PLAN phase(s):** public launch / packaging hygiene

## 1. Task

Lock owner decisions for opening `prism-utils/prism` to the public, record the
BSL 1.1 + CLA compatibility review, publish the executable opening plan
([`docs/PUBLIC_LAUNCH.md`](../../docs/PUBLIC_LAUNCH.md)), and ship a concise
tech README with the stated motivation. Implementation workstreams (LICENSE,
rename, scrub, doc move, CLA, `v1.0.0`, visibility flip) follow once remaining
BSL parameters are answered.

## 2. Scope

- **In scope (this PR slice):** opening plan, README rewrite, decision log,
  BSL compatibility notes, remaining BSL parameter questions.
- **Out of scope until BSL params land:** adding `LICENSE`, module rename,
  CLA bot, history scrub execution, creating `prism-implementation`, visibility
  flip, tagging `v1.0.0`.

## 3. Open questions

### Resolved (2026-08-12)

- [x] Q: MVP finished enough to open? — A: **Yes — public on current ship (option A).**
- [x] Q: License? — A: **BSL 1.1 + CLA on every external PR.**
- [x] Q: Module path? — A: **Rename all `elk-utilities` → `prism-utils`.**
- [x] Q: Auth defaults? — A: **Keep `AUTH_MODE=none`; document do-not-expose (C).**
- [x] Q: First public tag? — A: **`v1.0.0` when exit criteria green (B).**
- [x] Q: Name? — A: **Keep `prism` (A).**
- [x] Q: Upstream vs homelab? — A: **Agent/store/alert/Helm only upstream (A).**
- [x] Q: Homelab docs? — A: **Strip from public; move to private `prism-implementation` (C).**
- [x] Q: Secrets scrub? — A: **Full-history + rotate (A).**
- [x] Q: Contributor surface? — A: **Issue/PR templates + CoC + SECURITY (A).**
- [x] Q: Who flips visibility? — A: **Agent may flip when exit criteria green (B).**

### Still blocking `LICENSE` text

- [ ] Q: Licensor legal name? — A: PENDING
- [ ] Q: Additional Use Grant style (A competing-hosted ban / B numeric cap / C None)? — A: PENDING — recommend **A**
- [ ] Q: Change Date offset (3y / 4y)? — A: PENDING — recommend **4y**
- [ ] Q: Change License (Apache-2.0 / other GPL-compatible)? — A: PENDING — recommend **Apache-2.0**
- [ ] Q: CLA mechanism (CLA assistant bot / other)? — A: PENDING

## 4. Decision log

- BSL 1.1 over Apache-2.0: owner choice for source-available + conversion path.
  - ref: https://mariadb.com/bsl-faq-adopting/ — BSL is not OSI OSS; Change License must be GPL-compatible; production limited unless Additional Use Grant.
  - ref: https://blog.sentry.io/relicensing-sentry/ ; Cockroach BSL — production OK except competing SaaS/DBaaS; Change License Apache-2.0.
  - perf: N/A (legal).
  - product: protects against competing hosted offerings while allowing self-host; must not call the project “open source.”
- CLA on external PRs: required for BSL relicensing control.
  - ref: MariaDB FAQ contribution path (BSD *or* CLA); owner chose CLA.
  - product: clear inbound IP for commercial license + Change License conversion.
- Dependency compatibility: permissive Apache/MIT/BSD deps OK; GPL/AGPL before Change Date not OK.
  - ref: MariaDB FAQ “Is the BSL compatible with GPL (prior to the Change Date)? — No.”
  - product: add pre-`v1.0.0` license report gate.
- Auth default unchanged (`none`): owner acceptance of trusted-network default with docs warnings.
  - product: faster launch; risk owned via CONFIG/NOTES language.
- Homelab docs → private `prism-implementation`: keeps public tree clean of Traefik/site-main/prism-proxy cutover.

## 5. Acceptance checklist

- [x] `docs/PUBLIC_LAUNCH.md` rewritten as opening plan with locked decisions + BSL review + exit criteria
- [x] README rewritten: concise, tech audience, motivation included, benches linked out
- [x] Spec records resolved answers + remaining BSL parameter questions
- [x] Tests / `make lint test` — N/A (docs-only)

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** — N/A docs-only
- [ ] **Gate 3 — Docs match the task**
- [ ] **Gate 4 — Comments atomic** — N/A
- [ ] Full docs/REVIEW.md checklist (docs slice)

## 7. Reviewer notes

_(empty until first review)_
