# Spec: Patch trixie-slim release images for CVE-2026-53615 (util-linux / libblkid1)

<!--
  This file IS the loop state (see .ai/workflows/feature-loop.md).
-->

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/trivy-util-linux-baa6`
- **Owner phase:** reviewer
- **PLAN phase(s):** Phase 0 — Foundation & tooling (release images / supply-chain gate)

## 1. Task

`v1.0.7` (MEMORY_OBSERVE, SHA `a56d473`) never published an image. Release run
https://github.com/prism-utils/prism/actions/runs/31984034992 failed the **agent**
Trivy HIGH gate on **CVE-2026-53615** (`util-linux` / `libblkid1` installed
`2.41-5`, fixed `2.41.5-0+deb13u1` in **trixie-security**). The store image was
never scanned or pushed. Unrelated to MEMORY_OBSERVE. Patch the trixie-slim
release Dockerfiles so `apt` pulls the security-suite libblkid, keep the Trivy
gate honest (`ignore-unfixed: true`), squash-merge to main, and ship **v1.0.8**
(do **not** move `v1.0.7`). Homelab pin is out of scope.

## 2. Scope

- **In scope:**
  - `Dockerfile.release` (agent) and `Dockerfile.store.release` (store) — the
    **trixie-slim** images Trivy scans in `.github/workflows/release.yml`.
  - After `apt-get update`: ensure `debian-security` / `trixie-security` is in
    apt sources if the slim image omits it; `apt-get install -y --no-install-recommends libblkid1`
    so the security suite can pull `>= 2.41.5-0+deb13u1`; `apt-get upgrade -y`
    for other HIGH/CRITICAL OS CVEs that would fail `ignore-unfixed: true`;
    keep existing `ca-certificates` + `libstdc++6`; `rm -rf /var/lib/apt/lists/*`.
  - One-line atomic Dockerfile comments explaining why the security upgrade is
    required (CONTRIBUTING.md §3.8).
  - A unit test that reads those two Dockerfiles and asserts they keep the
    patched libblkid/util-linux upgrade (install of `libblkid1` and
    `2.41.5-0+deb13u1` / `trixie-security`). Test-first commit.
  - `make lint test` green.
- **Out of scope:**
  - `Dockerfile.alert.release` (distroless static — no util-linux).
  - `Dockerfile.agent.cgo` and `Dockerfile.store.build` — they use
    `debian:bookworm-slim`, not trixie, and are **not** scanned by `release.yml`.
    Bookworm still has no fixed util-linux for this CVE.
  - Moving tag `v1.0.7`. Homelab image pin. Workflow / Trivy ignore changes.
  - Tagging `v1.0.8` (orchestrator, after squash-merge).

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Which files? — A: Every **trixie-slim** image Trivy scans in
      `release.yml`: `Dockerfile.release` (agent) and `Dockerfile.store.release`
      (store). Also `Dockerfile.agent.cgo` and `Dockerfile.store.build` if they
      use the same base (they do **not** — bookworm-slim; leave them). **Not**
      `Dockerfile.alert.release`.
- [x] Q: How to patch? — A: After `apt-get update`, install/upgrade `libblkid1`
      (and `util-linux` if present) from default Debian + **trixie-security**.
      Prefer an explicit `apt-get install -y --no-install-recommends libblkid1`
      so the security suite can pull `>= 2.41.5-0+deb13u1`, plus
      `apt-get upgrade -y` for other HIGH/CRITICAL OS CVEs. Ensure
      `debian-security` / `trixie-security` is in apt sources if the slim image
      omits it. Then `rm -rf /var/lib/apt/lists/*`.
- [x] Q: Tests? — A: TDD: first commit a test that reads the release Dockerfiles
      and asserts they pull the patched util-linux/libblkid (install/upgrade of
      `libblkid1` and/or `2.41.5-0+deb13u1` / `trixie-security`). `make lint test`
      is enough; e2e not required. Docker build in CI is the real Trivy gate.
- [x] Q: Version? — A: After squash-merge, annotated tag `v1.0.8` on
      origin/main and push. Watch release.yml until
      `ghcr.io/prism-utils/prism-store:1.0.8` exists. Homelab pin is out of scope.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Upgrade libblkid from trixie-security rather than ignore CVE in Trivy:
  - ref: https://security-tracker.debian.org/tracker/CVE-2026-53615 — trixie
    (security) fixed in `2.41.5-0+deb13u1`; unfixed on bookworm.
  - perf: no runtime cost; slightly newer util-linux in a slim image.
  - product: release gate stays honest (`ignore-unfixed: true` still fails
    unfixed HIGH).
- New tag 1.0.8 instead of moving 1.0.7:
  - ref: https://semver.org/#spec-item-6 (patch for a backwards-compatible bugfix)
  - perf: n/a
  - product: 1.0.7 remains the MEMORY_OBSERVE source SHA; 1.0.8 is the first
    *published* image that includes it plus the base fix.
- Guard trixie-security in apt sources rather than assuming the slim image
  always ships it:
  - ref: https://github.com/debuerreotype/docker-debian-artifacts (official
    images usually include `debian-security`, but omission would silently
    leave libblkid at `2.41-5`).
  - perf: one `grep` at image build; no runtime cost.
  - product: the Dockerfile itself ensures the suite that carries the fix.

## 5. Acceptance checklist  (developer checks these off)

- [x] Agent + store release Dockerfiles install patched libblkid/util-linux from trixie-security
- [x] Test asserts those Dockerfiles keep the upgrade (test-first commit)
- [x] Docs: one-line note in Dockerfile comments why security upgrade is required (atomic comments)
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched — N/A: Dockerfile contract only)

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
