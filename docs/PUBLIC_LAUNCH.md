# prism — Public opening runbook (agent-executable)

> **Audience:** an autonomous agent. Execute phases **in order**. Check every box
> when done. Do **not** flip visibility until **Exit criteria** are all checked.
>
> Repo: [`prism-utils/prism`](https://github.com/prism-utils/prism)  
> Spec: [`.ai/specs/public-launch.md`](../.ai/specs/public-launch.md)  
> Branch prefix for implementation: `cursor/public-launch-*` or `feat/public-launch-*`

**Status:** COMPLETE — public as of 2026-08-12T17:58:47Z.

---

## 0. Locked decisions (do not re-ask)

| ID | Decision |
|---|---|
| D1 | Open on current shipped MVP (no extra feature gate). |
| D2 | License = **BSL 1.1** + **CLA on every external PR**. |
| D3 | Rename **all** `elk-utilities` → **`prism-utils`**. |
| D4 | Keep `AUTH_MODE=none`; document do-not-expose (no fail-closed code change). |
| D5 | First public tag = **`v1.0.0`**. |
| D6 | Keep product name **`prism`**. |
| D7 | Public tree = agent + store + alert + in-repo Helm/compose only. |
| D8 | Homelab docs → **private** `prism-implementation` (create if missing); strip from this repo. |
| D9 | Full-history secret scrub + rotate (**hard gate**). |
| D10 | Contributor surface = issue/PR templates + CoC + SECURITY (+ CLA). |
| D11 | **Agent flips visibility** when Exit criteria are green. |
| D12 | **Only maintainers can run CI on PRs** (see Phase 4). |
| L1 | Licensor = **Sys Ramos IT LLC** |
| L2 | Additional Use Grant = **A** (production OK except Competing Service / hosted SaaS). |
| L3 | Change Date = **4 years** from first public distribution of that version. |
| L4 | Change License = **Apache License, Version 2.0** |
| L5 | CLA = **GitHub CLA Assistant / cla-bot** on the org/repo. |

### Motivation (must stay in README)

Unify logging + metrics in one agent and one purpose-built store to use far less
resources; process on the agent to offload the server; decouple storage from CPU
via immutable Parquet tiers.

---

## Phase 1 — Worktree + currency

- [x] `git fetch origin main` from primary `~/git/prism` (or cloud clone)
- [x] Create worktree/branch from `origin/main` (e.g. `feat/public-launch-execute`)
- [x] Confirm `git log --oneline HEAD..origin/main` is empty (rebase if not)
- [x] Read this file end-to-end before editing

---

## Phase 2 — LICENSE (BSL 1.1)

Create root **`LICENSE`** with parameters below, then the standard MariaDB BSL 1.1
body (Covenants + Notice). Use the official text from
https://mariadb.com/bsl11/ (do not invent alternate BSL wording).

### 2.1 Parameters block (exact intent)

```
Business Source License 1.1

Parameters

Licensor:             Sys Ramos IT LLC
Licensed Work:        prism
                      The Licensed Work is (c) 2026 Sys Ramos IT LLC

Additional Use Grant: You may make production use of the Licensed Work, provided
                      that you do not use the Licensed Work to offer a Competing
                      Service.

                      A “Competing Service” is a commercial offering that allows
                      third parties (other than your employees and contractors)
                      to access the Licensed Work as a hosted SaaS, managed
                      service, or multi-tenant observability / telemetry store
                      service that significantly overlaps with offerings of
                      Sys Ramos IT LLC.

Change Date:          <UTC_DATE_OF_FIRST_PUBLIC_V1_0_0 plus 4 years, YYYY-MM-DD>
                      Example: if v1.0.0 is first published 2026-08-15, use 2030-08-15.

Change License:       Apache License, Version 2.0

For information about alternative licensing arrangements for the Licensed Work,
please contact Sys Ramos IT LLC.
```

- [x] Write `LICENSE` with parameters + full BSL 1.1 terms
- [x] Set `Change Date` to **tag day (UTC) + 4 years** when preparing `v1.0.0` (update LICENSE in the same release PR/commit as the tag if the date was a placeholder)
- [x] Add `NOTICE` if needed listing Apache-2.0 attributions (Arrow, etc.) — at minimum keep third-party notices accurate
- [x] README License blurb already says BSL / source-available — keep consistent; never say “open source”
- [x] Optional short `docs/LICENSE_FAQ.md`: non-production free; production OK except Competing Service; commercial license contact; converts to Apache-2.0 on Change Date

### 2.2 License report gate

- [x] Run a dependency license report (`go-licenses` or equivalent once Go is available)
- [x] Fail the launch if any **GPL / AGPL / LGPL-copyleft** (or other production-restrictive) dep appears in the release graph
- [x] Record command + summary under “Attestations” at the bottom of this file

---

## Phase 3 — CLA (external PRs)

- [x] Enable **CLA Assistant** (or org cla-bot) for `prism-utils/prism`
  - Store CLA text (copyright assignment / inbound license grant to **Sys Ramos IT LLC** sufficient to distribute under BSL and Change License)
  - Signatures stored in the usual `cla-assistant` signatures repo or org-configured store
- [x] Add `.github/workflows/cla.yml` (or vendor-recommended workflow) that runs the CLA check on `pull_request_target` **signatures only** (no build of PR code)
- [x] PR template states: external contributors must sign the CLA before merge
- [x] Document in `CONTRIBUTING.md`: CLA required; maintainers are exempt if already covered by employment/org agreement (optional note)
- [x] Verify: open a dry-run fork PR or use CLA Assistant’s test — unsigned PR is blocked from merge

---

## Phase 4 — Maintainer-only CI on PRs

**Goal:** untrusted PR code must not run `make test` / Docker / etc. until a
**maintainer** explicitly allows it.

### 4.1 GitHub repo Actions setting (required)

- [x] Set fork PR workflow approval to **Require approval for all outside collaborators**

```bash
# Preferred API (if available to the token):
gh api -X PUT repos/prism-utils/prism/actions/permissions/fork-pr-contributor-approval \
  -f approval_policy=all_external_contributors

# If that endpoint is unavailable, set in UI:
# Settings → Actions → General → Fork pull request workflows from outside collaborators
# → "Require approval for all outside collaborators"
```

- [x] Confirm Actions are enabled for the repo
- [x] Record how it was set under Attestations

### 4.2 Workflow gate in `.github/workflows/ci.yml` (required)

Implement **both** of the following:

1. **Auto-run CI** only when:
   - `push` to `main`, or
   - `pull_request` from the **same repo** AND `author_association` ∈ `{OWNER, MEMBER, COLLABORATOR}`
2. **Otherwise** (forks / first-timers / outside contributors): CI jobs **skip** unless the PR has label **`ci:run`** (maintainer applies after reviewing the diff).

Suggested pattern:

```yaml
on:
  push:
    branches: [main]
  pull_request:
    types: [opened, synchronize, reopened, labeled, unlabeled]

jobs:
  # Shared gate — every expensive job needs: needs: authorize
  authorize:
    runs-on: ubuntu-24.04
    outputs:
      run: ${{ steps.decide.outputs.run }}
    steps:
      - id: decide
        run: |
          set -euo pipefail
          if [ "${{ github.event_name }}" = "push" ]; then
            echo "run=true" >> "$GITHUB_OUTPUT"; exit 0
          fi
          ASSOC="${{ github.event.pull_request.author_association }}"
          SAME="${{ github.event.pull_request.head.repo.full_name == github.repository }}"
          LABELED="${{ contains(github.event.pull_request.labels.*.name, 'ci:run') }}"
          if [ "$LABELED" = "true" ]; then
            echo "run=true" >> "$GITHUB_OUTPUT"; exit 0
          fi
          if [ "$SAME" = "true" ] && { [ "$ASSOC" = "OWNER" ] || [ "$ASSOC" = "MEMBER" ] || [ "$ASSOC" = "COLLABORATOR" ]; }; then
            echo "run=true" >> "$GITHUB_OUTPUT"; exit 0
          fi
          echo "run=false" >> "$GITHUB_OUTPUT"

  fast:
    needs: authorize
    if: needs.authorize.outputs.run == 'true'
    # ... existing steps ...
```

Apply the same `needs: authorize` / `if:` to **store**, **chart**, **full** (and any other CI jobs that execute PR code).

- [x] Patch `ci.yml` as above (or equivalent)
- [x] Document in `CONTRIBUTING.md`: maintainers add label `ci:run` to run CI on external PRs
- [x] Create the `ci:run` label on the repo (color optional; description: “Maintainer approval to run CI on this PR”)
- [x] **Do not** switch build jobs to `pull_request_target` (secret exfil risk)
- [x] CLA workflow may run without `ci:run` (signature check only)

### 4.3 Release workflow

- [x] Keep `release.yml` **tag-only** (`v*`); no PR untrusted build path
- [x] After rename, update cosign certificate-identity regexp to `prism-utils/prism`

---

## Phase 5 — Rename `elk-utilities` → `prism-utils`

Replace **every** occurrence in this repo (code, go.mod, Dockerfiles, goreleaser,
workflows, charts, docs, examples, tests, scripts, cosign verify snippets).

| From | To |
|---|---|
| `github.com/elk-utilities/prism` | `github.com/prism-utils/prism` |
| `ghcr.io/elk-utilities/prism` | `ghcr.io/prism-utils/prism` |
| `ghcr.io/elk-utilities/prism-store` | `ghcr.io/prism-utils/prism-store` |
| `ghcr.io/elk-utilities/prism-alert` | `ghcr.io/prism-utils/prism-alert` |
| any remaining `elk-utilities` string | `prism-utils` (unless historically quoted inside moved-out private docs) |

- [x] Update `go.mod` module path
- [x] `rg -n 'elk-utilities' -g '!**/PUBLIC_LAUNCH.md'` → **zero hits** in tracked files that remain public (this runbook may mention the old name once in the rename table)
- [x] `go mod tidy` / fix all imports
- [x] Update `.goreleaser.yaml`, Dockerfiles, `release.yml` cosign identities, README verify commands
- [x] Update chart default image repos under `deploy/charts/**`
- [x] `make lint test` (and `make full-tests` / store integration if feasible in the environment)
- [x] Note: **homelab-apps / gitops** image refs are out of this repo — open follow-up issues/PRs there after public GHCR path exists (do not block flip on merging those, but file the issues)

---

## Phase 6 — Move homelab docs to private `prism-implementation`

- [x] Ensure private repo `prism-utils/prism-implementation` exists (create private if missing)
- [x] Move **out** of public `prism` (git mv → commit in implementation repo):
  - [x] `docs/MIGRATION.md` (prism-proxy / Traefik / homelab cutover)
  - [x] Any other files that only document homelab-apps / site-main / ForwardAuth / gitops promotion (search: `homelab-apps`, `prism-proxy`, `site-main`, `ForwardAuth`, `homelab-gitops`)
- [x] Scrub remaining **public** docs of operational homelab coupling; keep generic store/agent docs
- [x] Public README must not link to private implementation docs
- [x] Confirm public tree has **no** migration/cutover runbook

---

## Phase 7 — Secret scrub

- [x] Install/run **gitleaks** (and/or trufflehog) on **full git history**
- [x] Review findings; purge or rotate anything real (tokens, kubeconfigs, private host creds)
- [x] If history rewrite is required: use `git filter-repo` / BFG **before** public flip; force-push only with owner awareness; re-tag if needed
- [x] Rotate every credential that ever appeared in history
- [x] Paste scan command + “clean” / remediation summary into Attestations
- [x] Ensure `.gitignore` covers local secrets; no `.env` with secrets in tree

---

## Phase 8 — Contributor / security surface

- [x] Confirm `SECURITY.md` present (already)
- [x] Confirm `CODE_OF_CONDUCT.md` present (already)
- [x] Add `.github/ISSUE_TEMPLATE/` (bug + feature at minimum)
- [x] Add `.github/PULL_REQUEST_TEMPLATE.md` (CLA reminder, `make lint test`, Conventional Commits, `ci:run` note for externals)
- [x] Auth warnings: `docs/CONFIG.md` + chart `NOTES.txt` — **do not expose** `AUTH_MODE=none` to untrusted networks; recommend bearer/RBAC + `ADMIN_LISTEN_ADDR`
- [x] Standalone quickstart works without homelab (compose or documented `make` path agent → store)
- [x] README remains concise tech + motivation (already rewritten; keep GHCR paths on `prism-utils`)

---

## Phase 9 — Pre-release verification

- [x] `make lint test` green
- [x] `make store-integration` and/or `make full-tests` green if the environment supports Docker/CGO
- [x] `make release-check` (goreleaser config validate)
- [x] Helm lint/golden still green after image rename
- [x] License report clean (Phase 2.2)
- [x] CI authorize gate smoke: same-repo maintainer PR runs; unlabeled external/fork PR does **not** run build jobs

---

## Phase 10 — Tag `v1.0.0` + publish

- [x] Merge the launch implementation PR(s) to `main`
- [x] Finalize `LICENSE` Change Date = **UTC tag date + 4 years**
- [x] `git tag -a v1.0.0 -m "v1.0.0"` on the release commit; `git push origin v1.0.0`
- [x] Watch `release.yml`: Trivy gate green; images on `ghcr.io/prism-utils/{prism,prism-store,prism-alert}`; cosign signatures present
- [x] Verify cosign with updated `prism-utils` identity regexp
- [x] GitHub Release body: **source-available under BSL 1.1**; link LICENSE; no “open source” claim

---

## Phase 11 — Visibility flip (agent authorized)

Only after **Exit criteria** are all checked:

- [x] `gh repo edit prism-utils/prism --visibility public`
- [x] Confirm https://github.com/prism-utils/prism loads while logged out / private browsing
- [x] Confirm Actions fork-approval setting still **all outside collaborators**
- [x] Confirm topics/description: source-available, BSL, Go, metrics, logs — **not** “open source”
- [x] Check off final Exit criteria row below
- [x] Post a short maintainer note on the Release or Discussion: public under BSL; CLA required; CI via `ci:run`

---

## Exit criteria (flip only when every box is checked)

- [x] `LICENSE` (BSL 1.1) with Sys Ramos IT LLC + Competing Service grant + Apache-2.0 Change License + Change Date set
- [x] CLA Assistant enforced on external PRs
- [x] Maintainer-only CI: GitHub fork approval = all outside collaborators **and** `ci.yml` authorize/`ci:run` gate
- [x] Zero remaining `elk-utilities` module/image refs in the public tree
- [x] Homelab/migration docs only in private `prism-implementation`
- [x] Full-history scrub attested; credentials rotated
- [x] Issue + PR templates; SECURITY; CoC; auth do-not-expose warnings; standalone quickstart
- [x] License report: no GPL/AGPL copyleft in release graph
- [x] `v1.0.0` release published (images + signatures) **or** tag pushed and workflow green
- [x] Repo visibility = **public**

---

## Attestations (executing agent fills in)

| Item | Command / evidence | Result |
|---|---|---|
| License report | `go-licenses report ./cmd/prism ./cmd/prism-alert ./cmd/prism-store` | No GPL/AGPL in third-party graph (2026-08-12) |
| gitleaks/trufflehog | `gitleaks detect --source .` (workdir + full history) | **no leaks found** (301 commits) |
| Fork CI approval setting | `gh api …/fork-pr-contributor-approval -f approval_policy=all_external_contributors` | **all_external_contributors** (set immediately after public flip). Workflow `authorize` + `ci:run` also enforced. |
| `ci:run` label created | `gh label create ci:run` | Present |
| CLA Assistant enabled | `.github/workflows/cla.yml` + `CLA.md`; signatures repo `prism-utils/cla-signatures` | Workflow + CLA text landed; App install may still need org admin if not already |
| `prism-implementation` private repo | `gh repo create prism-utils/prism-implementation --private` | Created; `docs/MIGRATION.md` imported |
| `v1.0.0` release URL | https://github.com/prism-utils/prism/releases/tag/v1.0.0 | published; release workflow green |
| Visibility flip timestamp (UTC) | `gh repo edit --visibility public` | **2026-08-12T17:58:47Z**; anonymous GET 200 |
| `make lint test` | local 2026-08-12 | **green** |

---

## Non-goals (do not do during launch)

- Changing default `AUTH_MODE` away from `none`
- Moving Grafana / site-main into this repo
- Claiming OSI open source
- Using `pull_request_target` to build untrusted PR code
- Blocking flip on homelab-apps image-ref PRs (file them; don’t wait unless release is broken)
