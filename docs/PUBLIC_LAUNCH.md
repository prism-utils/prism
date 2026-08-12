# prism — Public launch checklist

> Gate document for flipping **`prism-utils/prism`** (GitHub home; formerly
> `elk-utilities/prism`) from **private** to **public**. This is not a feature
> roadmap; it is the bar that must be green before strangers can clone, run,
> and file issues safely.
>
> Spec / loop state: [`.ai/specs/public-launch.md`](../.ai/specs/public-launch.md).
> Architecture: [`DESIGN.md`](DESIGN.md). Store security: [`STORE.md`](STORE.md#rbac).

**Status today:** private; no `LICENSE`; no `SECURITY.md` (until this track
lands); release pipeline (GoReleaser + Trivy + cosign) already exists for `v*`
tags; agent foundation + store RBAC/SQL/PromQL/Loki paths exist and are used in
homelab via `homelab-apps` charts.

---

## 0. Launch posture (decide once)

| Decision | Recommended default | Why |
|---|---|---|
| **Visibility** | Public **beta** first (not “v1 production”) | Store is feature-rich but defaults and docs still assume a trusted network; beta sets expectation. |
| **License** | **Apache-2.0** | Matches OTel Collector / Arrow ecosystem peers; permissive + patent grant. **Must be confirmed by repo owners before adding `LICENSE`.** |
| **Module / org path** | Align Go module with **`github.com/prism-utils/prism`** (repo already redirected there; `go.mod` still says `elk-utilities`) | Module path is a breaking rename; finish it *before* public, not after. |
| **Name** | Keep `prism` for beta; freeze rename decision before `v1.0.0` | README already marks the name provisional. |
| **Homelab coupling** | Homelab charts stay in `homelab-apps`; upstream ships `deploy/charts/prism-*` + compose only | Public users must not need the three-repo stack. |

Do **not** flip the GitHub visibility switch until §1–§5 below are checked.

---

## 1. Legal & repository hygiene  *(hard blockers)*

- [ ] Owner confirms SPDX license (recommended: Apache-2.0) and adds root `LICENSE`.
- [ ] GoReleaser already packages `LICENSE*` — verify the file is present before the next `v*` tag.
- [ ] Run a dependency license report (`go-licenses` or equivalent) and file any incompatible deps.
- [ ] Add `NOTICE` if required by chosen license / bundled deps.
- [ ] Scrub git history + working tree for secrets, private hostnames, customer tokens, kubeconfigs (`gitleaks` / `trufflehog` on full history).
- [ ] Rotate any credential that ever lived in this private clone.
- [ ] Confirm GitHub org, topics, description, homepage; remove stale private-only references from README status blurb.
- [ ] Decide module-path / org rename **before** first public tag.

---

## 2. Security & threat model  *(hard blockers)*

Reference: [OpenTelemetry Collector security / config best practices](https://opentelemetry.io/docs/security/config-best-practices/) (bind narrowly, auth by default, least privilege).

### 2.1 Documents

- [ ] Root [`SECURITY.md`](../SECURITY.md) — vulnerability reporting (GitHub Security Advisories preferred).
- [ ] Short threat model in `SECURITY.md` or `docs/SECURITY.md`: tenant = `ns`; RBAC deny-by-default; SQL sandbox scope; Flight vs HTTP auth split; what is *not* guaranteed (no multi-cluster HA, no exactly-once).
- [ ] `CODE_OF_CONDUCT.md` (Contributor Covenant or equivalent) with a real contact.

### 2.2 Secure-by-default (code / chart — must land before public)

Today `AUTH_MODE` defaults to **`none`** (`cmd/prism-store`, `docs/CONFIG.md`). That is acceptable on a private cluster behind Traefik ForwardAuth; it is **not** acceptable as the first experience for a public binary listening on `:8080`.

- [ ] **Public-facing auth policy** (pick one and implement):
  - **A (preferred):** fail-fast on serve if no auth is configured (`AUTH_MODE=none` **and** RBAC unset) unless `ALLOW_INSECURE=true` (dev-only, logged loudly); **or**
  - **B:** change release/chart defaults to `AUTH_MODE=bearer` requiring `INGEST_TOKEN`, and document RBAC as the production path.
- [ ] Chart values / `NOTES.txt` scream when auth is off.
- [ ] Document split-plane: set `ADMIN_LISTEN_ADDR` so query/admin are not on the public ingest listener in production examples.
- [ ] Keep SQL read queue default **on**; document `/metrics` exposure on the shared listener ([`STORE.md`](STORE.md)).
- [ ] NetworkPolicy examples remain opt-in but recommended in the quickstart “production” path.
- [ ] Confirm RBAC + Flight fail-fast when `AUTH_MODE=none` stays green (`cmd/prism-store` already enforces this).

### 2.3 Supply chain

- [ ] `v*` release workflow: Trivy HIGH/CRITICAL gate + cosign + SBOM (already in `.github/workflows/release.yml`) — verify on a dry-run tag in a fork or pre-release.
- [ ] Publish `govulncheck ./...` (or equivalent) in CI, not only Trivy on images.
- [ ] Pin action SHAs or stay on audited major tags; document the release trust model in README.

---

## 3. Product surface & docs  *(hard blockers for a usable public beta)*

- [ ] README “Quick start” that works **without** homelab: agent → local file/http **or** compose `prism` + `prism-store` with auth enabled.
- [ ] Explicit **Supported / Unsupported** section (no clustering HA, no exactly-once, no ML/script processors yet — per [`DESIGN.md`](DESIGN.md) non-goals).
- [ ] Link freeze contracts: [`OUTPUT_CONTRACT.md`](OUTPUT_CONTRACT.md), [`CONFIG.md`](CONFIG.md), [`STORE.md`](STORE.md), [`MEMORY.md`](MEMORY.md), [`ALERTING.md`](ALERTING.md).
- [ ] Homelab-only docs (`MIGRATION.md` prism-proxy cutover, site-main reconciler assumptions) labeled **“Homelab / downstream”** so upstream users skip them.
- [ ] Helm: `deploy/charts/prism-store` + `prism-alert` installable from the public repo (OCI or raw); examples use secrets, not inline tokens.
- [ ] Version story: public beta tags (`v0.x` or `v1.0.0-beta.N`) until §2 secure defaults land; only then cut `v1.0.0`.

---

## 4. Contributor experience

- [ ] Issue templates (bug / feature) under `.github/ISSUE_TEMPLATE/`.
- [ ] PR template pointing at `make lint test` (+ `full-tests` when required) and Conventional Commits.
- [ ] Public-facing CONTRIBUTING summary that does **not** require Cursor agents; keep TDD as the project bar.
- [ ] CODEOWNERS (optional but useful once public).
- [ ] CI green on forks / PRs from external contributors (secrets that break fork CI documented or avoided).

---

## 5. Operational readiness

- [ ] Backup / retention operator notes (tier layout, `RETENTION_DAYS`, what to snapshot under `DATA_DIR`).
- [ ] Resource sizing pointer to [`MEMORY.md`](MEMORY.md).
- [ ] Clear statement: multi-tenant isolation is **logical** (`ns` + RBAC + path guards), not a SaaS control plane.
- [ ] Upgrade / compatibility policy for Parquet artifact versions and store on-disk layout.
- [ ] At least one “production-shaped” compose or Helm overlay with RBAC + split admin plane + NetworkPolicy.

---

## 6. Soft / post-launch (not blockers for beta)

- [ ] External security review or paid audit (schedule after beta traffic).
- [ ] Public roadmap issue / discussion board.
- [ ] Benchmarks published under `bench/` with reproducible commands.
- [ ] Rename finalization + trademark check.
- [ ] Community Slack/Discord (optional).

---

## Exit criteria

**Public beta flip is allowed when:**

1. §1 license + secret scrub are done.
2. §2 `SECURITY.md` + secure-by-default auth policy are merged and tested.
3. §3 standalone quickstart works on a clean machine.
4. A signed pre-release tag validates the existing release workflow.
5. Repo owners explicitly approve the visibility change.

**Public `v1.0.0` additionally requires:** frozen name, module path, and output/store contracts; auth defaults that fail closed without an explicit insecure escape hatch; no known Critical open CVEs in release images.
