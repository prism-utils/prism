# prism — Opening plan (decisions locked)

> Executable plan to flip [`prism-utils/prism`](https://github.com/prism-utils/prism)
> from private → public. Spec: [`.ai/specs/public-launch.md`](../.ai/specs/public-launch.md).
>
> This supersedes the earlier draft checklist preferences where they conflict.

## Locked decisions (2026-08-12)

| # | Topic | Decision |
|---|---|---|
| 1 | MVP readiness | **Open as public beta on what is already shipped** (agent e2e + store + alert + Helm/release). No extra MVP feature gate. |
| 2 | License | **BSL 1.1** + **CLA required on every external PR**. |
| 3 | Module / branding | Rename **all** `elk-utilities` → `prism-utils` (Go module, imports, GHCR, cosign identity, docs). |
| 4 | Auth defaults | Keep **`AUTH_MODE=none`**; document “do not expose unauthenticated”; no fail-closed code change for launch. |
| 5 | First public tag | **`v1.0.0`** when this plan’s exit criteria are green. |
| 6 | Name | Keep **`prism`**; freeze before `v1.0.0` (already the working name). |
| 7 | Upstream boundary | Public repo = agent + store + alert + in-repo Helm/compose. Grafana / site-main / reconciler stay in `homelab-apps`. |
| 8 | Homelab docs | **Remove from public tree.** Move to a **private** `prism-implementation` repo (create if missing). |
| 9 | Secrets | Full-history scrub + rotate anything that ever leaked (**hard gate**). |
| 10 | Contributor surface | Issue + PR templates + CoC + SECURITY (no CODEOWNERS/`govulncheck` required pre-flip). |
| 11 | Visibility flip | **Agent may flip** once exit criteria below are checked off. |

### Motivation (must appear in README)

Unify **logging + metrics** in a single edge agent and a store built for that
workload, so a deployment can use **far less resources** than a bolted-together
Loki+Prometheus+warehouse stack. Push aggregation/encoding to the **agent** to
offload the server, and **decouple storage from CPU** by design (immutable
Parquet tiers + separate query/ingest planes).

---

## BSL 1.1 + CLA — compatibility review

### Verdict

**No hard conflict** with prism’s current direct dependency set (permissive:
Apache-2.0 / MIT / BSD-class — Arrow, DuckDB, Prometheus client, OIDC, koanf,
gRPC, etc.). BSL projects routinely consume those licenses.

**BSL is not OSI “open source.”** It is **source-available** until the Change
Date. Marketing, README, and GitHub topics must say **source-available / BSL**,
never “open source,” until conversion.

### Real constraints (not blockers if handled)

| Area | Finding | Plan action |
|---|---|---|
| **GPL/AGPL deps** | MariaDB FAQ: BSL and GPL are **incompatible before Change Date**. | Gate: refuse new GPL/AGPL/LGPL-copyleft deps; run a license report before `v1.0.0`. |
| **Permissive deps** | MIT/Apache/BSD → OK inside a BSL work. | Keep NOTICE attribution for Apache deps (Arrow, etc.). |
| **CLA** | Required so prism-utils can ship BSL and later convert / dual-license commercially. MariaDB also accepts BSD-licensed patches; **CLA is stricter and matches your choice**. | Add CLA bot + `CONTRIBUTING` / PR template gate; block merge without signature. |
| **Additional Use Grant** | Without a grant, **production use is disallowed** by default BSL. | Must choose grant wording before LICENSE lands (see open params below). |
| **Change License** | Must be GPL-2+ or compatible. Cockroach/Sentry/Couchbase use **Apache-2.0**. | Recommend Apache-2.0. |
| **Change Date** | Max **4 years** from first public distribution of that version. | Recommend 3 or 4 years (param below). |
| **Downstream homelab** | Licensor (`prism-utils` / your entity) using prism in homelab is fine. Third parties need the Additional Use Grant or a commercial license. | Document in LICENSE FAQ / README License section. |
| **GHCR / binaries** | Same BSL applies to images and binaries. | Cosign/GHCR paths move to `ghcr.io/prism-utils/...`. |
| **Ecosystem friction** | Some distros/corp policies ban BSL; GitHub won’t treat it as OSI OSS. | Accept; do not claim OSI compliance. |
| **Goreleaser** | Already packages `LICENSE*`. | Ship root `LICENSE` as BSL 1.1 parameterized text. |

### Still required to write `LICENSE` (answer these next)

BSL 1.1 is a **template**. These four fields are mandatory:

1. **Licensor** legal name (e.g. individual or “Prism Utils …” entity)
2. **Additional Use Grant** style:
   - A) Production OK except offering prism as a **competing hosted/SaaS/DBaaS** (Cockroach/Sentry-style) — **recommended**
   - B) Production OK only under a **numeric cap** (MariaDB MaxScale-style)
   - C) **None** (non-production only unless commercial license)
3. **Change Date** offset from first public `v1.0.0`: **3 years** or **4 years**
4. **Change License**: recommend **Apache-2.0** (confirm or override)
5. **CLA mechanism**: GitHub CLA assistant / CLA bot repo / other

---

## Workstreams (ordered)

### A — Legal & identity

1. Answer BSL parameters above; add root `LICENSE` (BSL 1.1) + short `LICENSE_FAQ.md` if useful.
2. Add CLA workflow; PR template requires signed CLA for external contributors.
3. Rename module `github.com/elk-utilities/prism` → `github.com/prism-utils/prism` everywhere (Go imports, Dockerfiles, goreleaser, CI cosign identity regexp, README verify commands, chart values).
4. Rename GHCR to `ghcr.io/prism-utils/{prism,prism-store,prism-alert}`.
5. Full-history gitleaks/trufflehog; rotate leaked credentials; record attestation in this file.

### B — Docs boundary

1. Create private **`prism-implementation`** repo (if absent).
2. Move out of public tree: `docs/MIGRATION.md` and any homelab/Traefik/site-main/prism-proxy cutover narrative; scrub remaining homelab-only asides from `STORE.md` / `OUTPUT_CONTRACT.md` / README.
3. Public docs keep: DESIGN, CONFIG, STORE, MEMORY, OUTPUT_CONTRACT, ALERTING, TESTING, REVIEW, PLAN (sanitized), PUBLIC_LAUNCH/this plan.
4. README: concise tech README with motivation (done in launch PR); no giant bench tables in README — link `bench/`.

### C — Product surface for `v1.0.0`

1. Standalone quickstart (compose or make target) for agent → store **without** homelab.
2. Chart `NOTES.txt` + CONFIG: warn clearly that `AUTH_MODE=none` is for trusted networks only; recommend bearer/RBAC + `ADMIN_LISTEN_ADDR` split for exposure.
3. Issue + PR templates.
4. Tag **`v1.0.0`** via existing release workflow (Trivy + cosign + SBOM); verify cosign against `prism-utils` paths.

### D — Flip

1. Confirm A–C checkboxes green.
2. Agent sets GitHub repo visibility to **public** (decision 11).
3. Publish GitHub Release for `v1.0.0`; announce as **source-available (BSL 1.1)**, not open source.

---

## Exit criteria (agent may flip when all are true)

- [ ] BSL parameters answered; `LICENSE` merged
- [ ] CLA enforced on external PRs
- [ ] Zero `elk-utilities` references remain in this repo (module, images, docs, workflows)
- [ ] Homelab/migration docs removed from this repo and present only in private `prism-implementation`
- [ ] History scrub + rotation attested
- [ ] Concise README + SECURITY + CoC + issue/PR templates present
- [ ] Standalone quickstart works on a clean machine
- [ ] Auth “do not expose” warnings in CONFIG + chart NOTES
- [ ] `v1.0.0` release workflow dry-run or tag ready (Trivy green)
- [ ] License report: no GPL/AGPL copyleft deps in the release graph

---

## Non-goals for the flip

- Changing `AUTH_MODE` default away from `none`
- Moving Grafana/site-main into prism
- OSI open-source relicensing before Change Date
- CODEOWNERS / `govulncheck` as launch blockers
