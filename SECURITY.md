# Security Policy

## Supported versions

Security fixes are applied to the **latest released `v*` tag** on `main`.
Older tags receive fixes only when maintainers explicitly back-port.

During the **public beta** window (see [`docs/PUBLIC_LAUNCH.md`](docs/PUBLIC_LAUNCH.md)),
treat all releases as unstable: upgrade promptly; do not assume long-term
support for a given minor.

## Reporting a vulnerability

**Please do not open a public GitHub issue for security bugs.**

1. Prefer **GitHub Security Advisories** (Private vulnerability reporting) on
   this repository: **Security → Advisories → Report a vulnerability**.
2. If advisories are unavailable, email the maintainers listed in the GitHub
   org profile and include:
   - affected binary (`prism` / `prism-store` / `prism-alert`) and version/tag
   - deployment shape (`standalone` / `client` / `cluster`, auth mode, RBAC on/off)
   - reproduction steps or a minimal proof of concept
   - impact (auth bypass, cross-tenant read/write, RCE, DoS, info leak)

We aim to acknowledge within **7 days** and to provide a remediation plan or
fix release on a timeline that matches severity.

## Threat model (summary)

prism is an **edge collector + multi-tenant columnar store**, not a
multi-cluster SaaS control plane.

| Guarantee (when configured) | Non-guarantee |
|---|---|
| Tenant identity is the path `ns`; unknown/unauthorized tenants get a generic **404** (no existence leak) under RBAC | Network isolation without NetworkPolicy / gateway auth |
| RBAC (JWT/OIDC + deny-by-default policy): roles `reader` / `writer` / `admin` cannot escalate | Auth when `AUTH_MODE=none` and RBAC unset (legacy/dev) |
| Read-only SQL runs in a per-request, tenant-scoped DuckDB sandbox | Protection if the process can write arbitrary files as its OS user |
| HTTP ingest under RBAC uses JWT; Flight keeps separate `AUTH_MODE` and fail-fasts if RBAC+Flight+`none` | Exactly-once delivery, clustering HA, or cross-tenant scatter/gather |

Operators exposing `prism-store` beyond a trusted network **must** enable RBAC
or a non-`none` `AUTH_MODE`, prefer `ADMIN_LISTEN_ADDR` split-plane, and keep
the SQL read queue enabled. See [`docs/STORE.md`](docs/STORE.md#rbac) and
[`docs/PUBLIC_LAUNCH.md`](docs/PUBLIC_LAUNCH.md) §2.

## Supply chain

Tagged releases (`v*`) go through Trivy (HIGH/CRITICAL gate), SBOM generation,
and cosign keyless signing via GitHub OIDC (see `.github/workflows/release.yml`).
Verify signatures before promoting images into production.
