# Spec: Package + consumption contract to adopt prism as monitor-agent v2 collector

Status: READY

- **Slug / branch:** `feat/release-consumption-contract`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Phase 5 (outputs: http), Phase 8 (packaging), plus a new
  "release + consumption contract" slice tracked by GitHub issue #15.
- **Issue:** elk-utilities/prism#15

## 1. Task

`prism` is functionally complete but has **no published package** and lacks a
few transport/consumption features needed to adopt it as the edge collector for
the homelab-apps logging/metrics **v2** architecture (see
`~/git/home-wt/logchef-logging/.ai/specs/agent-datastore-strategy-v2.md`). This
task delivers the five items of issue #15 so a tagged `v1.0.0` release publishes
a signed multi-arch GHCR image + signed static binaries, prism can scrape
authenticated exporters and ship authenticated windows, and the output/artifact
contract is frozen + documented for the proxy sidecar to build against.

## 2. Scope

- **In scope:**
  1. **Release pipeline** — GoReleaser config + a tag-triggered GitHub Actions
     `release` workflow that publishes a multi-arch (`amd64`+`arm64`) GHCR image
     tagged SemVer + `sha-<short>`, a Trivy scan gate, SBOMs, cosign signatures,
     and a GitHub Release with static binaries + `checksums.txt`. Build only on a
     `v*` tag, **never** on merge-to-main. Wire `prism version` to the tag.
  2. **Authenticated egress** — `flight` output gains TLS + bearer auth; a new
     `http` output POSTs encoded blocks with bearer + TLS + bounded retry.
  3. **Prometheus input auth/TLS** — input-level `basic_auth`, `bearer_token`,
     and `tls {ca,cert,key,insecure_skip_verify,server_name}`, all `${ENV}`.
  4. **Frozen output contract** — a versioned `docs/OUTPUT_CONTRACT.md` pinning
     artifact taxonomy, naming, Flight descriptor path, and per-phase schemas.
  5. **Config surface** — `configs/exporters/*.yaml` reference configs and
     Beats-style `config.d` include globs so extra pipelines load from a blob
     path.
- **Out of scope:** the proxy sidecar (homelab-apps), ML, DuckDB/SQLite/OLAP
  output, downsampling, config hot-reload, per-target (as opposed to per-input)
  prometheus auth.

## 3. Open questions (resolved)

- [x] Q: One authenticated transport or both? — A: **Both.** The v2 design is
  Flight-centric but names Traefik ForwardAuth (HTTP) as the clean auth path
  (open question in the design). Landing both `flight` TLS+bearer and an `http`
  parquet POST resolves that open question by giving the consumer a choice.
- [x] Q: Per-target or per-input prometheus auth? — A: **Per-input**, matching
  Prometheus' own `scrape_config` model (one credential set per job/target list).
- [x] Q: Tag string `1.0.0` vs `v1.0.0`? — A: **`v1.0.0`** — Go module +
  GoReleaser + SemVer git-tag convention require the `v` prefix.
- [x] Q: One PR or five? — A: **One** coordinated release-enablement PR with
  per-item commits, tracked by the single issue #15. Noted deliberate deviation
  from the "one component per PR" norm because these items only become useful
  together (a release that ships all four consumption features).

## 4. Decision log

- Release tooling: **GoReleaser** (dockers + docker_manifests + docker_signs)
  triggered only on `v*` tags.
  - ref: https://goreleaser.com/blog/rust-zig/ ; https://goreleaser.com/customization/sign/docker_sign/ — canonical multi-arch manifest + keyless cosign pattern.
  - perf: build cost is CI-only, on tag; zero runtime cost. One tool covers
    binaries, checksums, image, manifest, SBOM, signatures — less bespoke YAML to
    rot.
  - product: GoReleaser + cosign keyless (OIDC, `id-token: write`) + Trivy + SBOM
    is the de-facto supply-chain-secure Go release standard; matches homelab-apps
    parity ask in the issue.
- Flight auth: **`grpc.WithPerRPCCredentials` (bearer) + TLS transport creds**.
  - ref: https://jbrandhorst.com/post/grpc-auth/ ; apache/arrow flightsql driver `grpcCredentials` — idiomatic bearer/basic over gRPC.
  - perf: per-RPC metadata is a small header; TLS handshake amortized over a
    persistent client. No hot-path cost (auth is per DoPut stream, not per row).
  - product: standard gRPC auth; works through a Bearer-checking reverse proxy.
- http output retry: **`github.com/cenkalti/backoff/v4`** (already in the
  DESIGN.md §13 dependency budget), capped exponential backoff + give-up.
  - ref: https://github.com/cenkalti/backoff — maintained, pure-Go.
  - perf: pure-Go, no per-record cost (retry is per block).
  - product: bounded backoff + clear give-up avoids the "silent zero-data" and
    thundering-herd failure modes the v2 design calls out.
- Prometheus input auth: **per-input** basic/bearer/TLS, matching Prometheus.
  - ref: https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config — auth is per scrape job, applied to its target list.
  - perf: credentials attached once at client build; no per-scrape cost beyond
    the header/TLS the endpoint requires.
  - product: mirrors the exporter set's real auth model; secrets via `${ENV}`
    keep them off disk.
- Config includes: **Beats-style `config.d/*.yaml` glob include** merged into the
  pipeline set at load, env-interpolated, cycle-free (one include level).
  - ref: https://www.elastic.co/guide/en/beats/filebeat/current/filebeat-configuration-reloading.html — the well-known `path/*.yml` external-config pattern.
  - perf: load-time only; bounded by the number of matched files.
  - product: lets a site config renderer drop per-exporter pipeline files into a
    directory the way Alloy River / Filebeat modules.d work today.

## 5. Acceptance checklist

- [ ] `.goreleaser.yaml`: multi-arch binaries (`linux/amd64`,`_arm64`),
      `checksums.txt`, SBOMs, `dockers`+`docker_manifests` to
      `ghcr.io/elk-utilities/prism` tagged `{{.Tag}}` + `sha-<short>`, keyless
      `signs` (checksum) + `docker_signs` (manifest); `version` ldflag wired.
- [ ] `.github/workflows/release.yml`: triggers on `push: tags: ['v*']` only;
      `contents: write` + `packages: write` + `id-token: write`; Trivy image scan
      gate; runs GoReleaser.
- [ ] `flight` output: `tls {ca,cert,key,insecure_skip_verify,server_name}` +
      `token` (bearer, `${ENV}`); dials TLS + per-RPC creds; `Validate()` covers
      new fields; collect receiver can require a bearer token for the auth test.
- [ ] `http` output package under `internal/output/http/`: POST block bytes with
      configurable method/headers/`token`/TLS, capped backoff retry + give-up;
      registered in `components.Default()`.
- [ ] `prometheus` input: `basic_auth {username,password}`, `bearer_token`,
      `tls {…}`; wired into the scrape client; `Validate()` with path-named errors.
- [ ] `docs/OUTPUT_CONTRACT.md` v1: taxonomy, naming, descriptor path, per-phase
      schemas; linked from `DESIGN.md`.
- [ ] `configs/exporters/{elasticsearch,postgres,clickhouse,mongodb}.yaml` +
      config.d include support in the loader with `include: ["config.d/*.yaml"]`.
- [ ] DESIGN.md/CONFIG.md updated for new config surface + http output (fix the
      existing "http output available" drift).
- [ ] Tests written first (a `test:` commit precedes implementation).
- [ ] `make lint test` green locally.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases**
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
