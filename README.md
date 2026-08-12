# prism

**Source-available** edge collector + columnar store for **logs and metrics**
in one pipeline. Go. Config-driven (OTel-shaped). Parquet on the wire.

> **License:** Business Source License 1.1 (BSL) — see [`LICENSE`](LICENSE)
> (BSL 1.1 — see also docs/LICENSE_FAQ.md). Not OSI-approved until the Change Date; source-available under BSL until then.
> External PRs require a **CLA**.
>
> **First public tag:** **`v1.0.0`**. Launch runbook:
> [`docs/PUBLIC_LAUNCH.md`](docs/PUBLIC_LAUNCH.md).

## Why

One agent and one purpose-built store for logging **and** metrics — instead of
bolting together separate log and metrics stacks — so a deployment can run on
**far less CPU/RAM**. Aggregation and encoding happen on the **agent** (server
offload). Immutable Parquet tiers **decouple storage from CPU**: ingest, merge,
and query scale on different axes.

## Components

| Binary | Role |
|---|---|
| **`prism`** | Edge agent: scrape/tail → Arrow → Parquet/JSON windows ([`OUTPUT_CONTRACT`](docs/OUTPUT_CONTRACT.md)). Prefer `CGO_ENABLED=0` static builds where the encoder allows. |
| **`prism-store`** | Multi-tenant store: HTTP/Flight ingest, DuckDB hot + tiered Parquet, PromQL + Loki-compatible APIs, read-only SQL, optional JWT/OIDC RBAC. Modes: `standalone` / `client` / `cluster`. |
| **`prism-alert`** | Per-tenant PromQL ruler → Alertmanager v4 webhooks. |

Deep dives: [`DESIGN`](docs/DESIGN.md) · [`CONFIG`](docs/CONFIG.md) · [`STORE`](docs/STORE.md) · [`MEMORY`](docs/MEMORY.md) · [`ALERTING`](docs/ALERTING.md)

## Pipeline

```
input → parse → processors → buffer → fan-out { encode → output }
```

Metrics path: `prometheus` scrape → buffer → `{parquet, summary JSON}`.  
Logs path: `file` tail → parse → template → buffer → `{parquet, summary JSON}`.

## Quickstart

```bash
make build                 # ./bin/prism
make test                  # unit
make full-tests            # unit + integration + e2e (Docker)
./bin/prism validate -c configs/logging.yaml
./bin/prism run -c configs/logging.yaml
```

Store (CGO + DuckDB): `go build -tags duckdb_arrow ./cmd/prism-store`  
Charts: `deploy/charts/prism-store`, `deploy/charts/prism-alert`  
Compose examples: `deploy/docker-compose.*.yml`

**Auth:** `AUTH_MODE` defaults to `none` for trusted networks. Do **not** expose
ingest/query to untrusted networks without `bearer`/RBAC and preferably
`ADMIN_LISTEN_ADDR` split-plane. See [`STORE` RBAC](docs/STORE.md#rbac).

## Releases

`v*` tags run [`.github/workflows/release.yml`](.github/workflows/release.yml):
multi-arch GHCR images, checksums, SBOM, cosign (OIDC). Images target
`ghcr.io/prism-utils/{prism,prism-store,prism-alert}` after the rename cut.

```bash
make release-check   # validate goreleaser config
make snapshot        # local build, no push
```

## Benchmarks

Reproducible agent/store vs ClickHouse harness: [`bench/`](bench/)
(`make bench`, `make bench-api`, `make bench-api-arrow`). Results and charts
live under `bench/`, not in this README.

## Develop

- Go **1.25+**; Docker for `full-tests` / bench.
- TDD is mandatory: [`CONTRIBUTING.md`](CONTRIBUTING.md), [`docs/TESTING.md`](docs/TESTING.md).
- Security reports: [`SECURITY.md`](SECURITY.md). Conduct: [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md).
- Agent loop (optional): [`AGENTS.md`](AGENTS.md).

## Non-goals (today)

No clustering HA / exactly-once, no ML or scripted processors in-tree, no
embedded SaaS control plane. Homelab deployment glue lives outside this repo.
