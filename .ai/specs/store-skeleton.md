# Spec: prism-store — naming + repository-design ADR + compiling skeleton

Status: READY

- **Slug / branch:** `feat/store-skeleton`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** new component track (store) — foundational slice
- **Issue:** elk-utilities/prism#22 (Epic #21)

## 1. Task

Establish the second prism binary, **`prism-store`** — the durable, tiered
columnar store + query server currently living in `homelab-apps/services/prism-proxy`
— inside this repo, without any behavior yet. This slice records the **ADR**
(name, one-module/two-binary/one-release layout, reuse boundaries, and the
**CGO/DuckDB** decision) in `docs/DESIGN.md`, adds a `docs/STORE.md` design page,
and lands a **compiling no-op skeleton** (`cmd/prism-store` + `internal/store/**`
package stubs + a shared `internal/version`) so the dependency-ordered
sub-issues (#23–#29) have a home. It blocks all of them (paths/names).

## 2. Scope

- **In scope:**
  - **ADR in `docs/DESIGN.md`** (new numbered section, e.g. §15 "ADR: prism-store"):
    - Name: **`prism-store`** (binary `prism-store`, image `ghcr.io/elk-utilities/prism-store`); `prism-proxy` is the old consumer name (compat note for migration).
    - Monorepo, **one Go module** `github.com/elk-utilities/prism`; `cmd/prism-store` beside `cmd/prism`.
    - Package layout `internal/store/{ingest,engine,lifecycle,merge,rollup,query,tenant,stats}`; reuse (do not fork) `internal/columnar`, `internal/encoder/parquet`, `internal/config`, `internal/tlsconf`, `internal/obs`; the store is the **consumer** side of `docs/OUTPUT_CONTRACT.md`.
    - **CGO decision:** `prism-store` links DuckDB via `github.com/marcboeker/go-duckdb` → it is **CGO-linked** (image base `debian:bookworm-slim` + `libstdc++6`, runtime uid/gid **472**). The **agent (`cmd/prism`) stays pure-static `CGO_ENABLED=0`** because it never imports `internal/store`. Document the build invariant: **build/scan each binary by explicit `./cmd/...` path** (never `CGO_ENABLED=0 go build ./...`, which would try to build the CGO store packages); tests run with `CGO_ENABLED=1`.
  - **`internal/version`**: a tiny shared package exposing a `Version` string (default `"dev"`, overridable via `-ldflags -X`), consumed by both `cmd/prism` and `cmd/prism-store`. Rewire `cmd/prism`'s existing `version` var to it without changing its output contract.
  - **`cmd/prism-store/main.go`**: compiles and runs as a **no-op server** — subcommands `version` (prints `prism-store <version>`) and `serve` (default): load config from env (`LISTEN_ADDR` default `:8080`, `DATA_DIR` default `/data`), start an `http.Server` (`ReadHeaderTimeout` 15s) exposing `GET /healthz` (`ok`) and `GET /readyz` (`MkdirAll(DATA_DIR)` writability → `503`/`ready`), graceful shutdown (10s) on SIGINT/SIGTERM. No ingest/engine/query yet (they return nothing / are absent).
  - **`internal/store/**` skeleton**: each package present with a `doc.go` stating its responsibility and the minimal exported placeholder(s) the next sub-issue will flesh out. Pure-Go, no `go-duckdb` yet (added in #24 with the engine). Every package builds and (where it has any logic, e.g. `tenant` validators) has a unit test.
  - **`internal/store/tenant`**: port the tenant + artifact **regex validators** now (they are shared by ingest #23 and engine #24 and are pure-Go): `TenantAllowed` (`^[a-z0-9][a-z0-9._-]{0,62}$`) and an artifact allow-check; table tests incl. empty, leading `.`/`-`, too-long, uppercase, path-traversal.
  - **Docs:** `docs/STORE.md` (store overview + on-disk layout + env table stub, marked "expanded per sub-issue"); update `README.md`, `AGENTS.md`, `TASKS.md` to describe the **two components** (agent + store).
- **Out of scope (later sub-issues):** any real ingest, DuckDB engine, compaction, rollups, query, provisioning, `/stats`, Flight receiver, Helm chart, release wiring, and the `go-duckdb` dependency itself. No homelab-specific strings.

## 3. Open questions  (resolved before READY)

- [x] Name → **`prism-store`** (issue #22 recommendation + product owner).
- [x] CGO/DuckDB acceptable for the store binary? → **Yes** (product owner; matches #29). Agent stays static.
- [x] Add `internal/version` here or in #29? → **Here** — both binaries need it now; #29 only wires ldflags.
- [x] Bring `go-duckdb` in now? → **No** — keep the skeleton pure-Go/no-op; the dep lands with the engine (#24) so CGO CI concerns are introduced with the code that needs them.

## 4. Decision log  (Decision Protocol)

- **One Go module, two binaries, one release (monorepo).**
  - ref: https://go.dev/doc/modules/layout — official "server project with multiple binaries" layout (`cmd/<binary>`, shared `internal/`).
  - perf: no cost; shared `internal/columnar`/`encoder/parquet` means the frozen OUTPUT_CONTRACT producer+consumer compile together (CI can assert round-trip).
  - product: contract, producer, consumer evolve atomically; one supply-chain pipeline.
- **`prism-store` is CGO-linked (DuckDB); agent stays `CGO_ENABLED=0`.**
  - ref: https://github.com/marcboeker/go-duckdb — DuckDB Go driver is cgo (bundled static libs per platform); no pure-Go DuckDB exists.
  - perf: DuckDB is the proven embedded OLAP engine for this exact tiered-parquet workload; a pure-Go substitute would regress query performance materially.
  - product: two artifacts with distinct build posture is standard; isolating the CGO surface to one binary preserves the agent's static guarantee.
- **Skeleton is a compiling no-op; validators ported early.**
  - ref: https://go.dev/doc/modules/layout — package-per-responsibility under `internal/`.
  - perf: n/a. product: unblocks parallel sub-issues with stable import paths; the pure-Go tenant/artifact validators are shared and cheap to land now with tests.

## 5. Acceptance checklist  (developer checks these off)

- [ ] **ADR** added to `docs/DESIGN.md` (name, one-module/two-binary/one-release, package layout, reuse boundaries, CGO decision + build invariant). No existing DESIGN content made untrue.
- [ ] **`internal/version`** package added; `cmd/prism` uses it with its `version` output unchanged (existing `cmd/prism` version test still passes).
- [ ] **`cmd/prism-store`** compiles; `prism-store version` prints the version; `serve` starts, serves `/healthz`=`ok` and `/readyz` (writability check → `503`/`ready`), and shuts down gracefully on signal. Handler unit tests (httptest) for healthz/readyz (incl. unwritable data dir → 503).
- [ ] **`internal/store/**`** skeleton packages present (`ingest`, `engine`, `lifecycle`, `merge`, `rollup`, `query`, `tenant`, `stats`), each with a `doc.go`; all compile.
- [ ] **`internal/store/tenant`** validators (`TenantAllowed`, artifact allow) with table-driven tests (empty, leading `.`/`-`, >63 chars, uppercase, path traversal, valid).
- [ ] **`docs/STORE.md`** created (overview, on-disk layout, env-table stub); `README.md` / `AGENTS.md` / `TASKS.md` updated to describe agent + store.
- [ ] Build invariant holds: `CGO_ENABLED=0 go build ./cmd/prism` still succeeds; `go build ./cmd/prism-store` succeeds; `go vet ./...` clean.
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally.

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md): factory/config patterns where relevant, no globals, slog only, atomic comments; skeleton packages are leaf and don't import `pipeline`/each other beyond the documented reuse.
- [ ] **Gate 2 — Tests cover edge cases** (tenant validator boundaries; readyz unwritable dir; version output).
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (ADR, STORE.md, README/AGENTS/TASKS reflect exactly what landed; no forward-referencing of unbuilt behavior as if present).
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8).
- [ ] Full docs/REVIEW.md checklist passes.

## 7. Reviewer notes

_(empty until first review)_
