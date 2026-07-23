# Spec: prism-store — release/CI (multi-arch signed image + static binaries)

Status: ALL_OK

- **Slug / branch:** `feat/store-release`
- **Owner phase:** orchestrator → developer
- **Issue:** elk-utilities/prism#29 (Epic #21) — depends on #22–#27 (merged).

## 1. Task

Build, sign, and publish the second artifact (`prism-store`) with the same
supply-chain posture as the agent (mirror the existing `prism` release): one
release pipeline, two artifacts. The store needs **CGO + DuckDB**, so its
build and runtime image diverge from the CGO-free agent.

## 2. Scope

- **In scope:**
  - **Version ldflag correctness (shared `internal/version`)** — the current Makefile/goreleaser inject `-X main.version=…`, but both `cmd/prism` and `cmd/prism-store` read `internal/version.Version`, so released binaries report `dev`. Change every ldflag to target `-X github.com/elk-utilities/prism/internal/version.Version=…` (Makefile `LDFLAGS`, goreleaser `prism` build, `release.yml` scan build, and the new store build). This fixes both binaries — the issue explicitly requires the shared `internal/version`.
  - **GoReleaser `prism-store` build** in `.goreleaser.yaml`: `id: prism-store`, `main: ./cmd/prism-store`, `binary: prism-store`, `CGO_ENABLED=1`, `goos: linux`, `goarch: [amd64, arm64]`, `-trimpath`, ldflags `-s -w -X …/internal/version.Version={{.Version}}`. Per-`goarch` cross toolchain via `overrides` (env `CC`/`CXX`): amd64 → `x86_64-linux-gnu-gcc`/`g++`, arm64 → `aarch64-linux-gnu-gcc`/`g++` (go-duckdb v1.8.5 bundles prebuilt static libs for linux amd64+arm64, so only the cross **linker** is needed — DuckDB itself is not recompiled). A separate `archives:` entry `prism-store` (tar.gz, `prism-store_{{.Version}}_{{.Os}}_{{.Arch}}`).
  - **`Dockerfile.store.release`** — mirrors `Dockerfile.release` but on `debian:bookworm-slim` with `libstdc++6` + CA certs installed (DuckDB C++ runtime), runtime **user 472** (non-root, matching the issue), copies the goreleaser-built `prism-store`, `ENTRYPOINT ["/usr/local/bin/prism-store"]`. The store starts the HTTP server by default with no args (the `serve` subcommand is also accepted, config comes from env), so `CMD ["serve"]` is optional/explicit — do not invent flags. Keep the agent's `Dockerfile.release` unchanged.
  - **GoReleaser `dockers:`/`docker_manifests:` for the store** — amd64 + arm64 images `ghcr.io/elk-utilities/prism-store:{{.Version}}-<arch>`, `:sha-<commit>-<arch>`, combined manifests `:{{.Version}}`, `:sha-…`, `:latest`; OCI labels mirror the agent. `docker_signs` already signs all manifests (keyless cosign) — ensure the store manifests are covered. SBOMs: the existing `sboms: artifacts: archive` covers the store archive too.
  - **`release.yml` scan job** — add a second Trivy gate for the store: build a CGO amd64 binary (`CGO_ENABLED=1 … go build ./cmd/prism-store`) + the `Dockerfile.store.release` image, scan at the same HIGH/CRITICAL, `ignore-unfixed` threshold. The release runner must `apt-get install gcc-aarch64-linux-gnu g++-aarch64-linux-gnu` (arm64 cross) before goreleaser. Keep the agent scan intact.
  - **`ci.yml`** — store unit tests already run via `make test` (`./...`). Add a store integration/e2e job (or extend `full`) that exercises the DuckDB path in CI (dind/native), consistent with `docs/TESTING.md`. If no store e2e suite exists yet, wire the hook + a minimal smoke (ingest→flush→query→stats) so the job is real, and note follow-up.
  - **Makefile** — a `snapshot`/`release-check` that also covers the store (e.g. store included in the single goreleaser run); add a `docker-store` convenience target mirroring `docker`.
  - **Docs** — `README.md` (or `docs/`): document the **two-artifact** release (agent = static/distroless; store = CGO/DuckDB/debian-slim, user 472), image names, and `cosign verify` instructions for `ghcr.io/elk-utilities/prism-store`.
- **Out of scope:** Helm (#28); benchmark; the actual DuckDB static-lib rebuild (use bundled libs); Windows/darwin store binaries (issue scopes linux like the image).

## 3. Open questions  (resolved before READY)

- [x] Cross-compile the store binaries in goreleaser, or build per-platform in Docker under QEMU? → **Cross-compile in goreleaser** with per-arch `CC`/`CXX`. go-duckdb bundles prebuilt static libs, so only a cross linker is needed; this keeps ONE pipeline producing both binaries + images (matches the agent) and avoids slow QEMU C++ builds.
- [x] Runtime base? → `debian:bookworm-slim` + `libstdc++6` (DuckDB needs the C++ runtime; distroless-static won't work). User **472** per the issue.
- [x] Fix the `main.version` ldflag mismatch as part of this? → **Yes** — the issue mandates the shared `internal/version`; the current target is a no-op leaving version `dev`.
- [x] Full multi-arch signed build locally verifiable? → **No** — GHCR push + keyless cosign + arm64 are tag/CI-gated. The loop verifies `goreleaser check`, a local amd64 store image build with correct injected version, and the binary build; CI-only steps are mirrored from the agent's proven pattern.

## 4. Decision log  (Decision Protocol)

- **goreleaser cross-CC over in-Docker QEMU compile.**
  - ref: go-duckdb cross-compilation guidance — https://github.com/marcboeker/go-duckdb/issues/279 and pkg.go.dev (bundled prebuilt static libs for linux amd64/arm64).
  - perf: linking against prebuilt libs is fast; QEMU-compiling DuckDB's C++ would be minutes-long per arch.
  - product: one pipeline, two artifacts — identical UX to the agent release.
- **debian:bookworm-slim runtime, user 472.**
  - ref: DuckDB requires libstdc++ at runtime; distroless-static provides no libc++.
  - perf: n/a; product: smallest supported base that satisfies the CGO/DuckDB runtime and the consumer's Kyverno non-root policy.
- **Correct the version ldflag to `internal/version.Version`.**
  - ref: Go `-ldflags -X importpath.name=value` — the value must name the actual symbol.
  - perf: n/a; product: released images/binaries report the real tag (currently always `dev`) — required for the pinned-release consumption in EPIC_I.

## 5. Acceptance checklist  (developer checks these off)

- [x] `goreleaser check` passes with both `prism` and `prism-store` builds/archives/dockers/manifests.
- [x] Ldflags target `…/internal/version.Version` everywhere (Makefile, goreleaser both builds, release.yml scan); `prism version` and `prism-store version` print the injected value (verify via a `-X` build, not `dev`).
- [x] `prism-store` build: CGO_ENABLED=1, linux amd64+arm64, per-arch `CC`/`CXX` overrides; archive `prism-store_<ver>_<os>_<arch>.tar.gz`.
- [x] `Dockerfile.store.release` builds locally for amd64 (`docker build`/buildx) on `debian:bookworm-slim` + `libstdc++6`, runs as user 472, and `docker run … version` prints the injected version.
- [x] Store images + manifests defined (`:<ver>-amd64/-arm64`, `:sha-…`, `:<ver>`, `:latest`); covered by `docker_signs`; SBOM covers the store archive.
- [x] `release.yml` adds a store Trivy scan gate + installs the arm64 cross toolchain before goreleaser; agent scan unchanged.
- [x] `ci.yml` runs a real store integration/e2e job (DuckDB path) — at minimum an ingest→flush→query→stats smoke.
- [x] `README`/docs document the two-artifact release + `cosign verify` for `prism-store`.
- [x] Agent artifacts unchanged in shape (still CGO-free distroless static); no regression to the `prism` release.
- [x] `make lint test` green; `CGO_ENABLED=0 go build ./cmd/prism` passes; `go build ./cmd/prism-store` passes.

## 6. Mandatory review gates  (reviewer owns)

- [x] **Gate 1 — Guidelines:** config mirrors the agent's proven pattern (consistency); no duplicated/divergent logic where it can be shared; store divergences (CGO, base, CC, user 472) are justified in comments; comments self-contained.
- [x] **Gate 2 — Edge cases:** ldflag change verified to actually inject (not silently no-op); arm64 cross toolchain present in the runner; store image runs non-root (472) with a read path to `/data`; Trivy gate fails the release on HIGH/CRITICAL; `goreleaser check` clean; snapshot/local amd64 build reproducible.
- [x] **Gate 3 — Docs/comments match code:** README two-artifact section, image names, and cosign instructions match the goreleaser output names exactly; Dockerfile comments match the base/user; no forward references.
- [x] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [x] Full docs/REVIEW.md checklist; TESTING.md layering (the CI store job is real, not a stub).

## 7. Reviewer notes

**Verdict: ALL_OK** (2026-07-23)

Independent verification:
- **Version ldflags:** `grep main.version` — zero load-bearing hits (spec prose only). Injection points: Makefile `LDFLAGS`, `.goreleaser.yaml` (both builds), `release.yml` scan (both), `Dockerfile` dev build. Proof: `go build -ldflags "-X github.com/elk-utilities/prism/internal/version.Version=vTEST" …` → `prism vTEST` / `prism-store vTEST`.
- **goreleaser check:** passes (v2); both builds/archives/dockers/manifests present; store `CGO_ENABLED=1` with per-arch `CC`/`CXX` overrides.
- **Dockerfile.store.release:** `debian:bookworm-slim`, `libstdc++6`, USER 472, `/data` owned 472, entrypoint `prism-store`. `Dockerfile.release` diff empty (agent unchanged).
- **release.yml:** store Trivy gate mirrors agent (HIGH/CRITICAL, ignore-unfixed); arm64 cross toolchain installed before goreleaser; agent scan steps intact (ldflag fix only).
- **ci.yml + smoke:** `make store-integration` green; `TestStoreIngestFlushQueryStatsSmoke` exercises real engine ingest→flush→query→stats via HTTP handlers.
- **Quality:** `make lint` 0 issues; `make test -race` green; `CGO_ENABLED=0 go build ./cmd/prism` + `go build ./cmd/prism-store` pass.
- **TDD:** `88c3a0a test:` precedes `c7949ed ci:` implementation commit.
- **CI/tag-only (config-checked, not locally executed):** GHCR push, keyless cosign OIDC, arm64 goreleaser docker image build, release Trivy on ubuntu runner.
- **Note:** `make docker-store` on darwin builds a host binary → `docker run … version` fails with exec-format-error unless `GOOS=linux`; on linux CI/ubuntu (where release runs) the target is correct. Dockerfile structure verified via successful `docker build`.

**Re-review: ALL_OK** (2026-07-23, post-`935e3a5`)

CI `full` job failed: `make integration` inherited global `CGO_ENABLED=0` after store smoke landed under `test/integration`, causing go-duckdb `undefined: Conn`. Fix `935e3a5` (Makefile-only): shared `INTEGRATION_GO_TEST := CGO_ENABLED=1 go test … ./test/integration/...` used by both `integration` and `store-integration`; redundant `store-integration` removed from `full-tests`. Verified: commit is Makefile-only; agent `make build` / `CGO_ENABLED=0 go build ./cmd/prism` unchanged; `make lint`, `make test`, `make store-integration` green; integration compile path links DuckDB (no `undefined: Conn`); no duplicate integration entrypoints.
