# Spec: prism-store — project-agnostic Helm chart + capacity model

Status: READY

- **Slug / branch:** `feat/store-helm`
- **Owner phase:** orchestrator → developer
- **Issue:** elk-utilities/prism#28 (Epic #21) — depends on #22–#27, #29 (merged).

## 1. Task

Ship a **project-agnostic** Helm chart for `prism-store` with measured-ratio
resource defaults, a documented capacity/sizing model, and an optional
`examples/` overlay for gateway/secret/Grafana wiring. The base chart has **no**
Traefik/ESO/Grafana hard dependency — consumers layer those in their own repo.

## 2. Scope

- **In scope** (`deploy/charts/prism-store/`):
  - **Workload** — a **StatefulSet** (stable PVC identity for the single-writer DuckDB), `replicas: 1`, `Recreate`-equivalent (StatefulSet default `OnDelete`/`RollingUpdate` with `replicas:1` is fine; document the single-writer constraint). RWO PVC via `volumeClaimTemplates`.
  - **SecurityContext** — pod `fsGroup: 472`; container `runAsNonRoot: true`, `runAsUser: 472`, `runAsGroup: 472`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, drop `ALL` caps; `/tmp` `emptyDir`; `/data` from the PVC (writable).
  - **Service** — ClusterIP exposing the public ingest port (8080) and, when `ADMIN_LISTEN_ADDR` is set, the admin/stats/query port on a separate port. (Match the split-plane model from #27.)
  - **Volume** — `/data` PVC; parameterize `persistence.size` (default **32Gi** per the ≤1k/s row) and `persistence.storageClass`. Document the future hot-SSD/cold-HDD split as a comment but keep a single-PVC default.
  - **Probes** — liveness `GET /healthz` (periodSeconds 15), readiness `GET /readyz` (periodSeconds 10), on the public port.
  - **Config env as values** — expose ALL store env (verified from `cmd/prism-store`): `LISTEN_ADDR`, `ADMIN_LISTEN_ADDR`, `ADMIN_TOKEN`, `FLIGHT_ADDR`, `DATA_DIR`, `ALLOWED_ARTIFACTS`, `MAX_BODY_BYTES`, `INGEST_TOKEN`, `AUTH_MODE`, `ROUTE_PREFIX`, `HOT_WINDOW_MINUTES`/`HOT_WINDOW_SECONDS`, `SEGMENTS_PER_TIER`, `MAX_SEGMENT_BYTES`, `RETENTION_DAYS`, `ROLLUP_STEPS`, `MAX_TIER`, `HOT_SNAPSHOT_SECONDS`, `FLUSH_TICK_SECONDS`, `MERGE_TICK_SECONDS`, `RETENTION_TICK_HOURS`/`RETENTION_TICK_SECONDS`. Values map with the code defaults (`:8080`, `/data`, `metrics-raw`, auth `none`, hot 10m, seg/tier 6, max-seg 2Gi, retention 15d, rollups `1m,5m,1h`, maxTier 8, snapshot 15s, flush 30s, merge 60s, retention 1h). Secrets (`INGEST_TOKEN`, `ADMIN_TOKEN`) sourced from an existing `Secret` via `valueFrom.secretKeyRef` (name parameterized), never inlined in values by default.
  - **Resources** — default to the **≤1k/s** profile: `requests: {cpu: 250m, memory: 512Mi}`, `limits: {cpu: "2", memory: 2Gi}`.
  - **Optional `networkPolicy`** template (default off): allow ingest port from a configurable source; restrict the admin port to a configurable namespace/selector.
  - **`examples/`** overlay values showing gateway/Ingress + secret + Grafana DuckDB datasource wiring (using the `print-view-sql` output from #26) — clearly marked as examples, not part of the base chart.
  - **Capacity model docs** — `values.yaml` comments carry the recommended-defaults table (4 rows) + the ratios/formula; `docs/STORE.md` gains a **Sizing** section: CPU ≈ 0.013 cores per 1k samples/s; `mem ≈ 128 MiB + ingest_rate × HOT_WINDOW_s × ~0.4 KiB/row × ~1.5`; storage ≈ `95 bytes/sample × rate × retention_days × 86400 × ~1.3`; levers = shrink `HOT_WINDOW`, cap `MAX_SEGMENT_BYTES` (512Mi–1Gi). Note the "10 MB/5k/s" myth is ~100× off.
  - **CI** — add `helm lint` + `helm template` snapshot tests to `ci.yml` (a `chart` job or fold into `fast`); commit a golden rendered manifest for the default profile and assert stability.
  - **Chart packaging/publish** — `Chart.yaml` (semver, appVersion tracks the store), `helm package`; add an OCI push to GHCR (`oci://ghcr.io/elk-utilities/charts`) on `v*` tags in `release.yml`, mirroring the image publish (tag-gated).
- **Out of scope:** the consumer's Traefik/ESO overlay (homelab, EPIC_I); actual Grafana deployment; benchmark; `MaxOpenTenants` (the issue lists it but the store exposes **no such env today** — see §7 Notes, do not invent a value).

## 3. Open questions  (resolved before READY)

- [x] Deployment or StatefulSet? → **StatefulSet**, `replicas:1`, `volumeClaimTemplates` — stable PVC identity for the single-writer DuckDB; avoids two pods ever mounting the same RWO volume during a rollout.
- [x] Where does the chart live? → `deploy/charts/prism-store/` (prism already has `deploy/`).
- [x] How are tokens supplied? → `secretKeyRef` to a pre-existing Secret (name/keys parameterized); base chart does **not** create or manage secrets (no ESO dependency).
- [x] Publish mechanism? → `helm package` + OCI push to GHCR on tag (mirrors the image release); local/CI verifies `helm lint` + `helm template` snapshot.
- [x] `MaxOpenTenants` value? → **Omit** — no corresponding env exists in `cmd/prism-store`; flag as a follow-up rather than shipping a dead value.

## 4. Decision log  (Decision Protocol)

- **StatefulSet + RWO volumeClaimTemplate over Deployment.**
  - ref: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/ — stable storage + at-most-one semantics.
  - perf: n/a; product: DuckDB is a single writer; a StatefulSet with `replicas:1` prevents a rollout from double-mounting `/data` and corrupting the engine file.
- **Measured-ratio resource defaults, not a flat assumption.**
  - ref: issue #28 measured table (node `sunset`, zstd Parquet metrics-raw); DuckDB memory guidance — https://duckdb.org/docs/guides/performance/how_to_tune_workloads
  - perf: memory is dominated by the hot window + arena (~0.4 KiB/row × 1.5), so the lever is `HOT_WINDOW`/`MAX_SEGMENT_BYTES`, not a fixed cap.
  - product: defaults match today's proxy footprint (250m/512Mi req, 2/2Gi limit) and scale via a documented table.
- **No gateway/secret manager in the base chart.**
  - ref: Helm best practices — keep charts portable; https://helm.sh/docs/chart_best_practices/
  - perf: n/a; product: the generalization goal — consumers bring Traefik/ESO/Grafana via overlays.

## 5. Acceptance checklist  (developer checks these off)

- [ ] `helm lint deploy/charts/prism-store` passes.
- [ ] `helm template` with default values renders a runnable StatefulSet+Service+PVC(+probes+securityContext) with no consumer-specific (Traefik/ESO/Grafana) resources.
- [ ] All store env from `cmd/prism-store` are wired as values with code-matching defaults; tokens via `secretKeyRef` (not inline).
- [ ] SecurityContext: uid/gid/fsGroup 472, `readOnlyRootFilesystem: true`, `/tmp` emptyDir, `/data` writable PVC; probes on `/healthz`(15s)/`/readyz`(10s).
- [ ] Default resources = ≤1k/s row (req 250m/512Mi, limit 2/2Gi); capacity table + formula in `values.yaml` comments and `docs/STORE.md` Sizing section.
- [ ] Optional `networkPolicy` (default off) renders correctly when enabled; admin port restricted separately from ingest.
- [ ] `examples/` overlay renders (gateway/secret/Grafana datasource) and is excluded from the base install.
- [ ] `helm template` snapshot/golden test in `ci.yml`; a `chart` CI job runs `helm lint` + template + snapshot.
- [ ] `Chart.yaml` valid semver; OCI publish step added to `release.yml` (tag-gated), mirroring the image publish.
- [ ] `make lint test` still green; no Go behavior changed; `go build ./cmd/prism-store` passes.

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Guidelines:** chart idiomatic (helpers `_helpers.tpl`, labels, `.Values` with sane defaults); no hard-coded consumer specifics; secrets never defaulted to literals; single-writer constraint enforced by the workload choice.
- [ ] **Gate 2 — Edge cases:** `helm template` with defaults AND with `networkPolicy.enabled=true`, `adminListenAddr` set (separate port + service), custom storageClass/size, auth modes (`none`/`bearer`/mTLS/trusted-header) — all render valid manifests; readOnlyRootFilesystem doesn't break `/data` or `/tmp`; probes hit the right port when split-plane is on.
- [ ] **Gate 3 — Docs/comments match code:** every value maps to a real env in `cmd/prism-store` (no dead values like `MaxOpenTenants`); defaults match the code constants; STORE.md sizing math matches the values comments; image tag/name matches #29's `ghcr.io/elk-utilities/prism-store`.
- [ ] **Gate 4 — Atomic comments** (§3.8): template/doc comments self-contained.
- [ ] Full docs/REVIEW.md checklist; TESTING.md layering (the CI `chart` job is real: lint + template + golden snapshot, not a stub).

## 7. Reviewer notes

_(empty until first review)_
