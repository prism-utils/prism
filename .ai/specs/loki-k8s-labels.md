# Spec: Enrich Loki/store log labels with Kubernetes metadata

Status: READY
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/prr-loki-k8s-labels-8a90`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** Parsers + Loki logs API surface (extends `internal/parser/logs`, docs; closes prism#126)
- **Issue:** [prism#126](https://github.com/prism-utils/prism/issues/126) (PRR / Option A)

## 1. Task

Homelab Grafana Explore on `prism-logs` cannot filter logs by Kubernetes
namespace / pod / container the way `kubectl logs` can — live series only expose
`job`, `format`, `stream`, `logtag`, etc. Option A: promote agent path metadata
into low-cardinality (bounded) string columns so the existing Loki-compat API
exposes them as stream labels. After merge, publish a prism release so Homelab
prod can consume the new labels.

## 2. Scope

- **In scope:**
  - `internal/parser/logs`: parse Kubernetes identity from `RawBatch.Source`
    (kubelet pod-log dir and containers symlink naming) and merge
    `namespace`, `pod`, `container` into every emitted row; also merge
    producer `RawBatch.Labels` with honor_labels semantics (record wins).
  - Never emit the raw file path as a label.
  - Unit tests for path parsing + row enrichment; Loki API test that
    `{namespace=…,pod=…,container=…}` matchers work when those columns are
    present in landed parquet.
  - Docs: `docs/STORE.md` (Loki label contract + cardinality guidance),
    `docs/OUTPUT_CONTRACT.md` (§3.2 optional K8s identity columns).
  - After `ALL_OK`: squash-merge PR, tag/publish next SemVer (minor:
    `v1.1.0`), close #126.
  - Homelab follow-up (orchestrator, after publish): grafana skill note +
    fleet review `group-1F.md` with LogQL verification (separate apps PR
    `cursor/prr-loki-labels-skill-8a90` if skill needs a commit).

- **Out of scope:**
  - Option B/C/D from the issue (Grafana derived fields, dual Loki, line prefixes).
  - Homelab monitor-agent / render-prism-config changes (paths already carry
    identity; enrichment is in the agent parser).
  - Structured metadata API, metric LogQL, full Promtail pipeline stages.
  - Labeling `node`, `uid`, or container ID.
  - Backfilling old parquet windows that lack the columns (new ingest only).

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Option A vs B/C/D? — A: **Option A** (user + issue recommendation).
- [x] Q: Label names? — A: **`namespace`, `pod`, `container`** (kubectl-aligned;
  finalized in STORE.md).
- [x] Q: Include `pod` despite Loki cardinality guidance? — A: **Yes** for
  product parity with kubectl; prism Loki is file-backed SQL (not a Loki
  index), and Homelab caps tailed files (`PRISM_LOG_PATH_MAX`). Document the
  trade-off; never label full paths / UIDs / request IDs.
- [x] Q: Where to enrich? — A: **`parser/logs` from `RawBatch.Source`** (+
  `RawBatch.Labels`), so every k8s-tailed pipeline benefits without store
  schema changes; Loki already promotes text columns to stream labels.
- [x] Q: SemVer after merge? — A: **`v1.1.0`** (backwards-compatible feature).

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- **Promote K8s identity as stream labels (Option A), not Grafana-only hacks:**
  - ref: https://grafana.com/docs/loki/latest/get-started/labels/bp-labels/ —
    use labels for namespaces / infrastructure identity; avoid unbounded values
    (paths, IDs).
  - perf: one path parse per RawBatch (not per line); three optional string
    columns; Loki matchers already push into DuckDB WHERE.
  - product: Explore `{namespace=…, pod=…}` matches kubectl mental model for
    all prism-store users, not only Homelab.

- **Parse both kubelet path layouts from `Source`:**
  - ref: https://kubernetes.io/docs/concepts/cluster-administration/logging/
    and kubelet layout `/var/log/pods/<ns>_<pod>_<uid>/<container>/<n>.log`
    plus `/var/log/containers/<pod>_<ns>_<container>-<id>.log` symlinks
    (https://k8s.guru/docs/observability/logs-output/).
  - perf: cheap string/path ops; no kube API calls; no-op when Source is not
    a k8s log path.
  - product: Homelab monitor-agent defaults to `/var/log/containers/*.log`
    and also uses `/var/log/pods/...` globs — both must work.

- **Keep `pod` as a label despite classic Loki pod-cardinality caution:**
  - ref: https://grafana.com/docs/loki/latest/get-started/labels/ — Loki OSS
    now prefers structured metadata for ephemeral pod names; prism's Loki
    adapter indexes nothing separately (STORE.md: labels = text columns).
  - perf: stream cardinality ≈ distinct (format × stream × ns × pod ×
    container) in SQL grouping only; Homelab file cap bounds active pods.
  - product: issue acceptance requires `{pod=…}` LogQL; omitting pod would
    fail kubectl parity. Document “do not label paths/UIDs”.

- **Honor_labels when merging path + RawBatch.Labels:**
  - ref: existing prometheus parser / RawBatch.Labels contract in
    `internal/data/batch.go`.
  - perf: merge only when keys absent on the row.
  - product: explicit producer labels and parsed line fields win over path
    inference.

## 5. Acceptance checklist  (developer checks these off)

- [ ] `parser/logs` extracts `namespace`/`pod`/`container` from kubelet
      pods + containers path forms on `RawBatch.Source`
- [ ] Path enrichment merges with honor_labels; non-k8s Source unchanged;
      raw path never becomes a column/label
- [ ] `RawBatch.Labels` merged into log rows (honor_labels)
- [ ] Loki API test: streams expose the three labels; LogQL matchers filter
- [ ] `docs/STORE.md` + `docs/OUTPUT_CONTRACT.md` document label contract +
      cardinality guidance
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
