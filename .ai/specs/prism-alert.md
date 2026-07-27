# Spec: prism-alert — PromQL Ruler + Alertmanager-compatible notify

Status: IN_REVIEW
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `feat/prism-alert`
- **Owner phase:** orchestrator
- **Issue:** elk-utilities/prism#68
- **PLAN phase(s):** v2 alerting (monitor-agent v2 parity) — new binary alongside `cmd/prism-store`.

## 1. Task

v2 (prism) has no alerting today. Build **`prism-alert`**: a single per-tenant
**Ruler** that (1) loads the *same* Prometheus rule-group YAML v1 uses, (2)
evaluates each rule against prism-store's PromQL instant API at `time=<now>`,
(3) runs the `for:`/`keepFiringFor`/resolved state machine with `$value`/`$labels`
template expansion, and (4) groups alerts Alertmanager-style and POSTs the
**identical Alertmanager v4 webhook** to the existing notifier `/webhook`. The
notifier is the stable seam and is **not** modified — it already consumes this
payload. New binary `cmd/prism-alert`; reuses vendored
`github.com/prometheus/prometheus` `rules`/`promql`/`model/rulefmt`/`template`.
One instance per user namespace. No Alertmanager container; no PromQL write.

## 2. Scope

- **In scope:**
  - `cmd/prism-alert` (serve/version, env+flag config, `/healthz` + `/readyz`).
  - `internal/alert/config` — typed config + `Validate()` + env/flag load.
  - `internal/alert/ruler` — HTTP PromQL `QueryFunc` (reader JWT from mounted
    token file) + `rules.Manager` lifecycle; loads all YAML in `RULES_DIR`.
  - `internal/alert/notify` — Alertmanager-parity dispatcher (group_by,
    group_wait/group_interval/repeat_interval/resolve_timeout) + AM v4 payload
    builder + notifier webhook client (bearer, ≤256 KiB batching, bounded retry).
  - Packaging: signed multi-arch `ghcr.io/elk-utilities/prism-alert` image
    (goreleaser + `Dockerfile.alert.release`), Helm chart `deploy/charts/prism-alert`.
  - Tests: unit (fire-after-`for`, resolved, templating, grouping/repeat/resolve,
    bad expr, store-unreachable fail-open), golden payload vs notifier fixtures,
    e2e (real PromQL over prism-store → prism-alert fires → fake notifier receives).
  - Docs: DESIGN §16 ADR, CONFIG §15 env table, STORE cross-ref, `docs/ALERTING.md`.
- **Out of scope:** silences/inhibition; changes to the notifier; an Alertmanager
  container; PromQL write / remote_write; UI / silence API; multi-tenant fan-out
  in one process (one instance per namespace).

## 3. Open questions  (resolved before READY)

- [x] Q: Reuse Prometheus `rules.Manager` or a lean in-tree state machine? —
  A: **Lean in-tree** state machine that still reuses the canonical
  `model/rulefmt` (byte-compatible rule loading), `promql/parser` (expr
  validation), and `template` (`$value`/`$labels` expansion) packages.
  `rules.Manager` transitively imports `prometheus/notifier` →
  `prometheus/config`, which compiles **155** AWS/Azure/GCP/openapi packages
  into the binary — measured via `go list -deps`. That directly conflicts with
  the "memory efficient and secure" requirement for a stateless ruler that keeps
  no TSDB and talks to one notifier over one webhook. The lean set pulls **zero**
  cloud SDKs (same footprint prism-store already has). Semantics
  (`for`/`keep_firing_for`/resolve, resend delay, templating) faithfully mirror
  `rules/alerting.go`; `for`-state resets on restart (same as a single
  Prometheus without remote-write — acceptable for v1).
- [x] Q: Where does grouping/dispatch live? — A: In-tree `internal/alert/notify`
  (Prometheus ships the ruler state machine; Alertmanager's dispatch is a
  separate binary we do not run). Implement a compact, bounded dispatcher.
- [x] Q: Behavior when prism-store is unreachable / expr is bad? —
  A: **Fail-open = keep last state.** A query error fails that group's eval and
  is logged+counted; alert states are unchanged (no spurious resolve). This is
  the native `rules.Manager` behavior; asserted by test.
- [x] Q: Reader JWT handling? — A: Read `STORE_TOKEN_FILE` **fresh per request**
  (rotation-safe, projected SA / Vault), never from config/disk-in-config; empty
  token file ⇒ no `Authorization` header (RBAC-off stores).
- [x] Q: Health port default? — A: `LISTEN_ADDR` default `:8080` (matches store
  convention; chart maps a Service port).
- [x] Q: `receiver` field value? — A: `RECEIVER` env, default `tenant-webhook`
  (issue payload example).

## 4. Decision log  (Decision Protocol)

- **State machine / templating:** lean in-tree evaluator reusing canonical
  `rulefmt`/`parser`/`template`; NOT `rules.Manager`.
  - ref: Prometheus alerting-rule semantics `rules/alerting.go` (`for`,
    `keep_firing_for`, `$value`/`$labels`, `needsSending`/`ResendDelay`) —
    https://github.com/prometheus/prometheus/blob/main/rules/alerting.go ;
    dependency cost verified locally with `go list -deps` (155 cloud/openapi
    packages via `rules`→`notifier`→`config`).
  - perf: one instant query per rule per interval; active-alert map bounded by
    live series; single eval goroutine, no goroutine-per-alert; resend delay
    suppresses per-tick resends. Binary/attack surface unchanged from prism-store.
  - product: still loads v1's rule YAML unchanged (same `rulefmt`) and expands
    annotations identically (same `template`), while honoring the explicit
    memory-efficiency + security requirement.
- **Grouping/dispatch:** in-tree bounded aggregation groups keyed by `group_by`.
  - ref: Alertmanager dispatch (route knobs `group_by`, `group_wait`,
    `group_interval`, `repeat_interval`) —
    https://prometheus.io/docs/alerting/latest/configuration/#route
  - perf: one `aggrGroup` per distinct `group_by` label tuple, each holding a map
    of active alerts (bounded by live series); a single timer per group; flushes
    coalesce into one webhook (≤256 KiB batches). No unbounded queue.
  - product: matches v1's Alertmanager route knobs exactly, so the same rules
    notify with the same batching/cadence tenants already rely on.
- **PromQL client:** HTTP instant query with `time=<now>` against
  `{ROUTE_PREFIX}/{ns}/api/v1/query`, JSON envelope → `promql.Vector`.
  - ref: Prometheus HTTP API instant query response format —
    https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
  - perf: streams the JSON body via `encoding/json` decoder; one bounded
    `http.Client` with timeout; reuses connections.
  - product: prism-store already serves this exact API (PR #65); "rule eval at
    now" uses the documented `time=` param.
- **Webhook retry:** bounded exponential backoff on transport/5xx via
  `github.com/cenkalti/backoff/v4` (already a dependency).
  - ref: prism `output/http` retry pattern (DESIGN §9) + backoff lib.
  - perf: capped elapsed time so a down notifier never blocks a group forever.
  - product: notifier returns 502 when all destinations fail; a bounded retry
    smooths transient blips without unbounded buffering.
- **Packaging:** pure-Go `CGO_ENABLED=0` binary (no DuckDB), distroless nonroot
  image, mirrors the `prism` agent supply chain (Trivy/SBOM/cosign via goreleaser).
  - ref: existing `.goreleaser.yaml` prism-agent build + `Dockerfile.release`.
  - perf: static binary, scratch-friendly, minimal attack surface.
  - product: parity with #15/#29 release engineering; one-per-tenant deployable.

## 5. Acceptance checklist  (developer checks these off)

- [x] `internal/alert/config`: `Config` (json tags), `DefaultConfig()`,
      `Validate()` (path-named errors), env+flag loader; secrets via `${ENV}`.
- [x] `internal/alert/ruler`: HTTP PromQL `QueryFunc` (token read fresh per
      request; `time=` now); **lean in-tree state machine** (see §3/§4 decision —
      not `rules.Manager`) reusing `rulefmt`/`parser`/`template`; loads every
      `*.yml`/`*.yaml` in `RULES_DIR`, `EVALUATION_INTERVAL` default 60s.
- [x] `internal/alert/notify`: dispatcher with `group_by/group_wait/group_interval/
      repeat_interval/resolve_timeout`; AM **v4** payload (version, groupKey,
      status, receiver, groupLabels, commonLabels, commonAnnotations, externalURL,
      alerts[] with fingerprint/generatorURL/startsAt/endsAt); resolved sent.
- [x] Webhook client: `Authorization: Bearer <WEBHOOK_SECRET>`, JSON POST,
      ≤256 KiB batches, bounded retry, `context` plumbed.
- [x] `cmd/prism-alert`: `serve` (default) + `version`; `/healthz`+`/readyz`;
      graceful shutdown; slog JSON.
- [x] Unit tests: firing-after-`for`, resolved transition, `$value`/`$labels`
      templating, grouping + repeat + resolve, bad expr routed (not fatal),
      store-unreachable fail-open (keep last state).
- [x] Golden test: emitted payload matches notifier `AlertmanagerWebhook` shape.
- [x] E2E (`test/e2e`, tag `e2e`): the canonical `promql` engine evaluates a
      **real PromQL expression** (`up == 1`) over an in-memory `storage.Queryable`
      serving the store's `/{ns}/api/v1/query` shape; the full ruler → dispatcher
      → v4 webhook chain fires into a real notifier receiver (bearer verified),
      covering firing→resolved. In-memory store (not `teststorage`/`promqltest`)
      keeps `go.mod` cloud-SDK-free.
- [x] Packaging: goreleaser `prism-alert` build + docker + manifest + sign;
      `Dockerfile.alert.release`; Helm chart under `deploy/charts/prism-alert`
      (workload, Service, probes, non-root securityContext, resources, env as
      values, optional NetworkPolicy, rules ConfigMap); golden render test + CI wiring.
- [x] `prism-alert version` wired to release tag (`internal/version`).
- [x] Docs: DESIGN ADR §15, CONFIG env table §15, STORE cross-ref, TESTING note,
      `docs/ALERTING.md` versioned contract page.
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally (+ `make full-tests` — I/O + wiring touched)

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

First automated review (bugbot) — addressed:

- **Fixed:** `keep_firing_for` had no test → added a deterministic
  `evalRule`-driven unit test (hold-then-resolve). `evalAll` now checks `ctx`
  between rules so shutdown does not drain every remaining rule through a slow
  store. Comments + `docs/ALERTING.md` now state delivery semantics honestly
  (best-effort, bounded in-request retry); the chart documents that an external
  `rulesConfigMap` update needs a restart (only inline `rules` stamps a checksum).

Second automated review (security-review) — addressed:

- **Fixed:** disabled the template `query` function (no per-series PromQL
  amplification); both HTTP clients refuse redirects (`CheckRedirect`) so the
  reader JWT / webhook bearer never follow a redirect to an attacker host;
  capped the webhook response drain with `LimitReader`; `validateAbsURL` now
  requires `http(s)` and rejects embedded URL credentials, and no longer echoes
  raw URLs in errors; template-expansion failures ship a generic `<template
  error>` (detail only logged); `WebhookSecret` is `json:"-"`;
  `automountServiceAccountToken: false` on the pod.

Accepted tradeoffs (deliberate for the lean, no-TSDB v1 ruler; documented):

- **SSRF hardening is scheme/userinfo-level only.** Private-IP/metadata egress
  is not blocked at the app: `STORE_BASE_URL`/`NOTIFIER_WEBHOOK_URL` are
  operator-provided in-cluster service URLs. Use an egress NetworkPolicy for
  network-level restriction (chart ships ingress-only, matching prism-store).
- **`STORE_TOKEN_FILE` path is not allowlisted** and `WEBHOOK_SECRET` is env-
  injected (matches prism-store's `INGEST_TOKEN`/`ADMIN_TOKEN` convention);
  both are operator-controlled. Custom CA / mTLS to the store is a later
  enhancement (default system-root TLS today).

- **Best-effort delivery, no durable queue.** A `Send` retries transient
  failures with bounded backoff; a firing group re-notifies on change or
  `repeat_interval`, but a resolve dropped after retries is not re-sent, and
  multi-chunk delivery has no cross-chunk rollback. Front the notifier with a
  durable receiver if at-least-once resolve is required.
- **Rules compiled once at startup;** `ResendDelay` is coupled to
  `EVALUATION_INTERVAL`; `/readyz` reports ready once serving (no dependency
  probe) — all fine for a stateless single-tenant ruler and can be lifted later.
- **Unbounded active-alert map** by design (bounded by live series); size the
  pod for rule cardinality (chart defaults documented).
