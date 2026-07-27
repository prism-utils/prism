# Spec: prism-alert — PromQL Ruler + Alertmanager-compatible notify

Status: READY
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

- [x] Q: Reuse Prometheus `rules.Manager` or hand-roll the state machine? —
  A: **Reuse** `rules.Manager` (issue mandates it; canonical `for`/`keepFiringFor`
  + `$value`/`$labels` templating). Provide no-op `storage.Appendable`/`Queryable`
  (we don't persist ALERTS series; `for`-state resets on restart, same as a
  single Prometheus without remote-write — acceptable for v1).
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

- **State machine / templating:** reuse `prometheus/prometheus/rules.Manager`.
  - ref: Prometheus rules engine `manager.go`/`alerting.go` (`ManagerOptions`,
    `QueryFunc`, `NotifyFunc`, `ForGracePeriod`, `ResendDelay`) —
    https://github.com/prometheus/prometheus/blob/main/rules/manager.go
  - perf: per-group eval is bounded (one instant query per rule per interval);
    no per-sample heap churn beyond the engine's own vector; no goroutine per
    alert. Manager owns `ResendDelay` so we don't resend every tick.
  - product: byte-identical `for`/`keepFiringFor` semantics and `$value/$labels`
    templating as upstream Prometheus — the whole point of "load v1 rules unchanged".
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

- [ ] `internal/alert/config`: `Config` (json tags), `DefaultConfig()`,
      `Validate()` (path-named errors), env+flag loader; secrets via `${ENV}`.
- [ ] `internal/alert/ruler`: HTTP PromQL `QueryFunc` (token read fresh per
      request; `time=` now), `rules.Manager` wired with no-op appendable/queryable,
      loads every `*.yml`/`*.yaml` in `RULES_DIR`, `EVALUATION_INTERVAL` default 60s.
- [ ] `internal/alert/notify`: dispatcher with `group_by/group_wait/group_interval/
      repeat_interval/resolve_timeout`; AM **v4** payload (version, groupKey,
      status, receiver, groupLabels, commonLabels, commonAnnotations, externalURL,
      alerts[] with fingerprint/generatorURL/startsAt/endsAt); `send_resolved:true`.
- [ ] Webhook client: `Authorization: Bearer <WEBHOOK_SECRET>`, JSON POST,
      ≤256 KiB batches, bounded retry, `context` plumbed.
- [ ] `cmd/prism-alert`: `serve` (default) + `version`; `/healthz`+`/readyz`;
      graceful shutdown; slog JSON.
- [ ] Unit tests: firing-after-`for`, resolved transition, `$value`/`$labels`
      templating, grouping + repeat + resolve, bad expr routed (not fatal),
      store-unreachable fail-open (keep last state).
- [ ] Golden test: emitted payload matches notifier `AlertmanagerWebhook` shape.
- [ ] E2E (`test/e2e`, tag `e2e`): real exporter → agent → prism-store →
      prism-alert fires on a **real PromQL expression** → fake notifier receives
      the v4 webhook (bearer verified).
- [ ] Packaging: goreleaser `prism-alert` build + docker + manifest + sign;
      `Dockerfile.alert.release`; Helm chart under `deploy/charts/prism-alert`
      (workload, Service, probes, non-root securityContext, resources, env as
      values, optional NetworkPolicy egress → store + notifier); golden render test.
- [ ] `prism-alert version` wired to release tag (`internal/version`).
- [ ] Docs: DESIGN ADR, CONFIG env table, STORE cross-ref, `docs/ALERTING.md`
      versioned contract page.
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally (+ `make full-tests` — I/O + wiring touched)

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code**
- [ ] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
