# Spec: Ruler append-only alert_events (Grafana source of truth)

Status: IN_REVIEW

- **Slug / branch:** `cursor/ruler-alert-events-e55a`
- **Owner phase:** developer
- **PLAN phase(s):** alerting — persist firing/resolved transitions for Grafana

## 1. Task

Grafana home Open alerts / Last events today scan merge-time `mat_open_alerts` /
`mat_last_events`. Those files lag and can write false zeros. **prism-alert is
the only evaluator** (PromQL + `for:` → notifier webhooks). This change makes
the ruler **append one parquet row per firing or resolved transition** into
prism-store, exposed as `/sql` relation `alert_events`. Grafana (homelab-apps,
follow-up PR) will read that table. This prism PR does **not** change Grafana.

## 2. Scope

- **In scope:**
  - New ingest artifact `alert-events` (file-backed like logs, **not** metrics
    hot catalog, **not** under `logs/` so Recent logs stay clean).
  - Land path `<tenant>/alerts/alert-events/` (immutable parquet windows).
  - `/sql` sandbox view `alert_events` (empty → zero-row typed stub, no 5xx).
  - prism-alert: on ruler **pending→firing** and **firing→resolved** only,
    encode rows and `POST {STORE_BASE_URL}{ROUTE_PREFIX}/{TENANT_NS}/ingest/alert-events`.
  - Persist **even if** notifier webhook send fails. Do **not** persist
    unchanged evals or `repeat_interval` resends.
  - Docs: `docs/ALERTING.md`, `docs/STORE.md`, `docs/CONFIG.md`.
- **Out of scope:** Grafana dashboards, disabling `MATERIALIZATIONS_FILE`,
  homelab Helm pins, live-demo, changing `for:` / rule YAML, silences, a
  `GET /alerts` API on the ruler, fixing `QUERY_HOT_ONLY` empty instants.

## 3. Open questions

- [x] Q: Persist at dispatcher flush (after group_wait) or at ruler transition? —
  A: **Ruler transition.** Home = “ruler says firing”; email still waits
  `group_wait`. Operator-approved.
- [x] Q: Reuse logs artifact vs new plane? — A: **New `alerts/` plane.** Mixing
  into `logs` would pollute Grafana Recent logs.
- [x] Q: Disk from prism-alert? — A: **HTTP ingest to prism-cache** (same
  `STORE_BASE_URL` as PromQL). Ruler has no data volume.

## 4. Decision log

- **File-backed ingest, not mat_*:** Grafana already POSTs `/sql`. A first-class
  `alert_events` view avoids merge-time compaction hiding history (mat files
  share dest basenames and go `.compacted`).
  - ref: [STORE.md merge-time materializations](https://github.com/prism-utils/prism/blob/main/docs/STORE.md) — `mat_*` live files only.
  - perf: one small parquet POST per transition (rare vs 60s eval).
  - product: append-only history survives metrics merges.
- **Schema aligned with webhook identity:** `fingerprint` is the same 16-hex
  label fingerprint the v4 webhook uses, so Open/Last events can join episodes.
  - ref: [Alertmanager webhook](https://prometheus.io/docs/alerting/latest/configuration/#webhook_config)
  - perf: latest-row-per-fingerprint is a cheap DuckDB scan.
  - product: Open alerts = latest row per fingerprint with `status='firing'`.

## 5. Acceptance checklist  (developer checks these off)

- [x] Tests first (`test:` commit before implementation) — CONTRIBUTING.md §1
- [x] `POST /{ns}/ingest/alert-events` lands parquet under
      `<DATA_DIR>/<ns>/alerts/alert-events/` (empty body 204 no-op; unknown
      tenant 404; artifact not in ALLOWED_ARTIFACTS 404)
- [x] `/sql` `SELECT * FROM alert_events` works with **zero files** (empty
      typed relation, not 400)
- [x] `/sql` returns ingested rows with columns:
      `ts`, `fingerprint`, `alertname`, `severity`, `status`, `summary`,
      `description`, `recommendation`, `starts_at`, `ends_at`, `labels`
      (`status` is `firing` or `resolved`; `ends_at` null/zero while firing)
- [x] `alert_events` is **unaffected by `QUERY_HOT_ONLY`** (like `logs`)
- [x] Ruler appends **one row on pending→firing** and **one row on
      firing→resolved**; no row on repeated identical firing evals
- [x] Persist is independent of webhook `Send` success (both fail-open)
- [x] `docs/ALERTING.md` documents the ingest; `STORE.md` documents the
      artifact + SQL relation; `CONFIG.md` if new env is added (prefer none)
- [x] `make lint test` green; `make full-tests` if ingest/SQL wiring touched

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
  - `listAlertEventFiles` feeds `read_parquet` paths without `safeTenantParquetInRoots` (metrics/logs reject symlinks and paths outside tenant roots). Filter each listed file with that helper so `/sql` `alert_events` keeps the sandbox contract.
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md)
  - No test that a symlink under `<ns>/alerts/alert-events/` is omitted from `alert_events` (metrics has `TestSQLSymlinkParquetExcluded`). Add a cross-tenant link case: must not leak rows and must not 5xx/400 the sandbox.
- [x] **Gate 3 — Docs & comments match**
- [x] **Gate 4 — Comments are atomic** (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes
  - SQL listing skips the tenant-root/symlink filter used by metrics and logs; same fix as gates 1–2.

## 7. Reviewer notes

**Verdict: `CHANGES_REQUESTED`** — gates 1, 2, and the full REVIEW.md checklist fail on `/sql` listing safety. Gates 3 and 4 hold. Do not merge.

### Checks re-run by the reviewer

- History: `848fcb6` `test(store,alert):` before `68745eb` `feat(store,alert):` (then lint fix `84f7c3f`, atomic-comment `87c7ca9`). TDD contract holds.
- `make lint` → `0 issues`.
- `make test` (`go test -race -tags duckdb_arrow ./...`) → all packages `ok`.
- In-process `test/e2e` `TestAlert` → `ok`.
- `make full-tests` did **not** print `full-tests: OK`: integration compose failed to bind `:18080` (`kubectl` port-forward on this host) and docker e2e (`TestFormatMatrixHotMergeCombos`, `TestPromQLEndToEnd`) failed with `network deploy_default not found`. Those are environment collisions, not product failures from this diff. Not a gate-1/2 blocker.

### What holds

- Scope is one slice: ingest `alert-events`, land under `<tenant>/alerts/`, `/sql` `alert_events` stub + rows, ruler persist on pending→firing / firing→resolved only, fail-open vs webhook.
- Tests describe the behavior (empty 204, unknown tenant/artifact 404, hot-only independent, no logs pollution, no row on resend, persist with nil webhook sink, pending drop, encode round-trip, client fail-open).
- Docs (`ALERTING.md`, `STORE.md`, `CONFIG.md` `ALLOWED_ARTIFACTS`, `DESIGN.md` §15) match the delivered code. No new env. Comments are atomic after `87c7ca9`.
- No new deps; `internal/alert/events` is not a pipeline component (Factory N/A). slog, wrapped errors, no panics in library code.

### The blocking item

`internal/store/query/sql_alert_events.go` `listAlertEventFiles` does a bare `ReadDir` of `<root>/alerts/alert-events/*.parquet` and passes those paths into `UNION ALL read_parquet(...)`. Metrics and logs listing run every path through `safeTenantParquetInRoots`, which skips symlinks and anything outside allowed tenant roots (`TestSQLSymlinkParquetExcluded`). A symlink `evil.parquet` → another tenant's parquet under this new plane would be opened. Fix: filter with `safeTenantParquetInRoots` (using the same `AllowedTenantRoots` already computed) and add a test that plants a cross-tenant symlink under `alerts/alert-events/` and asserts `alert_events` does not include those rows (and `/sql` stays 200).

### Non-blocking

- `keep_firing_for`: persist is correctly after the keep-firing window (resolvedAt set only then); a table case would pin it but is not required to clear this review.
- `EventSink` POSTs synchronously inside `evalRule` (10s client timeout, fail-open). Rare vs eval cadence; do not expand scope.
