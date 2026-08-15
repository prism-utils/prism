# Spec: Cancel abandoned read requests (HTTP 499)

Status: CHANGES_REQUESTED
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `cursor/query-client-cancel-c2b4`
- **Owner phase:** orchestrator
- **PLAN phase(s):** store query (post-v1.0.2) — Grafana-style dashboard fan-out
- **Ships as:** `v1.1.0` (backward-compatible feature; latest tag `v1.0.2`)

## 1. Task

When a client gives up on a read (Grafana panel refresh cancelled, browser
navigates away, proxy 499), prism-store must **stop the in-flight work** — DuckDB
sandbox query, PromQL evaluation, Loki scan, queued waiter — instead of running
the query to completion for nobody. A dashboard with many visualizations that
refresh, cancel, and refresh again otherwise stacks abandoned sandboxes and
becomes a self-DoS against the read queue and DuckDB memory cap.

The mechanism is a **trap**, not a new scheduler: Go already cancels
`http.Request.Context()` when the client goes away; go-duckdb already calls
`duckdb_interrupt` when that context is cancelled. This task wires that cancel
through every read path, returns **499** (nginx / Grafana / Loki convention)
instead of 400/502/503, and must not add allocations or extra goroutines on the
successful query path.

## 2. Scope

- **In scope:**
  - `POST /{ns}/sql` (JSON + Arrow) — `context.Canceled` → **499** `client closed` (today: 400 `bad query`)
  - PromQL read API — HTTP **499**, Prometheus envelope `errorType=canceled` (today: 503)
  - Loki read API — HTTP **499** (today: 503)
  - `GET /{ns}/query` — **499** on cancel (today: 500 `query failed`)
  - Read-queue waiter that sees `r.Context().Done()` — **499**, not 429; no `Retry-After`; still observe `RejectClientCanceled` and count `rejectedTotal`
  - Cluster coordinator `ReverseProxy` `ErrorHandler` — `context.Canceled` → **499**, not 502
  - Shared leaf helper so ingest's existing 499 and the new read 499 use one status/body
  - Docs: `docs/STORE.md` (SQL / PromQL / Loki / queue / admin queue snapshot); no new flags
  - Tests that prove a long DuckDB query **stops promptly** when the request context is cancelled (not merely that the handler returns an error after the query finishes)
- **Out of scope:**
  - Changing timeout behaviour (`context.DeadlineExceeded` stays SQL 400 / PromQL+Loki 503)
  - Cancelling `ExportHotSnapshot` from the request context (singleflight; shared across concurrent reads — a cancelled panel must not abort a snapshot another query is waiting on). After the snapshot returns, if the request ctx is already done, skip the sandbox query and 499
  - Ingest body-abort (already 499)
  - New env vars, new goroutine-per-request watchers, query-id registries, CloseNotifier
  - Homelab image pin (orchestrator after this tag publishes)

## 3. Open questions  (must be empty/answered before `Status: READY`)

- [x] Q: Custom cancel registry vs context trap? — A: **Trap.** `r.Context()` + existing `QueryContext` / `ExecContext` / PromQL `Exec`. No new per-request goroutine on the success path.
- [x] Q: 499 vs Prometheus 503 `canceled`? — A: **HTTP 499** for every read (Grafana proxy, Loki, nginx). PromQL JSON keeps `errorType=canceled`. The client that cancelled will not consume the body; 499 is for logs and `http_requests_total`.
- [x] Q: Queue wait-cancel stays 429? — A: **No — 499.** 429 is backpressure for a live client. A gone client is not "too many requests".
- [x] Q: Cancel hot snapshot? — A: **No.** Check `ctx.Err()` before starting a snapshot and after it returns; do not pass the request ctx into the export.
- [x] Q: SemVer? — A: **`v1.1.0`** after merge (new observed status + interrupt behaviour, backward compatible).

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Request-context trap (no query registry / CloseNotifier):
  - ref: https://pkg.go.dev/net/http#Request.Context — incoming server context is cancelled when the client connection closes or the request is cancelled (HTTP/2). CloseNotifier is deprecated in favour of this.
  - ref: https://github.com/marcboeker/go-duckdb/pull/143 — go-duckdb watches `ctx.Done()` and calls `duckdb_interrupt` so background DuckDB threads stop; `QueryContext`/`ExecContext` already used on sandbox SQL.
  - perf: zero extra allocations on the success path; interrupt cost is paid only when the client is gone. A registry or extra watcher goroutine per request would add occupancy under Grafana fan-out, which is the load we are relieving.
  - product: same model as Loki queriers and Grafana's datasource proxy (cancelled → 499, not 5xx).

- HTTP 499 for client-gone reads, timeouts unchanged:
  - ref: https://grafana.com/docs/loki/latest/query/troubleshoot-query/ — Loki documents client cancel as HTTP 499. Grafana PR 47473 maps aborted datasource proxy requests to 499 instead of 502.
  - perf: status mapping is a branch on an error already returned; no hot-path cost.
  - product: 499 is not a server fault (agents already treat ingest 499 as non-retry). Mapping cancel to 400/502/503 makes abandoned refreshes look like failures and can trip retries/alerts.

- Do not cancel in-flight hot snapshot from the request ctx:
  - ref: existing `exportGroup` singleflight on `ExportHotSnapshot` — one export per tenant coalesces a dashboard blast.
  - perf: snapshot is shared; killing it wastes work other waiters need. Skipping the sandbox after `ctx.Err()` is the cheap check.
  - product: cancelled panel does not poison concurrent panels of the same tenant.

- Queue wait-cancel → 499 without Retry-After:
  - ref: limiter already `select`s `r.Context().Done()` (sql-queue spec); Grafana/Loki 499 for client gone.
  - perf: same select; only the response status/body change. Slot was never held.
  - product: `rejectedTotal{reason=client_canceled}` stays the operator signal; HTTP status matches the cause.

## 5. Acceptance checklist  (developer checks these off)

- [x] Shared helper (tiny leaf package under `internal/store/`) exports status **499**, body `client closed`, and `IsCanceled(err)` (`context.Canceled` and existing ingest abort sentinels; **not** `DeadlineExceeded`). Ingest write path uses it (no behaviour change for ingest tests).
- [x] SQL JSON + Arrow: client-cancelled request → 499 `client closed`; timeout still 400; long `generate_series` query interrupted promptly (same bound as `TestSQLTimeoutInterrupts`: finishes well under the full query, not hung until DuckDB completes).
- [x] PromQL: cancelled eval → HTTP 499 + `errorType=canceled`; timeout still 503/`timeout`.
- [x] Loki: cancelled scan → HTTP 499; timeout still 503.
- [x] `GET /{ns}/query`: cancelled execute → 499.
- [x] Queue: waiter whose context is cancelled gets **499** `client closed`, **no** `Retry-After`, does not hold an inflight slot; full-queue and wait-timeout still **429** + `Retry-After: 1`. Observer still records `RejectClientCanceled`.
- [x] Cluster reverse proxy: cancelled inbound request → 499 (not 502); upstream handler sees its context cancelled (slow upstream must not run to completion).
- [x] After `ExportHotSnapshot` returns, a cancelled request does not open a sandbox / run user SQL (499). Snapshot itself is not tied to the request ctx.
- [x] `docs/STORE.md` updated: SQL cancel vs timeout; PromQL/Loki tables; queue client-cancel is 499 not 429; admin `/admin/queue` text no longer says every reject is a 429.
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (`make full-tests` not required: no new I/O stack / encoding / compose wiring)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
  - `lokiHandler.execError` comment still says a cancelled query is reported as unavailable; that function now returns 499. Update the comment so cancel vs timeout match the delivered statuses (499 vs 503).
- [x] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes
  - Blocked on Gate 3 (stale `execError` comment). Re-check after that comment matches 499 cancel vs 503 timeout.

## 7. Reviewer notes

<!-- Reviewer appends one actionable line under any gate it unchecks. Set
     Status: ALL_OK only when every box above is checked; otherwise
     Status: CHANGES_REQUESTED. -->

- **Verdict: CHANGES_REQUESTED** (Gate 3 + full checklist).
- History `origin/main..HEAD`: `test(store):` → `feat(store):` → `docs(store):` (TDD contract holds).
- `make lint`: 0 issues. `make test`: all store packages green (`httperr`, `ingest` including `TestIngestClientAbortReturns499`, `query`, `queue`, `cluster`). `internal/e2e.TestE2E_LoggingThreePhaseParquet` fails here and on `origin/main` (unrelated logging tailer; not this branch) — not a Gate 2 fail.
- Particulars: no extra success-path goroutine; SQL timeout still 400 / PromQL+Loki 503; queue waiter 499 without Retry-After; `ExportHotSnapshot` not bound to request ctx + post-export `ctx.Err()` skip; DuckDB interrupt elapsed-bound (`TestSQLClientCancelInterruptsLongQuery`, same 5s cap as `TestSQLTimeoutInterrupts`); `httperr.IsCanceled` does not treat `DeadlineExceeded` as cancel; public `docs/STORE.md` has no secrets.
- New comments are atomic. Pre-existing `//nolint:contextcheck` “below/above” wording was not introduced by this change.
