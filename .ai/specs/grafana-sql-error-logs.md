# Spec: SQL/Loki query error logs for Grafana canary

Status: ALL_OK

- **Slug / branch:** `cursor/grafana-canary-watch-004f`
- **Owner phase:** developer
- **Homelab epic:** https://github.com/prism-utils/homelab-apps/issues/750
- **Homelab child:** https://github.com/prism-utils/homelab-apps/issues/753
- **PLAN phase(s):** store observability (extends `#106` / `#113`)

## 1. Task

Grafana Infinity wraps prism `/sql` 400s as “400 Bad Request” with no DuckDB
body. `writeSQLErr` currently **does not log** for sandbox/user SQL 400s
(`errSandboxExec`, `errEmptySQL`, `errNonSelect`, `errMultiStatement`,
`errNoParquetSources`, deadline). Proxy logs were empty during the
2026-08-27 admin volume-panel incident.

RED counters already exist (`prism_store_query_requests_total{api,code,tenant}`,
`prism_store_query_errors_total{tenant,route,code_class}`). Do **not** invent
a parallel counter family. This change is **structured logs + a JSON 4xx
body** so a canary script and a fixing agent can see the engine error.

## 2. Scope

- **In scope:**
  - `writeSQLErr` (and Loki 4xx paths that currently swallow the engine error):
    log `ns`, HTTP status, truncated SQL/LogQL (cap ~512 bytes), `err`.
    Truncate so Grafana Search textbox content cannot dump unbounded log lines.
  - JSON error body on `/sql` 4xx/5xx: `{"error":"<engine or validation>"}`
    instead of plain `bad query` / `query failed` (keep status codes).
    Cap error string (~1KiB).
  - Tests first (`test:` commit before implementation).
  - Docs: one sentence in `docs/STORE.md` that `/sql` 4xx is JSON `{error}` and
    is logged with truncated SQL.
- **Out of scope:**
  - New Prometheus metric names (reuse `#106`/`#113`).
  - Homelab scrape / GitHub issue filing (sibling repo).
  - Changing successful 200 shape.
  - Merging to main (orchestrator will open a PR and **not** merge).

## 3. Open questions

- [x] Q: New counter with `reason`? — A: No. Existing
  `query_requests_total{api="sql",code="400"}` is enough; logs carry the reason.
- [x] Q: Log 404 unknown tenant? — A: Yes at debug/info is fine; Warn/Error
  reserved for 400 engine/validation and 5xx. 404 is probe/misroute noise.
- [x] Q: Merge? — A: **Do not merge.** Push branch + leave PR for orchestrator.

## 4. Decision log

- **Log 400s that today are silent.**
  - ref: https://github.com/prism-utils/homelab-apps/issues/753
  - perf: one slog line per failed query; truncate SQL to bound cost/PII.
  - product: Grafana UI is lossy; this is the only fixable error text.

- **JSON `{error}` body, same HTTP codes.**
  - ref: RFC 7807-ish; Grafana Infinity may still wrap it, but in-pod curl
    and the canary script can parse it.
  - perf: tiny.
  - product: do not change 200 `{columns,rows,...}`.

## 5. Acceptance checklist  (developer checks these off)

- [x] Failing test: POST `/sql` with invalid SQL returns 400 JSON `error` and
      a log/hook capturing truncated SQL + engine err (table-driven:
      parse/validation vs engine vs 404 tenant).
- [x] Loki 4xx similarly logs truncated LogQL + err (if the handler currently
      swallows it).
- [x] 200 responses unchanged.
- [x] Tests written first (a `test:` commit precedes implementation)
- [x] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns)

- [x] **Gate 1 — Follows the guidelines**
- [x] **Gate 2 — Tests cover edge cases**
- [x] **Gate 3 — Docs & comments match the task and the delivered code**
- [x] **Gate 4 — Comments are atomic**
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

Verdict: ALL_OK. Do not merge; leave PR 153 open.

- TDD history: `c14b46b test(store/query)` → `0e230b5 feat(store/query)` → `5bfe904 fix(store/query)`.
- Gate 1: store/query-only; slog; bounded truncate; no new counters; ctx plumbed on SQL `logQueryFailure`.
- Gate 2: table-driven validation / engine / 404 / 512-byte SQL+LogQL cap / 200 shape; existing empty-SQL and malformed-JSON 400 tests still hold.
- Gate 3: `docs/STORE.md` documents `/sql` JSON `{error}` + truncated SQL logs; 200 shape unchanged.
- Gate 4: new comments describe local caps/intent only (no other-file pointers).
- Checks: `make lint test` (0 issues; `./internal/store/query` `-count=1 -race` 36.5s ok); `make full-tests` OK (compose bind on `:18080` failed; integration+e2e still ok).

**DO NOT MERGE. DO NOT push to main.** Commit + push `cursor/grafana-canary-watch-004f` only.
