# Spec: ingest 5xx root causes + land-path latency (issues #115–#117)

Status: ALL_OK
<!-- one of: DRAFT | READY | IN_REVIEW | CHANGES_REQUESTED | ALL_OK -->

- **Slug / branch:** `fix/ingest-5xx-latency-slo`
- **Owner phase:** developer
- **PLAN phase(s):** Store ingest reliability (prod o11y triage Aug 2026)
- **Issues:** [#115](https://github.com/elk-utilities/prism/issues/115), [#116](https://github.com/elk-utilities/prism/issues/116), [#117](https://github.com/elk-utilities/prism/issues/117)

## 1. Task

Prod Traefik/admin o11y showed almost all 5xx as `POST 500` on prism-cache ingest. Root causes: (1) concurrent `logmeta.Bump` races on a shared `.meta_generation.tmp` (ENOENT rename → 500), (2) client disconnect / `unexpected EOF` mid-body mapped to 500 (pollutes availability + drives agent 5xx retries), (3) `finishLogLand` (Bump + SyncManifest + CarryLabelIndex) on the hot path under ~20 rps concurrent lands → p95 ~4–5s. Fix race + status mapping + serialize/offload finalize so ingest is correct and faster; ship as the next SemVer after merge.

## 2. Scope

- **In scope:**
  - `internal/store/logmeta/generation.go` (+ tests) — concurrency-safe `Bump`
  - `internal/store/ingest` — classify client-abort → HTTP **499**; docs in `docs/STORE.md`
  - `internal/store/engine` — treat `io.ErrUnexpectedEOF` / `context.Canceled` from body copy as client-abort (wrap or sentinel); **per-tenant serialize** around land finalize; **offload** SyncManifest + CarryLabelIndex after durable window write (generation bump stays before response or in the same serialized critical section so catalogs stay consistent — see Decision log)
  - `internal/output/http` — confirm/test that **499** is non-retryable (already true for 4xx≠429); add explicit test
  - STORE.md ingest status codes + note on concurrency-safe generation
- **Out of scope:**
  - Agent YAML timeout/backoff (homelab-apps#586)
  - Traefik gitops timeout audit (#1007)
  - Grafana SLO dashboard (homelab-apps#588)
  - Changing ingest RED metrics (still Traefik; see #113)
  - Full async “204 before durable write” (data loss risk)

## 3. Open questions  (must be empty/answered before `Status: ALL_OK`)

- [x] Q: Return 499 or 400 for client abort? — A: **499** (nginx/Cloudflare client-closed convention; Traefik already emits 499 on edge cancel; agent treats 4xx≠429 as permanent).
- [x] Q: Full async finalize vs serialize only? — A: **Serialize per-tenant land finalize** + keep durable file write on the request path; run `SyncManifest` + `CarryLabelIndex` inside the same per-tenant lock after `Bump` (ordered, no lost catalog). Do **not** return 204 before the window file is renamed/written. Optional micro-optimization: if profiling shows SyncManifest dominates, defer only index carry behind a singleflight with catch-up on next land — only if tests prove no permanent miss; default is sync under lock.
- [x] Q: Bump locking strategy? — A: **unique tmp via `os.CreateTemp`** + **process-local `sync.Mutex` keyed by tenant path** (or package-level map with cleanup) so rename never ENOENTs and increments are monotonic.

## 4. Decision log  (Decision Protocol — .ai/workflows/feature-loop.md)

- Client-abort status **499**:
  - ref: https://mailman.nginx.org/pipermail/nginx/2007-August/001581.html — NGX_HTTP_CLIENT_CLOSED_REQUEST 499
  - perf: negligible (branch on error type)
  - product: separates client cancel from server fault in Traefik RED; stops 5xx retry storms once agents see 4xx

- `Bump` unique tmp + mutex:
  - ref: Go `os.Rename` is atomic replace on same filesystem; concurrent shared `.tmp` is the classic lost-tmp race (ENOENT) — unique names are the standard fix (cf. write-temp-then-rename patterns in many stores)
  - perf: one mutex acquire per bump; CreateTemp is cheap vs parquet land
  - product: eliminates intermittent 500 under multi-agent fan-out

- Per-tenant serialize finishLogLand (not fire-and-forget 204):
  - ref: durability-before-ack for write APIs (e.g. object-store PUT semantics / “ack after durable”)
  - perf: reduces lock-convoy disk thrash under parallel lands; may not alone hit p95&lt;2s — agent pressure + dashboards land separately
  - product: no silent lag/miss for Loki after 204; correctness &gt; optimistic ack

## 5. Acceptance checklist  (developer checks these off)

- [x] `logmeta.Bump` concurrent stress test (many goroutines, one tenant) never errors; generation monotonic
- [x] Ingest handler: truncated/canceled body → **499** with short body; true engine/disk errors still **500**
- [x] Engine `LandLogWindow` / metrics ingest path maps client-abort copy errors so handler can classify them
- [x] Per-tenant serialize around land finalize (no concurrent `finishLogLand` for same tenant)
- [x] `output/http`: unit test that status **499** is permanent (no retry)
- [x] `docs/STORE.md` documents ingest status codes (incl. 499) + concurrency-safe generation bump
- [x] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [x] `make lint test` green locally (+ `make full-tests` if required by TESTING.md for this I/O path)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [x] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [x] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [x] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [x] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

Local `make lint test` green; acting reviewer under babysit — gates hold for #115–#117 scope.
