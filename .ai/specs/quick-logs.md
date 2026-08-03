# Spec: `prism run --quick logs` + minimal logs support in `prism-store`

Status: READY

- **Slug / branch:** `feat/quick-logs`
- **Owner phase:** orchestrator
- **PLAN phase(s):** agent CLI + store consumer (logs)

## 1. Task

Add a zero-config `prism run --quick logs` that reads log lines from **stdin**,
mines `template → count`, and prints the result to **stdout** (pure-Go, no
dependencies). When a store is configured (`--store`), the same window is
**also** shipped to `prism-store` as a `logs-summary` Parquet artifact, and the
store gains **minimal logs support** (land-as-file ingest + a `logs` relation on
`POST /{ns}/sql`) so "logs are available on prism-store now" is a real,
queryable end state. Local (native stdout) and remote (ship + availability log)
are **not exclusive**. No agent-side sampling — the user caps input with
`head`/`tail`. Shipped **together**: the store can query logs end-to-end.

## 2. Scope

- **In scope:**
  - Agent: `internal/quick` preset builder + `--quick`, `--store`, `--tenant`,
    `--token`, `--print-config` flags on `prism run` (wired in `cmd/prism`).
  - Store: land `logs-*` artifacts as files under `<tenant>/logs/<artifact>/`
    (no metrics hot-table transform); expose a `logs` relation to `/sql` via
    `read_parquet([...], union_by_name=true)`.
  - Docs: `docs/CONFIG.md` (`--quick`), `docs/STORE.md` (logs relation).
  - `e2e` proving stdin → agent → store ingest → `/sql` returns template counts.
- **Out of scope:** local display through prism-store (native chosen); logs in
  the metrics hot catalog / tiering / rollups / retention / PromQL; a `metrics`
  quick preset; any change to the frozen metrics schema, its view, or its guard.

## 3. Open questions — resolved

- [x] Local display → **agent-native** (pure-Go summary → stdout; agent stays `CGO_ENABLED=0`).
- [x] Template scope → **logs only**.
- [x] Packaging → **together**: store logs path lands with the CLI, e2e green.

## 4. Decision log

- **Local display = agent-native, not store-backed.**
  - ref: `docs/DESIGN.md` §15 — agent is `CGO_ENABLED=0`, must not import `internal/store`/DuckDB; it may act only as an HTTP client to a store.
  - perf: zero extra process; reuses the `summary` processor over one Arrow window — flat memory, instant.
  - product: honors "single static binary, no external runtime deps"; `my-app | prism run --quick logs`.
- **Store reads logs Parquet with `union_by_name=true` (separate `logs` relation).**
  - ref: [DuckDB — Reading Parquet Tips](https://duckdb.org/docs/current/data/parquet/tips); [UNION ALL BY NAME](https://duckdb.org/2025/01/10/union-by-name.html) — unifies files with different/missing columns, NULL-filling.
  - perf: metadata-scan-bound ([duckdb#8018](https://github.com/duckdb/duckdb/issues/8018)); acceptable on local disk and now parallel; bounded by `/sql` sandbox caps.
  - product: `logs-*` add per-format string columns (`OUTPUT_CONTRACT.md` §3.2–3.4), so name-based unification is the evolution-safe consumer pattern; the metrics `AssertNoUnionByName` guard stays intact on its own path.
- **In-memory preset instead of a shipped YAML file.**
  - ref: OTel Collector / Benthos factory model (`DESIGN.md` §4) — a preset is a pre-filled `config.Config` fed to the existing validated `pipeline.Build`.
  - perf: no file I/O; reuses the validated builder path.
  - product: one schema / one builder; `--print-config` dumps the equivalent config so users can graduate to a file.
- **Logs artifacts are opt-in on ingest (`ALLOWED_ARTIFACTS`), default unchanged.**
  - ref: `docs/STORE.md` ingest validation chain (unknown artifact → `404`).
  - product: existing metrics-only deployments untouched; operators enable logs explicitly.

## 5. Acceptance checklist

- [ ] `internal/quick` builds a `logs` preset (stdin → logs{auto} → 2s buffer → template+summary → json/stdout) that passes `config.Validate` and `pipeline.Build`.
- [ ] `--store` adds a `logs-summary` Parquet ship branch alongside the stdout branch (both run); agent logs a one-line "available on prism-store" hint; agent never queries remote.
- [ ] `--quick` + `-config` together is a clear error; `--print-config` dumps the preset and exits.
- [ ] Store lands `logs-raw|logs-template|logs-summary` as files under `<tenant>/logs/<artifact>/`; metrics path unchanged.
- [ ] `/sql` sandbox exposes a `logs` view (`union_by_name=true`) tenant-scoped, reusing existing row/time/memory caps; empty tenant answers zero rows; `metrics` view + `AssertNoUnionByName` untouched.
- [ ] `e2e`: stdin → agent → store ingest → `POST /{ns}/sql` returns the template counts.
- [ ] Docs updated (`CONFIG.md` `--quick`; `STORE.md` logs relation).
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1.
- [ ] `make lint test` green (+ `make full-tests` — touches I/O, encoding, wiring).

## 6. Mandatory review gates

- [ ] **Gate 1 — Guidelines** (CONTRIBUTING.md + DESIGN.md): agent stays `CGO_ENABLED=0`; leaf packages don't cross the store boundary.
- [ ] **Gate 2 — Edge cases**: empty stdin, non-log lines, `--quick`+`-config` conflict, unknown template, store unknown-artifact/tenant, `union_by_name` over a zero-file tenant, cancellation/drain.
- [ ] **Gate 3 — Docs & comments match delivered code** (no drift).
- [ ] **Gate 4 — Comments atomic** (CONTRIBUTING.md §3.8).
- [ ] Full docs/REVIEW.md checklist passes.

## 7. Reviewer notes

_(empty until first review)_
