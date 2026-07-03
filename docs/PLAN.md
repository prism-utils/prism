# prism — Build Plan

> A phased, **test-first** roadmap. Each phase is independently mergeable,
> leaves `main` green, and ends with concrete acceptance criteria. Do not start
> a phase before the previous one's criteria are met. Every phase follows the
> TDD loop in [`CONTRIBUTING.md`](../CONTRIBUTING.md): **failing test → minimal
> code → refactor**, with a test-only commit first.

Legend: each phase lists **Deliverables**, **Tests written first**, and
**Done when** (acceptance criteria a reviewer checks against [`REVIEW.md`](REVIEW.md)).

---

## Revision 2026-07 — target: working metrics + logging e2e

The build is re-scoped toward two concrete end-to-end paths (see
[`DESIGN.md`](DESIGN.md) §1 "In scope"). Deltas vs. the original phases below:

- **Topology change:** config is now a **list of pipelines**, each running in
  its **own worker** (one worker per input); each pipeline **fans out** after an
  **accumulation buffer** into a `data` branch (→parquet→file) and a `summary`
  branch (→json→file). `DESIGN.md` §6/§6.1/§7 are authoritative.
- **Prometheus scrape input** is pulled from "Deferred" into scope (Phase 3).
- **`summary`** emits aggregate rows encoded as **JSON** (`[{…}]`); prism embeds
  **no SQL** (storage/query is server-side).
- **`template`** = log-template mining (lessence, Drain-style fallback).
- **Dropped from this cut:** `ml` (Phase 6) and the entire scripted processor
  (Phase 7) — deferred, additive later via the registry.
- Each item is tracked as a spec under `.ai/specs/` and driven through the loop
  individually; [`../TASKS.md`](../TASKS.md) is the ordered tracker.

---

## Phase 0 — Foundation & tooling  *(this repo, mostly done)*

**Deliverables**
- Module, `.gitignore`, `.editorconfig`, `.golangci.yml`.
- `Makefile` (`build`, `test`, `full-tests`, `lint`, `bench`, `tidy`, `cover`).
- `Dockerfile` (multi-stage, static, non-root) + `deploy/` (compose, systemd).
- CI workflow running `make lint` + `make test` on every PR.
- Docs: DESIGN, PLAN, CONTRIBUTING, TESTING, REVIEW.
- Pin the Phase-1 dependency subset with `go get` (never hand-edit versions).

**Tests written first** — a `make test` that runs and passes on an empty
package; a CI job that fails on lint violation (prove the gate works).

**Done when**: `make lint test` is green in CI on a trivial package; all docs
present; no invented dependency versions.

---

## Phase 1 — Core contracts: component, registry, config, Host

**Deliverables**
- `internal/component`: the `Component`, `Input`, `Parser`, `Processor`,
  `Encoder`, `Output`, `Factory[T]`, `Host`, `Settings` interfaces.
- `internal/component.Registry` with `Register*`/`Lookup*` per kind.
- `internal/config`: typed `Config` tree + loader (yaml/json/env) + `Validate()`
  contract + path-accurate error messages.
- `obs`: `slog` logger + no-op metrics used by `Host`.

**Tests written first**
- Registry: register/lookup/duplicate-type-error/unknown-type-error.
- Config: load YAML == load equivalent JSON; env interpolation; `Validate`
  rejects bad values and names the path; unknown top-level key errors.

**Done when**: registry + config are fully covered, no runtime yet, and a fake
component can be registered and looked up in a test with zero production wiring.

---

## Phase 2 — Data model (Arrow) & the pipeline runtime

**Deliverables**
- `internal/data`: `RawBatch`, `RecordBatch` (Arrow-backed), `EncodedBlock`,
  allocator/ownership helpers, bounded-batch constructors.
- `internal/pipeline`: builder (config → wired stages) + staged bounded-channel
  runtime + `errgroup` lifecycle + graceful drain + failure policy hook.

**Tests written first**
- Runtime with **fake** input/parser/processor/encoder/output: data flows in
  order; EOF drains cleanly; ctx-cancel stops within deadline; **`goleak`**
  asserts no leaked goroutines; allocator balance asserted (no buffer leak).
- Backpressure: a blocking fake output stalls the input (bounded channel proof).

**Done when**: an all-fakes pipeline runs end-to-end from a config string, drains
on EOF and on cancel, leaks no goroutines/buffers, and honors processor order.

---

## Phase 3 — Inputs

**Deliverables**: `input/stdin`, `input/file` with `mode: batch` (read whole
file, emit bounded RawBatches, exit) and `mode: tail` (follow + rotation via
`nxadm/tail`).

**Tests written first**
- stdin: piped bytes → expected RawBatches → channel closes at EOF.
- file/batch: fixture file → N bounded batches → exit; empty file → clean exit.
- file/tail: appended lines observed; **rotation** (rename+recreate) does not
  drop/dup lines; constant memory over a large synthetic append.

**Done when**: all three input modes pass, tail survives rotation, memory is flat
under a streaming benchmark.

---

## Phase 4 — Parsers & field auto-discovery

**Deliverables**: `parser/json`, `parser/logfmt`, `parser/regex`, and
schema **auto-discovery** (infer columns/types from first N records, evolve
safely). Row→Arrow conversion lives here.

**Tests written first**
- Golden fixtures per format: raw → expected Arrow schema + column values.
- Auto-discovery: mixed/late fields → schema evolves without dropping data;
  type conflicts resolve deterministically (documented precedence).
- **Fuzz** the parsers: never panic; malformed input → error routed, not crash.

**Done when**: parsers are golden-tested + fuzzed clean; auto-discovery handles
schema evolution deterministically.

---

## Phase 5 — Parquet encoder & outputs

**Deliverables**: `encoder/parquet` (Arrow→Parquet, compression + row-group
config) and `encoder/raw`; `output/stdout`, `output/file` (rotation + atomic
rename), `output/http` (binary POST + backoff retry + give-up).

**Tests written first**
- Parquet round-trip: encode a known batch → **read it back** (arrow reader) →
  assert schema + values + compression; assert buffers released.
- file output: rotation by size/time; atomic rename; no partial files visible.
- http output: httptest server asserts body bytes + headers; retry on 5xx with
  bounded backoff; give-up after max attempts returns a typed error.

**Done when**: Parquet round-trips faithfully, http ret/retry proven against a
test server, file output rotates atomically, all buffers released.

---

## Phase 6 — Built-in compiled processors

**Deliverables**: `processor/summary` (windowed group-by aggregates over Arrow
columns), `processor/ml` (behind a `Detector` interface; pure-Go stat detector
first), `processor/template` (wrap `air-gapped/lessence`), with `enabled` toggles.

**Tests written first**
- summary: deterministic aggregates (count/sum/avg/p95) for fixture batches +
  window boundaries; empty group handling.
- ml: `Detector` contract test + a golden stat-detector case; `enabled:false`
  is a pass-through (identity) — proven.
- template: lessence normalization golden cases; toggle off = identity.

**Done when**: each built-in has deterministic golden coverage, toggles are true
no-ops when disabled, and none allocate per-record on the hot path (bench).

---

## Phase 7 — Scripted processor

**Deliverables**: `processor/script` with `ScriptEngine` interface; Starlark
(default) + expr engines; wazero WASM engine stub with its contract test.

**Tests written first**
- Engine contract test reused across engines (same input → same shaped output).
- Starlark: shape/drop/derive-field scripts; sandbox denies filesystem/network;
  a runaway script is bounded (step/time limit) and errors, not hangs.
- expr: predicate + derived field cases.

**Done when**: engines pass the shared contract; sandbox + resource bounds proven;
a bad script fails safe (routed error, no crash, no hang).

---

## Phase 8 — Assembly, packaging, e2e

**Deliverables**: `internal/components.Default()` assembler; `cmd/prism`
(`run`/`validate`/`version`); container image; systemd unit; `make full-tests`
integration (docker-compose: an http sink + MinIO-style object endpoint) and an
e2e that runs a real config: `stdin/file → parse → summary+ml → parquet → http`.

**Tests written first**
- `validate` rejects a broken config with a path-accurate message (CLI test).
- e2e: fixture logs through a real pipeline; assert the object landed in the
  sink and reads back as valid Parquet with expected rows.
- integration: `make full-tests` spins compose, runs e2e, tears down clean.

**Done when**: `make full-tests` is green locally and in CI; the container runs
non-root and processes a sample end-to-end; `validate` catches bad configs.

---

## Cross-cutting, every phase
- TDD order enforced (test-only commit precedes implementation — reviewer checks
  `git log`).
- `make lint test` green before PR; `make full-tests` green before a phase is
  "done".
- Benchmarks for any hot-path change; no `allocs/op` regression on parser/encoder.
- Docs updated in lockstep — if behavior diverges from DESIGN.md, fix one of them
  in the same PR.

## Deferred (post-foundation, registry makes them additive)
- Processors: `ml` (anomaly/aggregate detection) and `script`
  (Starlark/expr/wazero) — out of this cut, additive later.
- Outputs: `sqlite` cache, S3-native, ClickHouse, `http` on the critical path.
- Config hot-reload. - Dead-letter queue failure policy. - OTLP receiver.
- Prometheus `remote_write` receiver (this cut does scrape/pull only).
