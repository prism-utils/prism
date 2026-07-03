# prism — Task list

At-a-glance tracker derived from [`docs/PLAN.md`](docs/PLAN.md). Each phase is
built test-first (see [`CONTRIBUTING.md`](CONTRIBUTING.md)); check a box only
when its acceptance criteria in PLAN.md are met and `main` is green.

## Phase 0 — Foundation & tooling
- [x] Module, `.gitignore`, `.editorconfig`, `.golangci.yml`
- [x] `Makefile` (`build`/`test`/`full-tests`/`lint`/`bench`/`tidy`/`cover`/…)
- [x] `Dockerfile` (multi-stage, static, distroless-nonroot) + `deploy/` (compose, systemd)
- [x] CI workflow (fast gate: tidy+lint+test; full gate: integration+e2e)
- [x] Docs: DESIGN, PLAN, CONTRIBUTING, TESTING, REVIEW
- [x] Pin foundation deps via `go get` (`errgroup`, `goleak`) — no invented versions

## Phase 1 — Core contracts
- [x] `component`: interfaces (Component/Input/Parser/Processor/Encoder/Output), `Factory[T]`, `Host`, `Settings`
- [x] `component.Registry` (register/lookup per kind; duplicate/unknown/nil errors) + tests
- [x] `config`: typed tree + loader + total `Validate()` with path-named errors + tests
- [x] `obs`: slog logger + `Host`
- [ ] YAML loading + `${ENV}` interpolation (koanf yaml→json shim)

## Phase 2 — Data model (Arrow) & runtime
- [x] `data`: `RawBatch`/`RecordBatch`/`EncodedBlock` (interim row payload)
- [x] `pipeline`: builder (config→wired chain) + run loop + errgroup lifecycle + graceful drain + tests (goleak)
- [ ] Swap `RecordBatch` internals to Apache Arrow columns (schema + arrays + allocator)
- [ ] Per-stage bounded channels between every stage + backpressure test
- [ ] Configurable failure policy (drop | block | dead-letter)

## Phase 3 — Inputs
- [x] `input/stdin` + tests (bounded batches, cancel, goleak)
- [x] `input/file` mode `batch` (read whole file → EOF → stop)
- [ ] `input/file` mode `tail` (follow + rotation via `nxadm/tail`) + rotation test
- [ ] Constant-memory streaming benchmark for tail

## Phase 4 — Parsers & field auto-discovery
- [x] `parser/raw` (passthrough) — foundation
- [ ] `parser/json`, `parser/logfmt`, `parser/regex` (golden-tested)
- [ ] Schema auto-discovery (infer + evolve) with deterministic type precedence
- [ ] Fuzz the parsers (never panic; malformed → routed error)

## Phase 5 — Parquet encoder & outputs
- [x] `encoder/raw` (newline passthrough) — foundation
- [x] `output/stdout`
- [x] `output/file` (append) — foundation
- [ ] `encoder/parquet` (Arrow→Parquet, compression + row-group) + round-trip test
- [ ] `output/file` rotation (size/time) + atomic rename
- [ ] `output/http` (binary POST + backoff retry + give-up) + httptest coverage

## Phase 6 — Built-in compiled processors
- [ ] `processor/summary` (windowed group-by aggregates over columns)
- [ ] `processor/ml` (behind a `Detector` interface; pure-Go stat detector first)
- [ ] `processor/template` (wrap `air-gapped/lessence`)
- [ ] `enabled: false` is a proven identity no-op for each; no per-record allocs (bench)

## Phase 7 — Scripted processor
- [ ] `processor/script` + `ScriptEngine` interface
- [ ] Starlark engine (default) + expr engine + shared engine contract test
- [ ] wazero WASM engine
- [ ] Sandbox + resource bounds proven (bad script fails safe: no crash, no hang)

## Phase 8 — Assembly, packaging, e2e
- [x] `components.Default()` assembler (registers built-ins)
- [x] `cmd/prism` (`run`/`validate`/`version`) — runs a real pipeline
- [ ] `make full-tests` integration layer green (docker-compose: http sink + MinIO)
- [ ] e2e: `file → parse → summary+ml → parquet → http` lands + reads back
- [ ] Container runs non-root end-to-end; `validate` catches bad configs (CLI test)
- [ ] Consider swapping stdlib flag → cobra

---

### Current status
Foundation is **runnable**: `make build` + `make test` (with `-race`) green;
`prism run` executes a real `stdin|file → raw → raw → stdout|file` pipeline. Next
up: Arrow-backed `RecordBatch` (Phase 2) and the JSON/logfmt parsers (Phase 4).
