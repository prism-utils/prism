# prism — Task list

At-a-glance tracker derived from [`docs/PLAN.md`](docs/PLAN.md) and its
2026-07 revision. Each item is a spec under [`.ai/specs/`](.ai/specs/) built
test-first (see [`CONTRIBUTING.md`](CONTRIBUTING.md)); check a box only when its
acceptance criteria are met, its PR is merged, and `main` is green.

Target: two working end-to-end paths — **metrics** (`prometheus → buffer →
{parquet→file, summary→json→file}`) and **logging** (`file(tail) → parse →
template → buffer → {parquet→file, summary→json→file}`). No ML, no scripting.

## Foundation (done)
- [x] Module, tooling, Makefile, Dockerfile, CI, docs
- [x] `component`: interfaces + `Factory[T]` + `Host` + `Registry` + tests
- [x] `config`: typed tree + JSON loader + total `Validate()` + tests
- [x] `obs`: slog logger + `Host`
- [x] `data`: interim row-oriented `RawBatch`/`RecordBatch`/`EncodedBlock`
- [x] `pipeline`: single linear builder + run loop + errgroup + goleak tests
- [x] `input/stdin`, `input/file` (batch), `parser/raw`, `encoder/raw`,
      `output/stdout`, `output/file` (append), `components.Default()`, `cmd/prism`

## Ordered build (each = one spec + PR)
1. [x] **config-multipipeline** — koanf YAML + `${ENV}`; reshape config to
       `pipelines: []` with `input/parser/processors/buffer/branches`; total
       `Validate()` with path-named errors.
2. [x] **data-arrow** — swap `RecordBatch` internals to Apache Arrow
       (`apache/arrow-go/v18`): schema + column arrays + allocator; linear
       ownership; allocator-balance assertion helper.
3. [x] **runtime-multiworker** — per-input worker pipelines under a parent
       errgroup; per-stage bounded channels; fan-out branches (each branch owns
       its batch); failure policy (`drop | block`); per-pipeline isolation;
       goleak + backpressure tests.
4. [x] **buffer-window** — accumulation buffer flushing on first of
       `max_age` (30s) / `max_rows` / `max_bytes` (12MiB); flush-on-drain.
5. [x] **input-file-tail** — `mode: tail` via `nxadm/tail` + rotation test +
       constant-memory streaming benchmark.
6. [x] **input-prometheus** — scrape `/metrics` exposition on an interval
       (`prometheus/common/expfmt`) → structured samples; target/interval config.
7. [x] **parsers** — `parser/json`, `parser/logfmt`, `parser/regex`,
       `parser/prometheus`; schema auto-discovery (infer+evolve, deterministic
       type precedence); fuzz (never panic; malformed → routed error).
8. [x] **processor-template** — log-template mining (lessence; Drain-style
       in-tree fallback); adds a `template` column; `enabled:false` = identity.
9. [x] **processor-summary** — windowed group-by aggregates
       (`count/sum/avg/min/max/pXX`) over Arrow columns → aggregate `RecordBatch`.
10. [x] **encoders** — `encoder/parquet` (Arrow→Parquet, compression + row-group)
       with round-trip test; `encoder/json` (`[{…}]`) for summaries.
11. [x] **output-file-rotation** — size/time rotation + atomic rename; no partial
       files visible.
12. [x] **assembly** — `components.Default()` registers the new built-ins;
       `cmd/prism run/validate/version` drives multi-pipeline configs; `validate`
       rejects bad configs with a path-accurate message.
13. [x] **e2e-logging** — `file(tail) → logfmt → template → buffer →
       {parquet→file, summary→json→file}`; assert parquet reads back + summary
       JSON rows.
14. [x] **e2e-metrics** — `prometheus → buffer → {parquet→file,
       summary→json→file}` against a fake `/metrics` server; assert both sinks.
15. [x] **integration-packaging** — `make full-tests` green (compose/httptest);
       container runs non-root end-to-end.

---

### Current status
Both end-to-end paths are green and merged:

- **metrics** — `prometheus(scrape) → prometheus(parse) → buffer →
  {parquet→dir, summary(count/sum/avg/min/max/pNN by __name__)→json→dir}`
- **logging** — `file(tail) → logfmt/json/regex → template → buffer →
  {parquet→dir, summary(count by level)→json→dir}`

`prism run|validate|version` drives multi-pipeline YAML/JSON configs
(`configs/metrics.yaml`, `configs/logging.yaml`); one concurrent, isolated
worker per input; the accumulation buffer flushes on age/rows/bytes (defaults
30s / 12MiB) and aligns heterogeneous windows to a union schema. `make
full-tests` is green; the agent image is CGO-free on distroless nonroot. ML and
scripting remain deferred by design.

## Store track (epic #21, in progress)

Dependency-ordered sub-issues on branch `feat/store-skeleton` and follow-ups:

- [ ] **store-skeleton (#22)** — ADR, `cmd/prism-store` health skeleton,
      `internal/store/**` stubs, `internal/version`, tenant validators.
- [ ] **store-ingest (#23)** — HTTP-parquet ingest landing.
- [ ] **store-engine (#24)** — DuckDB hot catalog + `go-duckdb`.
- [ ] **store-lifecycle (#25)** — snapshot / flush / merge / retention ticks.
- [ ] **store-rollups (#26)** — 1m / 5m / 1h rollup materialization.
- [ ] **store-query (#27)** — read-only time-range query endpoint.
- [ ] **store-provision (#28)** — tenant ensure / admin routes.
- [ ] **store-release (#29)** — Helm chart, GHCR image, goreleaser ldflags.

See [`docs/STORE.md`](docs/STORE.md) and [`docs/DESIGN.md`](docs/DESIGN.md) §15.

---

## Public launch track

**Runbook (execute this):** [`docs/PUBLIC_LAUNCH.md`](docs/PUBLIC_LAUNCH.md)  
Spec: [`.ai/specs/public-launch.md`](.ai/specs/public-launch.md) — `Status: READY`

- [x] **Plan locked** — decisions D1–D12 + L1–L5; maintainer-only CI recipe; exit criteria.
- [ ] **Phase 2** — BSL 1.1 `LICENSE` (Sys Ramos IT LLC; Competing Service grant; Apache-2.0 @ +4y)
- [ ] **Phase 3** — CLA Assistant on external PRs
- [ ] **Phase 4** — Maintainer-only CI (fork approval + `ci.yml` authorize / `ci:run`)
- [ ] **Phase 5** — Rename all `prism-utils` → `prism-utils`
- [ ] **Phase 6** — Move homelab docs to private `prism-implementation`
- [ ] **Phase 7** — Full-history scrub + rotate
- [ ] **Phase 8** — Issue/PR templates + auth warnings + standalone quickstart
- [ ] **Phase 9–10** — Verify + tag **`v1.0.0`**
- [ ] **Phase 11** — Agent flips repo visibility to public
