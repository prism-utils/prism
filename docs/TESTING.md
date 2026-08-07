# prism — Testing

> How we test, what each layer is for, and how to run it. The rule of thumb:
> **fast tests prove logic, slow tests prove integration, benches prove we
> stayed cheap.** Everything is driven by `make` so nobody memorizes commands.

---

## 1. Test layers (the pyramid)

| Layer | Scope | Speed | Runs in `make test` | Runs in `make full-tests` |
|---|---|---|---|---|
| **Unit** | one function/component in isolation, fakes for deps | ms | ✅ | ✅ |
| **Golden** | encoders/parsers vs checked-in fixtures | ms | ✅ | ✅ |
| **Fuzz** | parsers/scripts never panic on hostile input | short seed / long soak | seed only | seed only |
| **Microbench** | hot-path `allocs/op`, `bytes/op`, throughput | — | on demand (`make microbench`) | — |
| **Integration** | real component ↔ real dependency (docker-compose) | seconds | ❌ | ✅ |
| **E2E** | full pipeline from a real config, in→out asserted | seconds | ❌ | ✅ |

Most tests are **unit + golden**. Integration/e2e are few and high-value.

---

## 2. Conventions

- **Table-driven** tests. Name cases; assert with `testify/require` (fatal) and
  `assert` (non-fatal) appropriately. Diffs via `google/go-cmp`.
- **Fakes over mocks.** Because every dependency is a small interface (see
  DESIGN.md §3), prefer hand-written fakes (`fakeOutput`, `fakeInput`) over a
  mocking framework. A fake is a struct that records calls / returns canned data.
- **No sleeps for synchronization.** Use channels, `context`, and
  `require.Eventually` with a deadline. A `time.Sleep` in a test is a review
  reject.
- **Goroutine leaks fail the test.** Use `go.uber.org/goleak` in
  `TestMain`/`t.Cleanup` for any package that starts goroutines (pipeline,
  inputs, outputs).
- **Buffer leaks fail the test.** Arrow allocator balance
  (`allocated == released`) is asserted for anything that touches `RecordBatch`.
- **Determinism.** No wall-clock, randomness, or network in unit tests — inject
  a clock, a seeded source, a fake transport. Golden files are regenerated only
  via `make golden-update`, reviewed as a normal diff.
- **Fuzz corpora** live in `testdata/fuzz/`; crashers are committed as
  regression seeds.

---

## 3. Layout

```
internal/foo/
  foo.go
  foo_test.go            # unit + golden for this package (same package or _test)
  testdata/              # fixtures + golden files for this package
test/
  e2e/                   # full-pipeline tests (build tag: e2e)
  integration/           # docker-compose-backed tests (build tag: integration)
  fixtures/              # shared sample logs/metrics
deploy/
  docker-compose.integration.yml   # http sink + object-store stand-in
```

Slow layers are behind **build tags** (`//go:build integration` / `e2e`) so
`make test` never accidentally runs them.

---

## 4. Make targets (the only interface you need)

```make
make build          # static binary -> ./bin/prism (CGO_ENABLED=0)
make test           # fast: unit + golden + fuzz seeds + race detector
make lint           # golangci-lint
make microbench    # hot-path Go benchmarks (parser/encoder), prints allocs/op
make bench         # reproducible prism-store vs ClickHouse harness (docker required)
make cover          # coverage.txt + html; prints total
make tidy           # go mod tidy + verify no diff
make full-tests     # test + lint + integration + e2e (spins docker-compose)
make integration    # just the integration layer (compose up -> test -> down)
make e2e            # just the e2e layer
make promql-e2e     # PromQL full-stack: real node-exporter -> agent -> store -> PromQL (docker)
make loki-e2e      # Logs full-stack: agent -> writer store -> read-only reader -> Loki API (docker)
make format-matrix-e2e  # HOT×MERGE format matrix (parquet|duckdb) metrics+logs /sql (docker)
make agent-duckdb-e2e   # Agent duckdb encoder → store ingest (+ mixed hot) via docker
make fuzz           # longer fuzz soak (FUZZTIME overridable)
make golden-update  # regenerate golden files (review the diff!)
make clean
```

- `make test` runs with `-race`. Race conditions fail the build.
- `make full-tests` is the gate for calling a phase "done". It brings up
  `deploy/docker-compose.integration.yml`, runs `integration` + `e2e` tagged
  tests against it, and tears down even on failure (`trap`/`--abort-on-…`).
- `make promql-e2e` is a standalone full-stack check (not part of `full-tests`):
  `deploy/docker-compose.promql-e2e.yml` scrapes a real `node-exporter` with the
  prism agent, ships to `prism-store`, and asserts PromQL over the result. It
  builds both images from source, so it is slower and gated on Docker.
- `make loki-e2e` is the logs counterpart (also standalone, Docker-gated):
  `deploy/docker-compose.loki-e2e.yml` has the prism agent tail a log fixture into
  a **writer** store (`RUN_JOBS=true`) and a second **reader** store
  (`RUN_JOBS=false`, `QUERY_HOT_ONLY=true`, the writer's data dir mounted
  read-only) answer both `/sql FROM logs` and the Loki API — `query_range` with a
  line filter, `labels`, `label/job/values` — over the same refreshed segments
  (the compose file shortens `LOGS_REFRESH_INTERVAL` so the assertion does not
  wait out the production default). It also
  asserts metric LogQL is rejected with `400`. Logs are file-backed, so this is
  the acceptance test for reader/writer parity. `LOKI_API_ENABLED` (default
  `true`) gates the routes; see [`CONFIG.md`](CONFIG.md) §14.
- `make format-matrix-e2e` proves `HOT_SEGMENT_FORMAT` × `MERGE_SEGMENT_FORMAT`
  (`parquet`\|`duckdb`, four combos) for metrics + logs: HTTP ingest → flush/merge
  → `/sql` against `deploy/docker-compose.format-matrix.yml`.
- `make agent-duckdb-e2e` proves agent `duckdb` encoder → HTTP ingest
  (`application/vnd.duckdb`) → `/sql`, including at least one mixed
  `HOT_SEGMENT_FORMAT=duckdb` combo (`deploy/docker-compose.agent-duckdb-e2e.yml`,
  CGO agent image).
- `test/e2e/alert_e2e_test.go` drives `prism-alert` end to end (no Docker): the
  canonical `promql` engine evaluates a real `up == 1` expression over an
  in-memory `storage.Queryable` serving the store's `/{ns}/api/v1/query` shape,
  and the full ruler → dispatcher → v4 webhook client chain fires into a real
  HTTP notifier receiver, asserting a firing→resolved transition. It uses an
  in-memory store (not `teststorage`/`promqltest`) so `go.mod` gains no cloud
  SDK — see [`DESIGN.md`](DESIGN.md) §15 → "prism-alert".

---

## 5. What each layer must cover (minimums)

- **Every component**: happy path, at least one failure path, config
  `Validate()` rejection, and (if it starts goroutines) a `goleak` check.
- **Parsers**: golden schema+values, schema evolution/auto-discovery, **fuzz**
  (no panic; malformed → routed error).
- **Encoders**: **round-trip** (encode → decode → equal), buffer release.
- **Outputs**: transport behavior against a test server / container
  (`httptest` for unit; real container for integration), retry + give-up.
- **Pipeline**: order honored, EOF drain, ctx-cancel drain, backpressure,
  no goroutine/buffer leaks.
- **Hot paths**: a benchmark asserting an allocation ceiling (guard against
  regressions in review).

---

## 6. What NOT to do in tests

- ❌ Real network / real filesystem outside `t.TempDir()` in unit tests.
- ❌ `time.Sleep` to "wait for" async work.
- ❌ Asserting on log output as the primary behavior check (assert on state).
- ❌ Sharing mutable state between test cases (each case is independent).
- ❌ Skipping integration/e2e "because it's slow" when the change touches I/O.
- ❌ Editing golden files by hand — regenerate + review.

---

## 7. Environment notes

- Unit/golden/bench need **only the Go toolchain**.
- Integration/e2e need **Docker + docker-compose**. `make full-tests` checks
  they're present and prints a clear message if not, rather than failing deep in
  a test.
- CI runs `make lint test` on every PR (fast gate) and `make full-tests` on the
  merge-blocking job. Local repro is the identical `make` target — no bespoke CI
  scripting.
