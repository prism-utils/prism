# prism — Design

> The authoritative description of *what* prism is and *why* it is shaped this
> way. Implementation lands phase-by-phase (see [`PLAN.md`](PLAN.md)); this
> document is the contract every phase is measured against. If code and this
> doc disagree, that is a bug in one of them — reconcile, don't drift.

---

## 1. Goals and non-goals

### Goals
- **Config-driven pipelines**: a config declares a **list of pipelines**, each
  `input → parser → processors → buffer → fan-out branches (encoder → output)`,
  in YAML/JSON, no recompilation to change topology.
- **Concurrent by construction**: each pipeline (one per input) runs in its own
  worker — inputs are processed independently, in isolation, in parallel.
- **Cleanly extensible**: adding an input, processor, encoder, or output is
  *implement one interface + register one factory*. Zero edits to core wiring.
- **Memory-efficient by construction**: bounded, streaming, columnar. Steady
  state must be flat regardless of input size (except explicit batch mode).
- **One static binary**, `CGO_ENABLED=0`, runs on Linux bare metal and in
  containers with no external runtime deps.
- **Testable to the seams**: every component is unit-testable in isolation;
  the pipeline is testable end-to-end from a config string.

### Non-goals (for the foundation)
- Not a general stream-processing platform (no clustering, no exactly-once,
  no distributed state, no cross-process coordination). It is an **edge agent**.
- **No ML and no scripted/plugin processors in this cut.** The `ml` detector and
  the `script` (Starlark/expr/wazero) engines remain future components the
  registry makes trivial to add; they are explicitly out of scope here.
- No embedded SQL/OLAP engine in the agent. The agent emits *summaries* as JSON;
  storing/querying them in SQLite is a **server-side (sink) concern**, not
  prism's. No SQLite/ClickHouse/S3-SDK outputs in the agent yet.
- No hot-reload of config in v1 (documented as a future extension point).

### In scope for this cut (the two working end-to-end paths)
- **Metrics:** `prometheus (scrape /metrics) → buffer → { parquet→file,
  summary→json→file }`.
- **Logging:** `file (tail) → parse → template → buffer → { parquet→file,
  summary→json→file }`.

---

## 2. Mental model (OTel-shaped, deliberately)

prism copies the **OpenTelemetry Collector** shape because it is proven and
familiar: typed components (`receiver`/`processor`/`exporter`) built by
**factories** from typed config, wired into pipelines. We keep that skeleton
and diverge only where our edge/columnar goals demand it.

Reference implementations we borrow patterns from (read before inventing):

| Project | What we take from it |
|---|---|
| **OpenTelemetry Collector** (Go) | factory + registry + typed config + `Start/Shutdown` lifecycle |
| **Redpanda Connect / Benthos** (Go) | config-first input→processor→output, plugin registration ergonomics, streaming batches |
| **Telegraf** (Go) | `execd`/shim external-plugin idea (informs our scripting boundary), aggregator model |
| **Vector** (Rust) | pipeline-as-config, backpressure discipline (what to imitate, in Go) |

**Deliberate divergence:** our unit of data on the wire between stages is a
**columnar Arrow record batch**, not a per-event struct. This is what makes
Parquet encoding free and vectorized processing cheap. See §5.

---

## 3. Component model

Five component kinds. Each is a tiny interface; everything else composes them.

```
Input      produces  RawBatch        (bytes/lines + metadata)
Parser     RawBatch  -> RecordBatch  (structured, Arrow)
Processor  RecordBatch -> RecordBatch (transform / enrich / aggregate / drop)
Encoder    RecordBatch -> EncodedBlock (parquet / json / raw bytes)
Output     consumes  EncodedBlock
```

`Parser` and `Encoder` are *specialized processors* at the pipeline edges, but
we give them their own interfaces because their contracts differ (a parser owns
schema discovery; an encoder owns framing/format). This keeps each interface
honest and single-responsibility (ISP).

### 3.1 Lifecycle — every component implements it

```go
// Component is the common lifecycle contract. Start must NOT block; long-running
// work belongs on a goroutine that respects ctx cancellation. Shutdown must be
// idempotent and flush/close cleanly within the caller's ctx deadline.
type Component interface {
    Start(ctx context.Context, host Host) error
    Shutdown(ctx context.Context) error
}
```

`Host` is the narrow capability surface a component is allowed to touch
(logger, metrics registry, temp dir, buffer allocator). Components get
capabilities via `Host`, never via globals — this is the seam that makes them
testable and prevents hidden coupling.

### 3.2 The kind interfaces

```go
type Input interface {
    Component
    // Batches emits RawBatches until the input is exhausted (batch/stdin) or
    // ctx is cancelled (tail). The channel is closed when no more data will
    // come. Backpressure is the channel: a slow pipeline slows the input.
    Batches() <-chan RawBatch
}

type Parser interface {
    Component
    // Parse converts raw bytes into a typed RecordBatch, discovering/merging
    // schema as needed. Never panics on malformed input; routes to an error
    // sink instead (see §8).
    Parse(ctx context.Context, in RawBatch) (RecordBatch, error)
}

type Processor interface {
    Component
    // Process transforms a batch. It may return a batch with fewer rows
    // (filtering), more columns (enrichment), or aggregated rows (summary).
    // Returning a zero-row batch is valid and means "fully filtered".
    Process(ctx context.Context, in RecordBatch) (RecordBatch, error)
}

type Encoder interface {
    Component
    // Encode serializes a batch into a self-contained block (e.g. a complete
    // Parquet file/row-group). Encoders own their buffering and MUST release
    // Arrow buffers back to the allocator when done.
    Encode(ctx context.Context, in RecordBatch) (EncodedBlock, error)
}

type Output interface {
    Component
    // Consume ships one encoded block. It owns ret/ack semantics for its
    // transport. Errors are returned for the pipeline's retry/DLQ policy.
    Consume(ctx context.Context, block EncodedBlock) error
}
```

**Why interfaces this small?** Interface Segregation + Dependency Inversion:
the pipeline runtime depends only on these abstractions, never on concrete
types. A test can substitute any stage with a fake in one line.

---

## 4. Extensibility: the factory + registry pattern

This is the backbone. **No core file changes when adding a component.**

```go
// Factory builds a typed component from its typed config. One factory per
// component "type" string used in YAML/JSON.
type Factory[T any] interface {
    Type() string                       // e.g. "file", "parquet", "http"
    DefaultConfig() Config              // zero value with sane defaults
    Create(cfg Config, set Settings) (T, error)
}

// Registry holds factories keyed by kind + type. Registration happens in each
// component package's init() (or an explicit RegisterAll for testability).
type Registry struct { /* inputs, parsers, processors, encoders, outputs */ }

func (r *Registry) RegisterInput(f Factory[Input]) error
func (r *Registry) RegisterProcessor(f Factory[Processor]) error
// ... one per kind
```

Config → component resolution:

```
YAML/JSON ──► decode into `pipelineConfig` (typed) ──►
  for each stage: registry.lookup(kind, type) ──►
    factory.DefaultConfig() ──► overlay user block ──► Validate() ──►
      factory.Create(cfg) ──► component instance
```

**Rules that keep this clean:**
- A component package **only** imports the `component` interfaces + its own
  libs. It never imports the pipeline runtime. (Acyclic, leaf packages.)
- Registration is explicit and injectable: production wires a `Registry` via a
  single `components.Default()` assembler; tests build a `Registry` with only
  the fakes they need. **No mandatory `init()` magic** — `init()` may be used
  for convenience, but the assembler is the source of truth so tests stay
  hermetic.
- Unknown `type` in config is a **load-time error with the list of known
  types**, never a silent no-op.

Adding "write to SQLite" later is: new package `output/sqlite`, implement
`Output`, add one line to the assembler, done. That is the whole point.

---

## 5. Data model — Apache Arrow record batches

The in-memory unit flowing between stages is an **Arrow `RecordBatch`** wrapped
in a thin `RecordBatch` type (schema + columnar arrays + provenance metadata).

**Why Arrow:**
- **Columnar** → Parquet encoding is close to free (Arrow→Parquet is native).
- **Memory-efficient** → contiguous, poolable buffers via Arrow's allocator;
  we reuse buffers instead of GC-thrashing per event.
- **Vectorized processing** → summary/aggregation over columns, not row loops.
- **Schema is first-class** → field auto-discovery is "evolve the schema".

**Contract:**
- Batches are **bounded** (`max_rows` / `max_bytes`, configurable). An input
  never materializes an unbounded batch.
- Ownership is **linear**: whoever receives a batch owns releasing it. Encoders
  and outputs `Release()` buffers when done. This is enforced by tests that
  assert allocator balance (allocated == released) — a leak fails CI.
- Row-oriented inputs (log lines) are converted to columnar by the **parser**,
  which is also where **field auto-discovery** happens (infer columns/types
  from the first N records, then evolve).
- The **`logs`** parser is deliberately conservative: it does **not** guess
  fields. By default it keeps the raw line in a normalized `message` column and
  extracts nothing else. It extracts fields **only** for a known format — `k8s`
  (CRI container-log), `json`, `syslog` (RFC3164/5424), `clf`, `cef` — selected
  explicitly via `format`, or discovered per line with `format: auto` (falling
  back to raw). Every row carries a stable `message` (the templatable text) and
  a `format` column; timestamp fields are never ingested. Downstream is then
  format-agnostic: template on `message`, summarize per template for any input,
  and add extracted fields to `group_by` for a known-format source.

`RawBatch` (pre-parse) is intentionally dumb: a slice of records (offsets into a
reused byte buffer) + source metadata (path, offset, timestamp). No per-line
allocations in the hot path.

---

## 6. Pipeline runtime

The runtime runs a **set of pipelines concurrently** — one per input, each in
its own worker, fully isolated from the others (a crash-safe error in one does
not stop the rest; see §10). A single pipeline is:

```
input ─chan RawBatch─► parser ─► [pre-processor…]* ─► buffer(window) ─┬─► [proc…]* ─► encoder ─► output   (branch: raw)
                                                                       ├─► [proc…]* ─► encoder ─► output   (branch: template)
                                                                       └─► [proc…]* ─► encoder ─► output   (branch: summary)
```

- **One worker per input.** The runtime builds N pipelines from `pipelines: [ … ]`
  and runs each under its own `errgroup` sub-context. Inputs never share state;
  parallelism is "N inputs → N workers". This is the concurrency model: isolate
  per input, not a shared thread pool fighting over one queue.
- **Stages communicate over bounded channels.** Channel capacity *is* the
  backpressure mechanism: a slow output blocks its branch, which (once every
  branch is blocked) blocks the buffer, which blocks the parser, which blocks
  the input. No unbounded in-memory queue.
- **Accumulation buffer (§6.1).** Between the parser/pre-processors and the
  fan-out sits a windowing buffer that accumulates records and flushes a bounded
  `RecordBatch` on the first of: max age, max rows, or max bytes. "Process"
  (summary, encode) happens per flushed window, not per record.
- **Fan-out branches.** After the buffer, the flushed window is dispatched to
  each configured branch. Every branch gets its **own** `RecordBatch` (a
  retained/sliced view — never a shared mutable batch), owns releasing it, and
  runs its own `[processors] → encoder → output` tail. A branch is where the
  raw / template / summary phases diverge from the same window (each a
  self-contained, time-range-named parquet artifact).
- **One goroutine per stage** within a pipeline (buffer, each branch), each
  owning a `for range ch { … }` loop with `ctx` selected.
- **Graceful shutdown**: `ctx` cancel → inputs stop emitting and close their
  channel → close propagates downstream → the buffer flushes its partial window
  → branches drain → `Shutdown` called in reverse order. Batch/stdin inputs
  reach natural EOF and drive the same drain.
- **Goroutine hygiene**: every goroutine is owned by an `errgroup` tied to the
  run context. `goleak` in tests guarantees no leaks.

`prism run` = build registry → load+validate config → build pipelines →
run each pipeline's `errgroup` under a parent `errgroup` → wait for all inputs
to reach EOF or for a signal → drain → exit code reflects success.

### 6.1 Accumulation buffer (windowing)

The buffer is the "accumulate before processing" stage. It is bounded three
ways, and flushes the accumulated window on **whichever bound is hit first**:

- `max_age` — wall-clock age of the oldest buffered record (**default `30s`**).
- `max_rows` — number of buffered rows (default: unset/no row cap).
- `max_bytes` — accumulated in-memory size (**default `12MiB`**) — the "agent
  memory queue" cap; this is the hard ceiling that keeps steady-state memory
  flat regardless of input rate.

A flush emits one bounded `RecordBatch` downstream to the fan-out. On shutdown
or EOF the buffer flushes whatever partial window it holds so no data is lost.
Because it enforces `max_bytes`, the buffer — not the input — is the component
that guarantees the memory discipline of §11 for windowed pipelines.

---

## 7. Configuration

- **One schema, two encodings.** Config is a Go struct tree with `json` tags.
  YAML is parsed via a yaml→json shim so a single struct + `encoding/json`
  serves both. No divergent YAML/JSON code paths.
- **Layering**: file → environment overrides → CLI flags (later precedence
  wins). Kept minimal in v1 (file + env).
- **Every config type implements `Validate() error`.** Validation is total and
  runs at load time — a bad config never reaches the runtime. Errors name the
  offending path (`processors[2].ml.window: must be > 0`).
- **Defaults come from the factory**, not from scattered literals.

Shape — the top level is `pipelines: [ … ]`. Each pipeline has an `input`, a
`parser`, optional pre-buffer `processors`, a `buffer`, and one or more
`branches`; each branch has optional `processors`, an `encoder`, and an
`output`. Every stage is `type` (selects the factory) + `options` (the
type-specific block the factory decodes into its own typed struct + validates).
Keeping `options` opaque at the top level is what lets `internal/config` stay
free of any component import (dependencies point inward, §14):

```yaml
pipelines:
  # ── metrics: prometheus → buffer → raw parquet (no summary) ──
  - name: metrics
    input:
      type: prometheus
      options:
        targets: ["http://localhost:9100/metrics"]
        interval: 15s
    parser:
      type: prometheus            # exposition text → columnar samples
    buffer:
      max_age: 30s                # flush on first of these three
      max_bytes: 12MiB
      max_rows: 0                 # 0 = no row cap
    branches:
      # No summary branch: server-side analytics aggregate the columnar parquet
      # directly, which is cheaper than pre-aggregating a fixed set here.
      - name: raw
        encoder: { type: parquet, options: { compression: zstd, row_group_rows: 50000 } }
        output:  { type: dir, options: { dir: /var/lib/prism/metrics/raw } }

  # ── logging: file(tail) → parse → buffer → three parquet phases ──
  - name: logging
    input:
      type: file
      options: { path: /var/log/app.log, mode: tail }
    parser:
      type: logs                  # no field-guessing; format: none|auto|k8s|json|syslog|clf|cef
      options: { format: auto }   # sniff known formats per line, else keep raw
    buffer:
      max_age: 30s
      max_bytes: 12MiB
    branches:
      # phase raw: records exactly as parsed, no template column.
      - name: raw
        encoder: { type: parquet, options: { compression: zstd } }
        output:  { type: dir, options: { dir: /var/lib/prism/logs/raw } }
      # phase template: records plus a mined, stable template column.
      - name: template
        processors:
          - type: template        # normalizes the message into a template key
            options: { source: message, target: template }
        encoder: { type: parquet, options: { compression: zstd } }
        output:  { type: dir, options: { dir: /var/lib/prism/logs/template } }
      # phase summary: count per template, as parquet (chart "template → count").
      - name: summary
        processors:
          - type: template
            options: { source: message, target: template }
          - type: summary
            options: { group_by: [template], aggregates: [count] }
        encoder: { type: parquet, options: { compression: zstd } }
        output:  { type: dir, options: { dir: /var/lib/prism/logs/summary } }
```

Secrets in any `options` block use `${VAR}` env interpolation (§12).

---

## 8. Processors: built-in (compiled)

All processors in this cut are **built-in / compiled** Go, sharing the one
`Processor` interface. Fast (no interpreter), toggled by config
(`enabled: true|false`, where a disabled processor is a proven identity no-op).

- `template` normalizes semi-structured log lines into a stable **template key**
  (the invariant skeleton with variable tokens masked, e.g.
  `user <*> logged in from <*>`), so summaries can group by log shape rather
  than by unique message. It wraps the Go logging-normalization library
  (`github.com/air-gapped/lessence`, in-process, pure-Go) when available; the
  fallback is a Drain-style streaming template miner implemented in-tree (a
  fixed-depth parse tree that clusters lines into templates online). Either way
  it adds a `template` column and never drops data.
- `summary` does the roll-ups — `count/sum/avg/min/max/percentiles`, grouped by
  configured columns — over the Arrow columns of one flushed window. Its output
  is a small `RecordBatch` of aggregate rows; paired with the `json` encoder
  (§9) it becomes the `[{...}, ...]` summary the sink stores. prism itself does
  **no** SQL — the "store in SQLite / query" step is server-side.

Deferred (registry makes them additive later, out of scope now): `ml`
(anomaly/aggregate detection behind a `Detector` interface) and `script`
(Starlark/expr/wazero runtime engines).

**Ordering is explicit and honored.** `processors:` is an ordered list;
prism runs them in exactly that order. No implicit reordering.

---

## 9. Encoders & outputs

- **Encoders**: `parquet` (Arrow→Parquet via `apache/arrow-go`, configurable
  compression + row-group sizing) encodes the full window; `json` serializes a
  batch as a JSON array `[{col: val, …}, …]` (one object per row) — this is the
  encoder the `summary` branch uses to emit its aggregate rows. `arrow`
  serializes the window as an Arrow IPC stream (schema + record batch) — the
  columnar wire format for the `flight` output, so a receiver ingests the
  columns directly instead of re-parsing a row format. `raw` remains for
  debug/passthrough. An encoder emits a self-contained `EncodedBlock` (a
  complete Parquet file or a framed byte blob) plus metadata (row count, byte
  size).
- **Outputs**:
  - `file` — write blocks to files with time/size rotation and atomic rename.
  - `dir` — write each block to its own file (temp-file + atomic rename), the
    sink for self-contained Parquet windows. Files are named
    `<pipeline>-<phase>-<start>-<end>-<seq>.<ext>`, where `phase` is the branch
    name (`raw` | `template` | `summary`) and `start`/`end` are the flushed
    window's UTC bounds in a compact, fixed-width, lexically-sortable form
    (`20060102T150405.000000000Z`). Consumers select files for a timestamp range
    by name alone, without opening footers; the runtime stamps this provenance
    onto each `EncodedBlock` (`BlockMeta`) and the buffer stamps the window range
    onto the flushed `RecordBatch`. Absent provenance falls back to a legacy
    `<nanos>-<seq>` name. One output writes one directory.
  - `flight` — ship the window to an Apache Arrow Flight server via `DoPut`
    (client side). It pairs with the `arrow` encoder: the block's IPC records are
    reframed as `FlightData` so the network payload lands directly in the
    receiver's columnar storage — no row-by-row re-parse on the server, the
    memory/CPU win Arrow Flight is designed for. Producing pipeline/branch/window
    provenance rides in the `FlightDescriptor` path
    (`[pipeline, branch, startNano, endNano]`) so the receiver can name artifacts
    with the same time-range scheme as `dir`. The receiver is
    `prism collect -addr <a> -dir <d>` (§below): a minimal Flight server whose
    `DoPut` handler persists each window as a range-named Parquet file, making the
    transport end-to-end testable and a usable ingest endpoint.
  - `stdout` — write block bytes (debug/pipe).
  - `http` — POST the block as a binary body, configurable method/headers/auth,
    with bounded exponential backoff + retry and a clear give-up path (available;
    not on the critical path for this cut).

The `flight` output + `prism collect` receiver form the columnar-network path:
a per-branch `flight` sink can run *alongside* the durable `dir` Parquet sink, so
the same window is both persisted locally and streamed columnar to an analytics
ingest endpoint.

Outputs own transport-level retry. Cross-cutting failure policy (drop vs block
vs dead-letter) is a pipeline concern (§10), configured once.

---

## 10. Errors, failure policy, and observability

- **No panics in library code.** Malformed records are data, not crashes — they
  route to a configurable failure policy: `drop` (count it), `block`
  (backpressure), or `dead_letter` (future). Parsers/processors return errors;
  the runtime applies policy.
- **Per-pipeline isolation.** Each input's pipeline runs in its own worker; a
  fatal error in one pipeline is logged and stops *that* pipeline, but the
  others keep running. One bad input never takes down the agent.
- **Errors wrap with `%w`** and are inspected with `errors.Is/As`. Sentinel
  errors for expected conditions (EOF, config-not-found).
- **Self-observability**: structured logs via `log/slog`; internal metrics
  (records in/out/dropped, batch sizes, bytes emitted, output latency, retries)
  exposed via an optional Prometheus endpoint; `pprof` behind a flag. These are
  first-class because "silent zero-data" is the failure mode we most fear.

---

## 11. Memory & performance discipline (best practices)

Non-negotiables, enforced by tests/benches (see [`TESTING.md`](TESTING.md)):

1. **Stream, never slurp.** Only `batch` mode reads a whole file, and even then
   in bounded chunks. `tail`/`stdin` are constant-memory.
2. **Bounded everything.** Batches, channels, buffers all have configured caps.
3. **Reuse buffers.** Arrow allocator + `sync.Pool` for scratch; no per-record
   heap allocations on the hot path.
4. **Release ownership.** Allocator balance asserted in tests (leak == fail).
5. **Backpressure, not buffering.** Channels bound memory; we slow the source,
   we do not grow a queue.
6. **Benchmarks gate regressions.** `allocs/op` and `bytes/op` tracked for the
   parser and encoder hot paths.

---

## 12. Packaging & runtime targets

- **Single static binary**, `CGO_ENABLED=0`, cross-compiled per arch
  (`linux/amd64`, `linux/arm64`). Chosen SQLite driver (when added) and all
  current deps are pure-Go to preserve this.
- **Container image**: multi-stage build → `scratch`/`distroless` final, non-root,
  read-only rootfs friendly. See `Dockerfile`.
- **Bare metal**: shipped with a `systemd` unit (`deploy/systemd/prism.service`),
  config at `/etc/prism/prism.yaml`.
- **Config + secrets**: secrets come from env (`${VAR}` interpolation) so the
  same config file is safe to commit; no secrets on disk in the config.

---

## 13. Dependency budget (reuse over rewrite)

We reuse libraries aggressively but each one must earn its place. Intended set
(pinned in Phase 0 at latest release — never invent versions):

| Concern | Library | Why |
|---|---|---|
| Columnar model + Parquet | `github.com/apache/arrow-go/v18` | native Arrow↔Parquet, pure Go, poolable buffers |
| Config (yaml+json+env) | `github.com/knadh/koanf/v2` | layered config, light, one struct for both formats |
| File tailing | `github.com/nxadm/tail` | rotation-aware follow |
| Prometheus scrape | `github.com/prometheus/common/expfmt` | parse `/metrics` exposition text (pure-Go) |
| CLI | stdlib `flag` (cobra a possible later swap) | `run` / `validate` / `version` subcommands |
| Scripting *(deferred)* | `go.starlark.net`, `github.com/expr-lang/expr`, `github.com/tetratelabs/wazero` | runtime injection — out of scope this cut |
| Logging normalization | `github.com/air-gapped/lessence` | the `template` built-in (Go, in-process); Drain-style in-tree fallback |
| Retry/backoff | `github.com/cenkalti/backoff/v4` | http output retry |
| Metrics | `github.com/prometheus/client_golang` | self-observability |
| Testing | `github.com/stretchr/testify`, `go.uber.org/goleak`, `github.com/google/go-cmp` | assertions, leak detection, diffs |

**Rule:** no dependency that forces CGO or breaks the static-binary/cross-compile
guarantee. If a lib would, we wrap the pure-Go alternative or write the thin
piece ourselves.

---

## 14. Package layout

```
cmd/prism/                 # main: run/validate/version, wires components.Default()
internal/
  component/               # the interfaces + Registry + Factory + Host (§3,§4)
  config/                  # typed config tree (pipelines[]), loader, Validate()
  pipeline/                # runtime: builder + per-input workers + buffer +
                           #   fan-out branches + staged channels + errgroup (§6)
  buffer/                  # windowing accumulator (max_age/max_rows/max_bytes) (§6.1)
  data/                    # RecordBatch/RawBatch/EncodedBlock + Arrow helpers (§5)
  input/{stdin,file,prometheus}/   # Input implementations (prometheus = scrape)
  parser/{raw,json,logfmt,regex,prometheus}/
  processor/{summary,template}/    # built-ins in this cut (ml, script deferred)
  encoder/{raw,parquet,json}/
  output/{stdout,file,http}/
  obs/                     # slog + metrics + pprof wiring
  components/              # Default() assembler: registers the built-ins
pkg/                       # (only if we ever expose a stable public API)
```

`internal/` by default — nothing is a public API contract until we say so.
Leaf component packages never import `pipeline`; `pipeline` and `components`
depend on `component` interfaces only. Dependency direction points inward.
