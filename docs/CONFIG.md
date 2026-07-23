# prism configuration reference

This is the complete reference for prism's config file: the top-level structure,
every built-in component and its options (with defaults and validation), the unit
formats, and the shipped example configs. For the architecture behind these
knobs see [`DESIGN.md`](DESIGN.md).

- [1. How config is loaded](#1-how-config-is-loaded)
- [2. Top-level structure](#2-top-level-structure)
- [3. Pipeline](#3-pipeline)
- [4. Units: durations & byte sizes](#4-units-durations--byte-sizes)
- [5. Buffer](#5-buffer)
- [6. Inputs](#6-inputs)
- [7. Parsers](#7-parsers)
- [8. Processors](#8-processors)
- [9. Encoders](#9-encoders)
- [10. Outputs](#10-outputs)
- [11. `prism collect` (Arrow Flight receiver)](#11-prism-collect-arrow-flight-receiver)
- [12. Shipped example configs](#12-shipped-example-configs)
- [13. Full annotated example](#13-full-annotated-example)
- [14. `prism-store` (ingest receiver)](#14-prism-store-ingest-receiver)

---

## 1. How config is loaded

- **Format:** YAML or JSON — the same schema. JSON is a subset of YAML, so both
  parse to one tree.
- **Environment interpolation:** any `${VAR}` in the file is replaced with the
  environment value before parsing (empty string if unset). Use it for paths,
  URLs, and endpoints.
- **Validation is total and up front:** the config is fully validated at load
  time, so a malformed config never reaches the runtime. Errors name the
  offending path, e.g. `pipelines[0].branches[1].encoder.type`.
- **Unknown keys are rejected:** a typo like `buffer:` or an option a component
  doesn't recognize fails the load rather than being silently ignored.

Run it:

```bash
prism validate -config prism.yaml   # load + validate, then exit
prism run      -config prism.yaml   # run until interrupted (SIGINT/SIGTERM)
```

Every component config block has the same shape — a `type` selecting the
component, plus a `type`-specific `options` block:

```yaml
input:
  type: file            # which component
  options:              # that component's options (see its section below)
    path: /var/log/app.log
    mode: tail
```

Components whose sections say "no options" take just `{ type: <name> }`.

---

## 2. Top-level structure

A config is a set of independent **pipelines**. Each pipeline runs in its own
worker; one failing pipeline never takes down the others.

```yaml
include:            # optional — merge pipelines from other files (see below)
  - "config.d/*.yaml"
pipelines:          # at least one required (across the main file + includes)
  - name: <string>  # required, unique across the file
    input:    { ... }          # required — one source
    parser:   { ... }          # required — bytes → columns
    processors: [ ... ]        # optional — pre-buffer transforms (in order)
    buffer:   { ... }          # windowing bounds (defaults applied if omitted)
    on_error: drop | block     # optional — malformed-data policy (default block)
    branches:                  # at least one required — fan-out tails
      - name: <string>
        processors: [ ... ]    # optional — per-branch transforms
        encoder:  { ... }      # required — columns → bytes
        output:   { ... }      # required — where the bytes go
```

### Splitting config with `include` (config.d)

The top-level `include` is a list of file globs whose `pipelines` are merged
into the set — the Beats-style `config.d/*.yaml` pattern, so a site renderer can
drop one file per source into a directory:

- Globs resolve relative to the config file's directory (absolute globs are used
  as-is); matches are merged in sorted order.
- Only the **top-level** config may declare `include`; an included file that
  itself declares `include` is rejected (one level only).
- Pipeline names must still be unique across the merged set.
- `${ENV}` interpolation applies to included files too.
- `include` is honored by `prism run`/`validate` (file-based loading). It is not
  supported when a config is supplied on a stream without a base directory.

See `configs/prism-agent.yaml` and `configs/exporters/*.yaml` for a worked
example (per-exporter pipelines split into their own files).

The data flow of one pipeline:

```
input → parser → [processors] → buffer ─┬─ branch: [processors] → encoder → output
                                        ├─ branch: [processors] → encoder → output
                                        └─ …
```

Processors before the buffer transform every record on the way in; processors
inside a branch transform only that branch's copy of each flushed window.

---

## 3. Pipeline

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | — | Unique pipeline name; appears in logs and output file names. |
| `input` | stage | yes | — | The single data source. See [Inputs](#6-inputs). |
| `parser` | stage | yes | — | Turns raw records into typed columns. See [Parsers](#7-parsers). |
| `processors` | list of stage | no | none | Pre-buffer transforms applied in order to every record. |
| `buffer` | object | no | 30s age + 12 MiB | Windowing bounds. See [Buffer](#5-buffer). |
| `on_error` | string | no | `block` | `drop` = log & skip the offending window, keep running; `block` = stop this pipeline on a parser/processor error. |
| `branches` | list of branch | yes (≥1) | — | Fan-out tails; each independently processes/encodes/outputs the flushed window. |

Each **branch**:

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | string | yes | — | Branch name; used as the output "phase" in file names (`raw`, `template`, `summary`, …). |
| `processors` | list of stage | no | none | Transforms applied to this branch's copy of the window. |
| `encoder` | stage | yes | — | Serializes the window. See [Encoders](#9-encoders). |
| `output` | stage | yes | — | Ships the encoded block. See [Outputs](#10-outputs). |

---

## 4. Units: durations & byte sizes

**Durations** are Go duration strings, always quoted: `"30s"`, `"1m30s"`,
`"500ms"`, `"2h"`. The literal `0` (unquoted) is also accepted; a bare non-zero
number is rejected because its unit would be ambiguous.

**Byte sizes** accept a human string or a plain byte count:

- Binary units (powers of 1024): `KiB`, `MiB`, `GiB` — e.g. `"12MiB"`.
- SI units (powers of 1000): `KB`, `MB`, `GB` — e.g. `"10MB"`.
- Plain integer = bytes: `1048576`.

---

## 5. Buffer

The buffer is a windowing accumulator: it collects parsed records and flushes a
columnar window to the branches on the **first** bound it hits. This is what
turns a stream into bounded, columnar batches and keeps memory flat.

```yaml
buffer:
  max_age:   "5s"      # flush when the oldest buffered record reaches this age
  max_rows:  100000    # flush when this many rows accumulate
  max_bytes: "12MiB"   # flush when the estimated buffered size reaches this
```

| Field | Type | Default | Description |
|---|---|---|---|
| `max_age` | duration | `30s` (see note) | Wall-clock age of the oldest record that triggers a flush. |
| `max_rows` | int | unset | Row count that triggers a flush. |
| `max_bytes` | byte size | `12MiB` (see note) | Estimated buffered bytes that trigger a flush. |

Rules:

- **At least one bound must be active.** All three being `0` is rejected.
- All values must be `>= 0`.
- **Defaults:** if you omit the `buffer` block entirely (all three unset), prism
  applies `max_age: 30s` + `max_bytes: 12MiB`. If you set *any* bound yourself,
  no defaults are added — set the ones you want.
- The window's `[start, end]` time range is stamped onto every output artifact
  (used in the `dir` file name), so files are selectable by time range.

---

## 6. Inputs

Exactly one input per pipeline.

### `stdin`

Reads newline-delimited records from standard input. No source options beyond
batch sizing.

| Option | Type | Default | Description |
|---|---|---|---|
| `batch_size` | int | `1000` | Max records per emitted raw batch. |

```yaml
input: { type: stdin, options: { batch_size: 1000 } }
```

### `file`

Reads a file, either once (`batch`) or by following appends (`tail`).

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | string | yes | — | File to read. |
| `mode` | string | no | `batch` | `batch` reads the whole file once, in bounded chunks; `tail` follows new lines as they are appended. |
| `batch_size` | int | no | `1000` | Max records per emitted raw batch (must be `> 0`). |

```yaml
input:
  type: file
  options: { path: "/var/log/app.log", mode: tail, batch_size: 500 }
```

### `prometheus`

Scrapes one or more Prometheus `/metrics` (text exposition) endpoints on an
interval. Pair with the `prometheus` parser.

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `targets` | list of string | yes (≥1) | — | `/metrics` URLs to scrape; none may be empty. |
| `interval` | duration string | no | `15s` | Scrape period (must be `> 0`). |
| `timeout` | duration string | no | `10s` | Per-request timeout (must be `> 0`). |
| `basic_auth` | object | no | — | HTTP Basic credentials `{username, password}` sent on every scrape. Mutually exclusive with `bearer_token`. |
| `bearer_token` | string | no | — | Sent as `Authorization: Bearer <token>`. Mutually exclusive with `basic_auth`. |
| `tls` | object | no | — | Client TLS block (see [TLS block](#tls-block)) for https targets. |

Auth/TLS apply to **all** targets of this input (the Prometheus `scrape_config`
model). Put one exporter behind one input when credentials differ. All secret
fields should come from `${ENV}`.

```yaml
input:
  type: prometheus
  options:
    targets: ["https://es:9114/metrics"]
    interval: "15s"
    timeout: "10s"
    basic_auth:
      username: "${ES_EXPORTER_USER}"
      password: "${ES_EXPORTER_PASS}"
    tls:
      ca: /etc/prism/tls/ca.pem
      server_name: es.internal
```

> Note: prometheus input option durations are plain strings (`"1s"`), parsed by
> the component — the same syntax as the config `Duration` unit.

<a id="tls-block"></a>
#### TLS block (shared)

Used by the `prometheus` input and the `http` / `flight` outputs. File fields
are paths read at start; no key material lives in the config.

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `ca` | string (path) | no | system roots | PEM bundle to verify the server cert. |
| `cert` | string (path) | no | — | Client cert for mTLS (must be set with `key`). |
| `key` | string (path) | no | — | Client key for mTLS (must be set with `cert`). |
| `server_name` | string | no | — | SNI / verification hostname override. |
| `insecure_skip_verify` | bool | no | `false` | Disable cert verification (self-signed dev only — unsafe in prod). |

---

## 7. Parsers

Exactly one parser per pipeline; it turns raw records into a columnar batch.

### `raw`

No parsing: each record becomes a single `line` (binary) column. No options.

```yaml
parser: { type: raw }
```

### `logfmt`

Parses `key=value` logfmt lines; each key becomes a column. No options.

```yaml
parser: { type: logfmt }
```

### `json`

Parses one JSON object per line; keys become columns (nested keys are
flattened). No options.

```yaml
parser: { type: json }
```

### `regex`

Parses each line with an RE2 regex; every **named capture group** becomes a
column.

| Option | Type | Required | Description |
|---|---|---|---|
| `pattern` | string | yes | RE2 pattern with at least one named group `(?P<name>…)`. |

```yaml
parser:
  type: regex
  options:
    pattern: '^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] "(?P<method>\S+) (?P<path>\S+)'
```

### `prometheus`

Parses Prometheus text exposition into a fixed sample schema: `__name__`,
`labels`, `timestamp_ms`, `value`. Pair with the `prometheus` input. No options.

```yaml
parser: { type: prometheus }
```

### `logs`

A conservative log parser: it does **not** guess fields unless the line is in a
**known format**. It always produces a normalized, templatable `message` column
and a `format` column recording how each line was read. Timestamp-like fields
are never emitted (they are variable noise for grouping/templating).

| Option | Type | Default | Description |
|---|---|---|---|
| `format` | string | `none` | `none` \| `auto` \| `k8s` \| `json` \| `syslog` \| `clf` \| `cef`. |
| `message` | string | `message` | Name of the normalized templatable column. |

`format` values:

- **`none`** — no field extraction; the whole line is kept as `message`. Safe
  default: everything is still templatable and summarizable per template.
- **`auto`** — sniff each line; if it matches a known format, extract that
  format's fields, otherwise keep the raw line as `message`.
- **`k8s`** — CRI container-log lines: `<RFC3339Nano> <stdout|stderr> <F|P> <msg>`
  (the timestamp is dropped; `stream`/partial flags and `message` are kept).
- **`json`** — structured JSON logs; a string message-like key becomes
  `message`, other keys become columns, timestamp-like keys dropped.
- **`syslog`** — RFC 3164 and RFC 5424 syslog.
- **`clf`** — Common Log Format (web access): `host`, `method`, `path`,
  `protocol`, `status`, `size`; the request is normalized into `message`.
- **`cef`** — ArcSight Common Event Format headers + extensions.

```yaml
parser:
  type: logs
  options: { format: auto }     # or an explicit format like clf
```

The design intent (see the logging example configs): for an **unknown/`none`**
line, summarize purely on the mined template; for a **known format**, summarize
on the extracted fields plus the templated message.

---

## 8. Processors

Processors transform a columnar batch. They may appear **before the buffer**
(pipeline-level `processors`, applied to every record) or **inside a branch**
(applied to that branch's copy of the flushed window).

### `template`

Mines a stable "template" from a text column using a Drain-style algorithm:
variable tokens (numbers, ids, ips, …) collapse to `<*>`, so `user 42 from 10.0.0.1`
and `user 99 from 10.0.0.9` share one template. This is what makes
"template X → count Y" charts possible.

| Option | Type | Default | Description |
|---|---|---|---|
| `source` | string | `line` | The string column to mine. Use `message` with the `logs` parser. |
| `target` | string | `template` | Name of the added template column. |
| `enabled` | bool | `true` | `false` makes the processor an identity pass (useful to toggle via config). |

```yaml
processors:
  - type: template
    options: { source: message, target: template }
```

### `summary`

Declarative windowed aggregation: group rows by string columns and compute
aggregates. One row out per group.

| Option | Type | Required | Description |
|---|---|---|---|
| `group_by` | list of string | no | String columns to group by. Empty = one global group. |
| `aggregates` | list of string | yes (≥1) | Aggregate directives (see below). |

Aggregate directive syntax:

- `count` → output column `count` (rows per group).
- `<fn>:<field>` where `fn` is `sum`, `avg`, `min`, `max` → output column
  `<fn>_<field>` (e.g. `sum:value` → `sum_value`).
- `p<NN>:<field>` percentile, `NN` in `[0,100]` → output column `p<NN>_<field>`
  (e.g. `p95:latency_ms` → `p95_latency_ms`).

```yaml
# count per log template (works for any input)
processors:
  - type: summary
    options: { group_by: ["template"], aggregates: ["count"] }

# per-series stats for metrics
processors:
  - type: summary
    options:
      group_by: ["__name__"]
      aggregates: ["count", "sum:value", "avg:value", "max:value", "p95:value"]
```

> `group_by` columns must be string-typed in the batch. With the `logs` parser,
> extracted numeric fields (e.g. CLF `status`) may be typed — group by a string
> field (e.g. `method`) plus `template`, as in `configs/logging-clf.yaml`.

---

## 9. Encoders

An encoder serializes a flushed window into a self-contained block.

### `parquet`

Arrow → Parquet (a complete Parquet file per window). The durable columnar sink.

| Option | Type | Default | Description |
|---|---|---|---|
| `compression` | string | `snappy` | `snappy` \| `zstd` \| `gzip` \| `none` (`uncompressed` is an alias for `none`). |
| `row_group_rows` | int | `0` | Rows per Parquet row-group; `0` keeps one row-group for the whole window. |
| `bloom.enabled` | bool | `true` | Write token + n-gram Bloom filters to footer KV for configured string columns. |
| `bloom.columns` | string[] | `[message]` | String columns to index; absent/non-string columns are skipped silently. |
| `bloom.tokens` | bool | `true` | Emit a word-token bloom per row-group (`[^a-zA-Z0-9]+` splitting). |
| `bloom.ngram` | int | `3` | N-gram length for the substring bloom; `0` disables the n-gram bloom. |
| `bloom.fp` | float | `0.01` | Target false-positive rate in `(0,1)` used to size each bloom. |

```yaml
encoder:
  type: parquet
  options:
    compression: zstd
    row_group_rows: 50000
    bloom:
      enabled: true
      columns: [message]
      tokens: true
      ngram: 3
      fp: 0.01
```

### `arrow`

Serializes the window as an **Arrow IPC stream** (schema + record batch). This
is the columnar wire format the [`flight`](#10-outputs) output ships, so a
receiver ingests the columns directly with no row-by-row re-parse. No options.

```yaml
encoder: { type: arrow }
```

### `json`

Serializes the batch as a JSON array `[{col: val, …}, …]` (one object per row).
Handy for summaries and debugging. No options.

```yaml
encoder: { type: json }
```

### `raw`

Passthrough of the raw payload bytes; debug/passthrough. No options.

```yaml
encoder: { type: raw }
```

---

## 10. Outputs

An output ships one encoded block, owning its transport-level retry/ack.

### `dir`

Writes each window to its own file in a directory (temp-file + atomic rename).
The main sink for self-contained Parquet windows.

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `dir` | string | yes | — | Destination directory (created if missing). |
| `prefix` | string | no | none | Prepended to every file name. |
| `extension` | string | no | block format | Overrides the file extension; defaults to the encoder's format (e.g. `parquet`). |

**File naming:** `<prefix><pipeline>-<branch>-<start>-<end>-<seq>.<ext>` where
`branch` is the phase (`raw`/`template`/`summary`/…) and `start`/`end` are the
window's UTC bounds in a compact, fixed-width, lexically-sortable form
(`20060102T150405.000000000Z`). Consumers select files for a time range by name
alone. If provenance is absent, a legacy `<nanos>-<seq>` name is used.

> Exactly one output should write to a given directory. Two outputs sharing a
> directory is a misconfiguration (their sequence counters are independent).

```yaml
output: { type: dir, options: { dir: "/var/lib/prism/logs/raw" } }
```

### `file`

Appends blocks to a single file.

| Option | Type | Required | Description |
|---|---|---|---|
| `path` | string | yes | Destination file. |

```yaml
output: { type: file, options: { path: "/tmp/out.jsonl" } }
```

### `stdout`

Writes block bytes to standard output (debug/pipe). No options.

```yaml
output: { type: stdout }
```

### `flight`

Ships the window to an Apache Arrow **Flight** server via `DoPut` (client side).
Pair it with the `arrow` encoder: the block's IPC records are reframed as
`FlightData` so the payload lands directly in the receiver's columnar storage —
the memory/CPU win Flight is designed for. Pipeline/branch/window provenance
rides in the Flight descriptor so the receiver can name artifacts with the same
time-range scheme as `dir`.

| Option | Type | Required | Description |
|---|---|---|---|
| `addr` | string | yes | Flight server endpoint, `host:port`. |
| `token` | string | no | Bearer token sent per-RPC as `authorization: Bearer <token>` (use `${ENV}`). |
| `tls` | object | no | Client TLS block (see [TLS block](#tls-block)). When set, the connection uses TLS; otherwise it is plaintext. |

```yaml
# run alongside a durable dir sink: same window persisted locally AND streamed
- name: wire
  encoder: { type: arrow }
  output:
    type: flight
    options:
      addr: "ingress.tenant.example.com:443"
      token: "${AGENT_API_KEY}"
      tls: { server_name: "ingress.tenant.example.com" }
```

### `http`

POSTs each encoded block (a self-contained Parquet window) to an HTTP(S)
endpoint, retrying transient failures (`429`/`5xx`/transport errors) with capped
exponential backoff and giving up with a typed error. This is the authenticated
egress that reaches a Bearer-checking ingress (e.g. Traefik ForwardAuth). A
`4xx` other than `429` is permanent and fails immediately.

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `url` | string | yes | — | Endpoint each block is POSTed to. |
| `method` | string | no | `POST` | `POST`, `PUT`, or `PATCH`. |
| `headers` | map[string]string | no | — | Extra request headers (values may use `${ENV}`). |
| `token` | string | no | — | Sent as `Authorization: Bearer <token>` (use `${ENV}`). |
| `content_type` | string | no | `application/octet-stream` | Request `Content-Type`. |
| `tls` | object | no | — | Client TLS block (see [TLS block](#tls-block)). |
| `max_retries` | int | no | `5` | Retries after the first attempt (`>= 0`). |
| `timeout` | duration string | no | `30s` | Per-attempt timeout. |
| `initial_backoff` | duration string | no | `500ms` | First retry delay. |
| `max_backoff` | duration string | no | `30s` | Retry delay cap. |

```yaml
output:
  type: http
  options:
    url: "https://ingress.tenant.example.com/logs-raw"
    token: "${AGENT_API_KEY}"
    content_type: "application/vnd.apache.parquet"
    max_retries: 5
```

---

## 11. `prism collect` (Arrow Flight receiver)

The receiver for the `flight` output: a minimal Flight server whose `DoPut`
handler persists each received window to a **time-range-named Parquet** file
(the same naming as `dir`), making the columnar transport testable and a usable
ingest endpoint. It is a subcommand, not a pipeline config:

```bash
prism collect -addr :8815 -dir ./ingest
```

| Flag | Default | Description |
|---|---|---|
| `-addr` | `:8815` | Address to bind the Flight receiver on. |
| `-dir` | — (required) | Directory to persist received windows as Parquet. |
| `-token` | — | Require this bearer token on every RPC (or set `PRISM_COLLECT_TOKEN`). Pair with a `flight` output's `token`. |

End to end, locally:

```bash
prism collect -addr :8815 -dir ./ingest &
PRISM_METRICS_URL=http://host/metrics PRISM_OUT=./out PRISM_FLIGHT_ADDR=localhost:8815 \
  prism run -config configs/metrics-flight.yaml
# both ./out/metrics/raw and ./ingest receive byte-identical range-named parquet
```

---

## 12. Shipped example configs

Ready-to-run configs live in [`configs/`](../configs); each is heavily
commented. All use `${PRISM_*}` env vars for paths/URLs.

| File | What it does |
|---|---|
| `configs/metrics.yaml` | Scrape a Prometheus endpoint → persist raw series as time-range Parquet. No summary branch (server-side analytics aggregate the columnar Parquet directly). |
| `configs/metrics-flight.yaml` | Same as above, plus a second `flight` branch that streams the same window (Arrow IPC) to a `prism collect` receiver — durable local Parquet **and** columnar network ingest. |
| `configs/logging.yaml` | Three-phase logging with the `logs` parser (`format: auto`): `raw` Parquet, `template` Parquet, and a per-template `summary` Parquet ("template X → count Y"). Works for any input. |
| `configs/logging-clf.yaml` | Known-format (CLF web access) logging: extract fields, template the request line, and summarize per `(method, template)` — the "known format → fields + templated message" flow. |

---

## 13. Full annotated example

A two-pipeline config exercising most options:

```yaml
pipelines:
  # ---- metrics: scrape → raw Parquet (range-named) ----------------------
  - name: metrics
    input:
      type: prometheus
      options:
        targets: ["${PRISM_METRICS_URL}"]
        interval: "1s"
        timeout: "10s"
    parser: { type: prometheus }
    buffer:
      max_age: "5s"
      max_bytes: "12MiB"
    on_error: drop            # a bad scrape window is logged and skipped
    branches:
      - name: raw
        encoder: { type: parquet, options: { compression: snappy } }
        output:  { type: dir, options: { dir: "${PRISM_OUT}/metrics/raw" } }

  # ---- logs: tail → raw + template + per-template summary ---------------
  - name: logs
    input:
      type: file
      options: { path: "${PRISM_LOG}", mode: tail, batch_size: 500 }
    parser:
      type: logs
      options: { format: auto }
    buffer:
      max_age: "2s"
      max_bytes: "12MiB"
    branches:
      - name: raw
        encoder: { type: parquet, options: { compression: snappy } }
        output:  { type: dir, options: { dir: "${PRISM_OUT}/logs/raw" } }
      - name: template
        processors:
          - type: template
            options: { source: message, target: template }
        encoder: { type: parquet, options: { compression: snappy } }
        output:  { type: dir, options: { dir: "${PRISM_OUT}/logs/template" } }
      - name: summary
        processors:
          - type: template
            options: { source: message, target: template }
          - type: summary
            options: { group_by: ["template"], aggregates: ["count"] }
        encoder: { type: parquet, options: { compression: snappy } }
        output:  { type: dir, options: { dir: "${PRISM_OUT}/logs/summary" } }
```

Validate it before running:

```bash
PRISM_METRICS_URL=… PRISM_OUT=… PRISM_LOG=… prism validate -config prism.yaml
```

---

## 14. `prism-store` (ingest receiver)

`cmd/prism-store` is the durable store server. Ingest is configured entirely
via environment variables (no YAML config file).

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | HTTP bind address (`/healthz`, `/readyz`, ingest). |
| `FLIGHT_ADDR` | _(empty — off)_ | When set, binds an Arrow Flight `DoPut` receiver on this address. |
| `DATA_DIR` | `/data` | Shared data root for all tenants. |
| `ALLOWED_ARTIFACTS` | `metrics-raw` | Comma-separated artifact types accepted on ingest routes. |
| `MAX_BODY_BYTES` | `268435456` | Maximum HTTP ingest body size (256 MiB). |
| `INGEST_TOKEN` | _(empty)_ | Static bearer token when `AUTH_MODE=bearer`. |
| `AUTH_MODE` | `none` | Pluggable auth: `none`, `bearer`, `mtls`, `trusted-header`. |
| `ROUTE_PREFIX` | _(empty)_ | Optional path prefix prepended to ingest routes (e.g. `/prism-proxy`). |
| `HOT_WINDOW_SECONDS` | _(unset)_ | Hot-window duration in seconds (overrides minutes when set). |
| `HOT_WINDOW_MINUTES` | `10` | Hot-window duration in minutes when seconds unset. |
| `SEGMENTS_PER_TIER` | `6` | Minimum live segments at a tier before merge compaction runs. |
| `MAX_SEGMENT_BYTES` | `2147483648` | Seal threshold (2 GiB); sealed segments are never merge inputs. |
| `RETENTION_DAYS` | `15` | Delete tier segments and rollups strictly older than this window. |
| `ROLLUP_STEPS` | `1m,5m,1h` | Comma-separated rollup intervals built after L1+ merges. |
| `MAX_TIER` | `8` | Highest tier scanned (`L0`…`L8`). |
| `HOT_SNAPSHOT_SECONDS` | `15` | Hot snapshot export ticker interval. |
| `FLUSH_TICK_SECONDS` | `30` | Hot→L0 flush ticker interval. |
| `MERGE_TICK_SECONDS` | `60` | Tier merge ticker interval. |
| `RETENTION_TICK_SECONDS` | _(unset)_ | Retention ticker in seconds; when unset, `RETENTION_TICK_HOURS` applies. |
| `RETENTION_TICK_HOURS` | `1` | Retention ticker in hours when seconds unset. |
| `E2E_EXPOSE_QUERY_SQL` | _(empty — off)_ | When `1`, query JSON responses include the generated SQL (e2e/regression only). |
| `ADMIN_LISTEN_ADDR` | _(empty — off)_ | When set, binds `/admin/*`, `/stats`, and query on a second HTTP server; public `LISTEN_ADDR` keeps ingest + health only. Unset = single mux (dev). |
| `ADMIN_TOKEN` | _(empty — off)_ | Static bearer token for admin-plane routes (`/admin/*`, `/stats`, query on admin bind). Constant-time compare; unset = open (use network isolation). Superseded when `AUTHZ_POLICY_FILE` is set. |
| `AUTHZ_POLICY_FILE` | _(empty — off)_ | Path to deny-by-default RBAC policy YAML. When set, enables JWT/OIDC auth + RBAC on HTTP query/ingest/admin routes. |
| `OIDC_ISSUER` | _(required when RBAC on)_ | OIDC issuer URL for JWT verification (discovery fetches JWKS when JWKS file/URL unset). |
| `OIDC_JWKS_URL` | _(empty)_ | Static JWKS URL (alternative to discovery). |
| `OIDC_JWKS_FILE` | _(empty)_ | Filesystem path to static JWKS JSON (offline-friendly). |
| `OIDC_AUDIENCE` | _(required when RBAC on)_ | Comma-separated accepted `aud` values. |
| `AUTHZ_RELOAD_SECONDS` | `15` | Policy file reload poll interval. |

When **`AUTHZ_POLICY_FILE`** is set, RBAC supersedes `ADMIN_TOKEN` / `INGEST_TOKEN`
on HTTP data/admin routes. **`AUTH_MODE` still governs Arrow Flight** — RBAC does
not cover Flight. If RBAC is on and `FLIGHT_ADDR` is set, `AUTH_MODE=none` is
rejected at startup; use `bearer`/`mtls`/`trusted-header` for Flight or disable
`FLIGHT_ADDR`.

See [`STORE.md`](STORE.md) for query routes, union shape, rollup thresholds, admin provisioning, the `/stats` billing contract, RBAC policy format, and the view-SQL helper.

### HTTP routes

| Method | Path | Success | Failure |
|---|---|---|---|
| `GET` | `/healthz` | `200` body `ok\n` | — |
| `GET` | `/readyz` | `200` body `ready\n` | `503` when `DATA_DIR` is not writable |
| `GET` | `<ROUTE_PREFIX>/{tenant}/query?start=&end=&step=` | `200 application/json` | see query validation in [`STORE.md`](STORE.md) |
| `POST` | `<ROUTE_PREFIX>/{tenant}/ingest/{artifact}` | `204 No Content` | see validation chain below |
| `POST` | `/admin/tenants/{tenant}/ensure` | `204 No Content` | admin plane; see [`STORE.md`](STORE.md) |
| `GET` | `/stats?ns=` | `200 application/json` | admin plane; billing contract in [`STORE.md`](STORE.md) |

When `ROUTE_PREFIX` is empty the ingest path is `/{tenant}/ingest/{artifact}`.

### Ingest validation chain (HTTP and Flight)

Applied in order; the first failure wins:

1. **Auth** — mode-dependent (`401 unauthorized` / `403 forbidden` on tenant mismatch)
2. **Tenant** — must match `internal/store/tenant` validators (`404 unknown tenant`)
3. **Artifact** — well-formed and listed in `ALLOWED_ARTIFACTS` (`404 unknown artifact type`)
4. **Body size** — HTTP only, via `http.MaxBytesReader` (`413 window too large`)
5. **Land** — `engine.Ingest`; empty body is a no-op (`204`)

### Auth modes

| Mode | Identity source | Path tenant rule |
|---|---|---|
| `none` | none (path tenant is authoritative) | no extra check |
| `bearer` | `Authorization: Bearer <INGEST_TOKEN>` (constant-time compare) | no per-tenant identity |
| `mtls` | TLS client certificate CN | CN must equal path tenant (`403` on mismatch) |
| `trusted-header` | `X-Tenant` request header | header must equal path tenant (`403` on mismatch) |

Flight `DoPut` uses the same chain: tenant and artifact come from the
`FlightDescriptor` path `[tenant, artifact, …]`; bearer auth uses gRPC metadata
`authorization: Bearer <token>`.

Graceful shutdown: `SIGINT` / `SIGTERM` → HTTP `Shutdown` (10s) and Flight stop.
