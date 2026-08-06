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

### Zero-config quick presets (`--quick`)

`prism run --quick <template>` runs a built-in preset instead of a config file,
so you can pipe logs in with no YAML. The only template in this cut is `logs`:

```bash
my-app 2>&1 | prism run --quick logs         # prints "template → count" to stdout
```

The `logs` preset is `stdin → logs{format:auto} → template + summary`, flushing a
short (2s) window, and prints the per-template counts as JSON to stdout. Cap the
input with `head`/`tail` — there is no agent-side sampling.

| Flag | Default | Description |
|---|---|---|
| `--quick` | — | Preset name (`logs`). Mutually exclusive with `-config`. |
| `--store` | — | prism-store base URL to **also** ship `logs-summary` Parquet to (local stdout still runs). |
| `--tenant` | `default` | Tenant namespace used in the `--store` ingest path. |
| `--token` | — | Bearer token sent to the store on ingest. |
| `--print-config` | `false` | Print the effective preset config (JSON, reloadable) and exit. |

With `--store`, the agent logs one line pointing at the store and the SQL to run;
it does **not** query the store itself. Query the shipped logs with the `logs`
relation (see [`STORE.md`](STORE.md#arbitrary-sql-api)):

```sql
SELECT template, CAST(sum(count) AS BIGINT) AS count FROM logs GROUP BY template ORDER BY count DESC
```

The store must allow the artifact: set `ALLOWED_ARTIFACTS=metrics-raw,logs-summary`
(logs are opt-in; see [§14](#14-prism-store-ingest-receiver)).

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
| `labels` | map string→string | no | — | Static labels attached to every scraped sample (e.g. `{job: clickhouse}`), like Prometheus scrape-target labels. A sample's own exposition label wins on collision. |

Auth/TLS/labels apply to **all** targets of this input (the Prometheus
`scrape_config` model). Put one exporter behind one input when credentials or
labels differ. All secret fields should come from `${ENV}`.

An **`instance`** label is derived automatically from each target's `host:port`
(Prometheus convention), unless `labels.instance` is set. Together with `job`,
this lets the well-known exporter Grafana dashboards — which filter by
`instance`/`job` — resolve against prism's PromQL API. Raw-exposition scrapes
otherwise carry only the exporter's own labels (no `instance`/`job`).

```yaml
input:
  type: prometheus
  options:
    targets: ["https://es:9114/metrics"]
    interval: "15s"
    timeout: "10s"
    labels: { job: "elasticsearch" }   # + auto instance="es:9114"
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

### `duckdb`

Arrow → checkpointed single-table `.duckdb` file (`EncodedBlock.Format =
duckdb`). Requires a **CGO** build of `prism` (go-duckdb). The table name inside
the file is `data`. `STORAGE_VERSION` defaults to `v1.0.0` (same pin as
`DUCKDB_STORAGE_VERSION` on the store). Pair with `dir` (`.duckdb` extension),
`http` (`Content-Type: application/vnd.duckdb` when unset), or `flight` (opaque
DoPut with `format=duckdb` metadata).

| Option | Type | Default | Description |
|---|---|---|---|
| `storage_version` | string | `v1.0.0` | DuckDB `STORAGE_VERSION` pin for the sealed file. |

```yaml
encoder:
  type: duckdb
  options:
    storage_version: v1.0.0
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
Pair it with the `arrow` encoder (IPC reframed as `FlightData`) or the `duckdb`
encoder (opaque `.duckdb` DoPut body with `format=duckdb` in app metadata /
trailing path segment). Pipeline/branch/window provenance rides in the Flight
descriptor so the receiver can name artifacts with the same time-range scheme
as `dir`.

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
| `content_type` | string | no | (format-dependent) | Request `Content-Type`. Empty → `application/vnd.duckdb` for duckdb blocks, otherwise `application/octet-stream`. |
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

`cmd/prism-store` is the durable store server. Configuration is entirely via
environment variables (no YAML config file). This table is the **authoritative,
complete** reference — every variable read by `loadConfig()` in
`cmd/prism-store/main.go` and `loadRBACConfig()` in `cmd/prism-store/rbac.go`,
plus `E2E_EXPOSE_QUERY_SQL` (read by `query.ExposeSQLFromEnv()`).

For memory sizing see [`MEMORY.md`](MEMORY.md). For features and RBAC operation
see [`STORE.md`](STORE.md).

| Env | Type | Default | Meaning |
|---|---|---|---|
| `ADMIN_LISTEN_ADDR` | string | _(empty — off)_ | When set, binds `/admin/*`, `/stats`, and query/SQL on a second HTTP server; public `LISTEN_ADDR` keeps ingest + health only. Unset = single mux (dev). |
| `ADMIN_TOKEN` | string | _(empty — off)_ | Static bearer for admin-plane routes when RBAC is off. Constant-time compare. Superseded when `AUTHZ_POLICY_FILE` is set. |
| `ALLOWED_ARTIFACTS` | string (comma-separated) | `metrics-raw` | Artifact types accepted on ingest routes. Logs (`logs-raw`/`logs-template`/`logs-summary`) are landed as files and queried via the `logs` relation; add them here to enable (e.g. `metrics-raw,logs-summary`). |
| `AUTH_MODE` | string | `none` | Ingest/Flight auth when RBAC is off: `none`, `bearer`, `mtls`, `trusted-header`. HTTP ingest ignores this when RBAC is on (JWT). Flight always uses `AUTH_MODE`. |
| `AUTHZ_POLICY_FILE` | string | _(empty — off)_ | Path to deny-by-default RBAC policy YAML. When set, enables JWT/OIDC + RBAC on HTTP query/ingest/admin routes. |
| `AUTHZ_RELOAD_SECONDS` | int (seconds) | `15` | Policy file reload poll interval. |
| `CLIENT_TENANTS` | string (comma-separated) | _(empty)_ | Owned tenant namespaces in `client` mode; **required** when `MODE=client`. |
| `CLUSTER_CLIENTS` | string | _(empty)_ | Static `tenant=http://host:port,...` map for `cluster` mode; **required** when `MODE=cluster`. |
| `DATA_DIR` | string | `/data` | Shared data root for all tenants. |
| `DUCKDB_MEMORY_LIMIT` | string | _(empty)_ | DuckDB `memory_limit` for tenant engines, `/sql` sandboxes, merge, and rollup workers when set. Unset ⇒ DuckDB default (~80% RAM per instance). |
| `DUCKDB_THREADS` | int | `0` (unset) | DuckDB `threads` when `> 0` on **merge/lifecycle** (and query when `QUERY_DUCKDB_THREADS` unset). Unset ⇒ DuckDB default. Keep at `1` on small memory envelopes. |
| `QUERY_DUCKDB_THREADS` | int | _(falls back to `DUCKDB_THREADS`)_ | DuckDB `threads` for `/sql`, Loki, and PromQL sandboxes only. When the logs open set exceeds 500 files, sandboxes fall back to 1 thread. |
| `E2E_EXPOSE_QUERY_SQL` | string | _(empty — off)_ | When `1`, structured query JSON includes generated SQL (e2e/regression only). |
| `FLIGHT_ADDR` | string | _(empty — off)_ | When set, binds an Arrow Flight `DoPut` receiver on this address. |
| `FLUSH_TICK_SECONDS` | int (seconds) | `30` | Hot→L0 flush ticker interval. |
| `HOT_SNAPSHOT_SECONDS` | int (seconds) | `15` | Hot snapshot export ticker interval. |
| `HOT_SEGMENT_FORMAT` | string | `parquet` | On-disk format for the metrics hot snapshot (`hot/current.parquet` or `hot/current.duckdb`). Values: `parquet` \| `duckdb`. Invalid values fail startup. Live `engine.duckdb` is unchanged; this is the sandbox/replica export only. |
| `MERGE_SEGMENT_FORMAT` | string | `parquet` | On-disk format for metrics flush→L0 and metrics/logs tier merges (`.parquet` or `.duckdb`). Values: `parquet` \| `duckdb`. Invalid values fail startup. |
| `DUCKDB_STORAGE_VERSION` | string | `v1.0.0` | `STORAGE_VERSION` pin for newly created `.duckdb` hot/merge artifacts (must be readable by the bundled go-duckdb). |
| `HOT_WINDOW_MINUTES` | int (minutes) | `10` | Hot-window duration when `HOT_WINDOW_SECONDS` is unset. |
| `HOT_WINDOW_SECONDS` | int (seconds) | _(unset)_ | Hot-window duration in seconds; overrides minutes when set to a positive integer. |
| `INGEST_TOKEN` | string | _(empty)_ | Static bearer token when `AUTH_MODE=bearer` (RBAC off). |
| `LISTEN_ADDR` | string | `:8080` | Primary HTTP bind (`/healthz`, `/readyz`, ingest or combined mux). |
| `LOKI_API_ENABLED` | bool | `true` | When `false`, the Loki logs read API (`/{ns}/loki/api/v1/*`) is not registered. Logs-only; reuses the `/sql` sandbox, RBAC `query`, the `/sql` in-flight queue, `SQL_API_MAX_ROWS` (entries per query), and `SQL_API_TIMEOUT_SECONDS`. |
| `LOGS_RECENT_LOOKBACK_HOURS` | int (hours) | `0` (off) | When `> 0`, Loki label/browse requests with omitted `start` only open log files within now−lookback. Explicit `start`/`end` still reach cold history. |
| `MAX_LOG_FILES` | int | `0` (off) | Per-artifact cap across landing + `logs/<artifact>/tiers/`. When exceeded, retention deletes oldest windows first. |
| `LOG_COALESCE_MAX_AGE_SECONDS` | int (seconds) | `0` (off) | When `> 0` (or bytes set), buffer same-artifact lands and seal one Parquet after this age. |
| `LOG_COALESCE_MAX_BYTES` | int (bytes) | `0` (off) | Seal coalesced log buffer when pending bytes reach this size. |
| `MAX_BODY_BYTES` | int64 (bytes) | `268435456` | Maximum HTTP ingest body size (256 MiB). |
| `MAX_OPEN_TENANTS` | int | `32` | LRU cap on concurrently open per-tenant DuckDB engines (`engine.duckdb`). |
| `MAX_SEGMENT_BYTES` | int64 (bytes) | `2147483648` | Segment seal threshold (2 GiB); sealed segments are never merge inputs. |
| `MAX_TIER` | int | `8` | Highest tier directory scanned (`L0`…`L8`). |
| `MERGE_TICK_SECONDS` | int (seconds) | `60` | Tier merge ticker interval. |
| `MODE` | string | `standalone` | Deployment role: `standalone`, `client`, or `cluster`. |
| `OIDC_AUDIENCE` | string (comma-separated) | _(required when RBAC on)_ | Accepted JWT `aud` values. |
| `OIDC_ISSUER` | string | _(required when RBAC on)_ | OIDC issuer URL; discovery fetches JWKS when JWKS file/URL unset. |
| `OIDC_JWKS_FILE` | string | _(empty)_ | Filesystem path to static JWKS JSON (offline/air-gapped). |
| `OIDC_JWKS_URL` | string | _(empty)_ | Static JWKS URL (alternative to discovery). |
| `PROMQL_API_ENABLED` | bool | `true` | When `false`, the Prometheus read API (`/{ns}/api/v1/*`) is not registered. Metrics-only; reuses the `/sql` sandbox, RBAC `query`, and the `/sql` in-flight queue. |
| `PROMQL_LOOKBACK_DELTA_SECONDS` | int (seconds) | `300` | How far back a PromQL instant vector selector looks for a sample. |
| `PROMQL_MAX_POINTS` | int | `11000` | Max resolution steps per range query (`(end-start)/step`); exceeding returns `400`. |
| `PROMQL_MAX_SAMPLES` | int | `50000000` | Max samples a single PromQL query may load into memory (mirrors Prometheus `--query.max-samples`); exceeding returns `422`. |
| `PROMQL_TIMEOUT_SECONDS` | int (seconds) | `30` | Per-query timeout for the PromQL API. |
| `QUERY_HOT_ONLY` | bool | `false` | When `true`, structured query, `/sql`, and PromQL sandboxes union only hot data (no tier/rollup Parquet). |
| `RETENTION_DAYS` | int | `15` | Delete tier segments and rollups strictly older than this window. |
| `RETENTION_TICK_HOURS` | int (hours) | `1` | Retention ticker interval when `RETENTION_TICK_SECONDS` is unset. |
| `RETENTION_TICK_SECONDS` | int (seconds) | _(unset)_ | Retention ticker in seconds; overrides hours when set to a positive integer. |
| `ROLLUP_STEPS` | string (comma-separated) | `1m,5m,1h` | Rollup intervals materialized after L1+ merges. |
| `ROUTE_PREFIX` | string | _(empty)_ | Optional path prefix for ingest/query/SQL routes (e.g. `/prism-proxy`). |
| `RUN_JOBS` | bool | `true` | When `false`, skip background maintenance (snapshot, flush, merge, rollups, retention). Ingest/query still run. |
| `SEGMENTS_PER_TIER` | int | `6` | Minimum live segments at a tier before merge compaction runs (metrics tiers **and** per-artifact logs landing → `logs/<artifact>/tiers/L0`). |
| `SQL_API_ENABLED` | bool | `true` | When `false`, `POST /{ns}/sql` is not registered. |
| `SQL_API_MAX_BODY_BYTES` | int64 (bytes) | `1048576` | Maximum POST `/sql` JSON body (1 MiB). |
| `SQL_API_MAX_INFLIGHT` | int | `4` | Max concurrent `/sql` executions when `SQL_API_QUEUE_ENABLED=true`. |
| `SQL_API_MAX_QUEUE` | int | `64` | Max `/sql` requests allowed to wait for a slot when queue enabled. |
| `SQL_API_MAX_ROWS` | int | `100000` | Maximum rows per SQL response (`truncated` when exceeded). |
| `SQL_API_QUEUE_ENABLED` | bool | `false` | Enable in-flight limiter on `/sql` (data nodes only; off = prior unbounded behavior). |
| `SQL_API_QUEUE_TIMEOUT_MS` | int (milliseconds) | `5000` | Max wait for an `/sql` slot before `429`. |
| `SQL_API_TIMEOUT_SECONDS` | int (seconds) | `30` | Per-query timeout for `POST /{ns}/sql`. |

**Arrow transport (build-from-source):** release CI and `make docker-store` build
`prism-store` with `-tags duckdb_arrow` (CGO) so `Accept:
application/vnd.apache.arrow.stream` returns a streaming Arrow IPC body on the
same `POST /{ns}/sql` route. Builds without the tag compile a stub that responds
`406 Not Acceptable` to Arrow requests; JSON is unaffected.

When **`AUTHZ_POLICY_FILE`** is set, RBAC supersedes `ADMIN_TOKEN` / `INGEST_TOKEN`
on HTTP data/admin routes. **`AUTH_MODE` still governs Arrow Flight** — RBAC does
not cover Flight. If RBAC is on and `FLIGHT_ADDR` is set, `AUTH_MODE=none` is
rejected at startup; use `bearer`/`mtls`/`trusted-header` for Flight or disable
`FLIGHT_ADDR`.

See [`STORE.md`](STORE.md) for query routes, union shape, rollup thresholds, admin provisioning, the `/stats` billing contract, RBAC, and the view-SQL helper. Memory sizing: [`MEMORY.md`](MEMORY.md).

### HTTP routes

| Method | Path | Success | Failure |
|---|---|---|---|
| `GET` | `/healthz` | `200` body `ok\n` | — |
| `GET` | `/readyz` | `200` body `ready\n` | `503` when `DATA_DIR` is not writable |
| `GET` | `<ROUTE_PREFIX>/{tenant}/query?start=&end=&step=` | `200 application/json` | see query validation in [`STORE.md`](STORE.md) |
| `POST` | `<ROUTE_PREFIX>/{tenant}/sql` | `200 application/json` (default) or `200 application/vnd.apache.arrow.stream` when `Accept` requests Arrow | arbitrary read-only SQL; see [`STORE.md`](STORE.md) § Arbitrary SQL API |
| `GET`/`POST` | `<ROUTE_PREFIX>/{tenant}/api/v1/query`, `/query_range`, `/series`, `/labels` | `200 application/json` (Prometheus envelope) | PromQL read API; see [`STORE.md`](STORE.md) § PromQL API |
| `GET` | `<ROUTE_PREFIX>/{tenant}/api/v1/label/{name}/values` | `200 application/json` | PromQL label values; see [`STORE.md`](STORE.md) § PromQL API |
| `GET`/`POST` | `<ROUTE_PREFIX>/{tenant}/loki/api/v1/query_range`, `/labels`, `/label/{name}/values` | `200 application/json` (Loki envelope) | Loki logs read API; see [`STORE.md`](STORE.md) § Loki logs API |
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

## 15. `prism-alert` (PromQL ruler + notifier)

`cmd/prism-alert` is a per-tenant PromQL ruler with Alertmanager-compatible
webhook notification. Configuration is via environment variables (secrets via
the environment or a mounted file); a few non-secret operational fields also
accept flags that **override** the environment (`-listen`, `-rules-dir`,
`-store-base-url`, `-tenant`, `-notifier-url`, `-evaluation-interval`). This
table is the **authoritative, complete** reference — every variable read by
`Load()` in `internal/alert/config/config.go`. For the alerting contract, state
machine, and webhook payload see [`ALERTING.md`](ALERTING.md).

| Env | Type | Default | Meaning |
|---|---|---|---|
| `STORE_BASE_URL` | string (URL) | _(required)_ | prism-store query base URL (`scheme://host[:port]`). |
| `TENANT_NS` | string | _(required)_ | Tenant namespace this instance rules for; must match `^[a-z0-9][a-z0-9._-]{0,62}$`. |
| `ROUTE_PREFIX` | string | _(empty)_ | prism-store optional path prefix (matches its `ROUTE_PREFIX`). |
| `STORE_TOKEN_FILE` | string | _(empty — no auth header)_ | Path to a prism-store reader JWT, read fresh per request so rotation needs no restart. |
| `QUERY_HOT_ONLY` | bool | `true` | Tag every evaluation with the store's `hot_only` extension so recurring rules never scan cold Parquet tiers. Set `false` to allow full-range evaluation. |
| `NOTIFIER_WEBHOOK_URL` | string (URL) | _(required)_ | Notifier `/webhook` endpoint the v4 payload is POSTed to. |
| `WEBHOOK_SECRET` | string | _(required)_ | Bearer token presented to the notifier (`Authorization: Bearer …`). |
| `RECEIVER` | string | `tenant-webhook` | Receiver name stamped on every emitted payload. |
| `EXTERNAL_URL` | string | _(empty)_ | Payload `externalURL` (links a receiver back to a UI). |
| `RULES_DIR` | string | `/etc/prism-alert/rules` | Directory of Prometheus rule-group YAML (`*.yml` / `*.yaml`); a missing dir yields no rules (no crash). |
| `EVALUATION_INTERVAL` | duration | `60s` | Ruler evaluation cadence (must be `> 0`). |
| `GROUP_BY` | string (comma-separated) | `alertname,severity` | Labels alerts are grouped on before notifying (≥1 required). |
| `GROUP_WAIT` | duration | `30s` | Delay before the first notification for a new group (may be `0`). |
| `GROUP_INTERVAL` | duration | `5m` | Minimum spacing between notifications for a group once fired, when its alert set changes (`> 0`). |
| `REPEAT_INTERVAL` | duration | `4h` | How often an unchanged firing group re-notifies (`> 0`). |
| `RESOLVE_TIMEOUT` | duration | `5m` | `endsAt` horizon stamped on firing alerts so a receiver auto-resolves if updates stop (`> 0`). |
| `LISTEN_ADDR` | string | `:8080` | Health/probe HTTP bind; the ruler serves no query API. |

Durations parse with `time.ParseDuration` (e.g. `30s`, `5m`, `4h`). Validation
is total and names the offending variable; the process exits non-zero on any
invalid or missing required value before evaluating a single rule.

### HTTP routes

| Method | Path | Success | Failure |
|---|---|---|---|
| `GET` | `/healthz` | `200` body `ok\n` | — |
| `GET` | `/readyz` | `200` body `ready\n` | — |

The ruler exposes only these probes; it is a client of prism-store and the
notifier, not a server of alert data.

Graceful shutdown: `SIGINT` / `SIGTERM` → HTTP `Shutdown` (10s); the evaluation
loop and dispatcher stop on context cancellation.
