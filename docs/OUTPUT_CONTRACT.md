# prism — Output Protocol (Artifact Contract)

> **Contract version: `v1`.** This document freezes the artifact taxonomy,
> naming, Flight descriptor, and per-phase Parquet schemas that downstream
> consumers (the homelab-apps proxy sidecar + DuckDB loader) build against. It
> is a **compatibility surface**: additive changes bump the minor contract
> version; a breaking change to any frozen field bumps the major version and is
> called out here. If code and this doc disagree, that is a bug — reconcile.

Consumers should pin a contract version and treat unknown extra columns as
forward-compatible (ignore, don't fail).

---

## 1. Artifact taxonomy (pipeline × phase)

Every artifact is one **window** of one **branch** ("phase") of one pipeline.
The four artifact types a v2 consumer expects:

| Artifact type   | Produced by (pipeline / branch)                | Payload |
|-----------------|------------------------------------------------|---------|
| `metrics-raw`   | metrics pipeline, `raw` branch                 | Parquet |
| `logs-raw`      | logging pipeline, `raw` branch                 | Parquet |
| `logs-template` | logging pipeline, `template` branch            | Parquet |
| `logs-summary`  | logging pipeline, `summary` branch             | Parquet |

The **phase** is the pipeline's **branch name** (`raw` | `template` |
`summary`). The artifact type is `<domain>-<phase>`; `<domain>` (`metrics` /
`logs`) is a deployment convention carried by the pipeline name. There is
deliberately **no** `metrics-summary` phase (server-side analytics aggregate the
columnar `metrics-raw` directly; see DESIGN.md §7).

---

## 2. Naming (frozen)

### 2.1 File artifacts (`dir` output, and files the `collect` receiver writes)

```
<pipeline>-<phase>-<start>-<end>-<seq>.<ext>
```

- `<pipeline>` / `<phase>`: the producing pipeline and branch names; any
  character outside `[A-Za-z0-9_.]` is mapped to `-`.
- `<start>` / `<end>`: the flushed window's UTC bounds in a compact, fixed-width,
  **lexically-sortable** layout: `20060102T150405.000000000Z` (basic ISO-8601,
  9-digit nanoseconds, `Z`). A consumer selects files for a time range by name
  alone, without opening Parquet footers.
- `<seq>`: a monotonically increasing per-output counter that disambiguates
  windows sharing bounds.
- `<ext>`: the encoder format (`parquet` for all four artifact types).

Absent window provenance, names fall back to the legacy `<nanos>-<seq>.<ext>`.

### 2.2 Flight descriptor path (frozen)

The `flight` output encodes provenance in the `FlightDescriptor` **PATH**:

```
[ pipeline, branch, startUnixNano, endUnixNano ]
```

- `pipeline`, `branch`: strings (`"unknown"` if absent).
- `startUnixNano`, `endUnixNano`: decimal Unix-nanosecond strings; `"0"` for a
  zero/absent bound.

The `collect` receiver decodes this path and writes a file named per §2.1, so
Flight-delivered and locally-written artifacts are named identically.

---

## 3. Per-phase Parquet schemas (frozen, versioned)

Column order is not significant; consumers must select by name. Types are Arrow
logical types (Parquet physical types follow arrow-go's default mapping).

### 3.1 `metrics-raw`

Emitted by the `prometheus` parser. One row per exposition sample.

| Column         | Type    | Notes |
|----------------|---------|-------|
| `__name__`     | string  | metric name |
| `labels`       | string  | canonical label set, verbatim between `{}` (may be empty) |
| `value`        | float64 | sample value |
| `timestamp_ms` | int64   | sample timestamp in ms; `0` when the exporter omits it |

### 3.2 `logs-raw`

Emitted by the `logs` parser. Two columns are **guaranteed** on every row:

| Column    | Type   | Notes |
|-----------|--------|-------|
| `message` | string | the normalized, templatable log text |
| `format`  | string | which shape produced the row (`none`/`k8s`/`json`/`syslog`/`clf`/`cef`) |

For a **known format** the parser may add extra **string** columns with the
format's extracted fields (e.g. `stream` for k8s, structured keys for json).
Timestamp fields are **never** ingested (storage stamps ingest time). Consumers
must tolerate these additional string columns.

### 3.3 `logs-template`

`logs-raw` columns **plus**:

| Column     | Type   | Notes |
|------------|--------|-------|
| `template` | string | mined, stable template key (variable tokens masked) |

### 3.4 `logs-summary`

Emitted by the `summary` processor. One row per group.

| Column            | Type    | Notes |
|-------------------|---------|-------|
| `<group_by[i]>`   | string  | one column per configured `group_by` key (e.g. `template`) |
| `count`           | int64   | rows in the group (present when `count` is requested) |
| `<fn>_<field>`    | float64 | one per non-count aggregate: `sum_`, `avg_`, `min_`, `max_`, `pNN_` |

Example: `group_by: [template]`, `aggregates: [count]` →
columns `template` (string), `count` (int64).

---

## 4. Schema evolution rules

- **Additive within a major:** new columns may appear; consumers ignore unknown
  columns. Guaranteed columns above never change type within a major version.
- **Renames/type changes are breaking:** they bump the contract major version and
  are documented here with a migration note.
- The DuckDB loader should read columns by name with explicit casts and treat
  missing optional columns as null, so a mixed fleet of agent versions is safe.

---

## 5. Change log

- **v1** (initial freeze): taxonomy (`metrics-raw`, `logs-raw`, `logs-template`,
  `logs-summary`), file + Flight descriptor naming, and the four per-phase
  schemas above.
