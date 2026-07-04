# Spec: logs parser with known formats + no field-guessing default

Status: READY

- **Slug / branch:** `feat/logs-known-formats`
- **Owner phase:** orchestrator
- **PLAN phase(s):** Phase 4 — Parsers

## 1. Task

For logging we must not guess fields. Add a `logs` parser that keeps the raw
message verbatim by default and only extracts fields when the line is in a known
format — `k8s` (CRI container-log), `json`, `syslog` (RFC3164/5424), `clf`
(Common Log Format), `cef` (Common Event Format) — chosen explicitly or by
auto-discovery. Every row exposes a stable `message` column (the templatable
text) and a `format` column; timestamp-like fields are never ingested. This
enables the flow: unparsed → summarize on template only; known format →
summarize on the extracted fields and template the message.

## 2. Scope

- **In scope:**
  - New `internal/parser/logs` package (`Type: "logs"`), registered in
    `internal/components`.
  - `format` option: `none` (default) | `auto` | `k8s` | `json` | `syslog` |
    `clf` | `cef`. `message` option names the templatable column (default
    `message`).
  - Per-format field extraction with timestamp fields dropped; `auto` sniffs the
    format per line and falls back to `none`.
  - Update `configs/logging.yaml` to use `logs` (format `auto`, template on
    `message`). Add a known-format example config demonstrating field summary.
  - DESIGN.md parser note.
- **Out of scope:** Arrow Flight (PR-C), benchmark changes (PR-D). No change to
  the buffer/summary/template processors themselves.

## 3. Open questions  (resolved in Phase 0)

- [x] k8s meaning → CRI container-log line `<ts> <stream> <F|P> <msg>`.
- [x] auto-discovery required → yes, per line, with `none` fallback.

## 4. Decision log

- Always emit a normalized `message` column so downstream `template`/`summary`
  are format-agnostic (source = `message`).
  - ref: Vector `parse_*`/OpenTelemetry log data model both normalize to a body
    field (https://opentelemetry.io/docs/specs/otel/logs/data-model/) — a stable
    body/message is the canonical templatable field.
  - perf: one string column, no reflection; template source is fixed.
  - product: operators write one summary (group by template) that works for all
    formats, and add parsed fields only where they exist.
- Never ingest timestamp fields from a known format.
  - ref: OTel log data model separates Timestamp from Attributes; storage stamps
    its own ingest time — a per-line high-cardinality timestamp is useless as a
    summary dimension and bloats columns.
  - perf: fewer/narrower columns; better dictionary/RLE in parquet.
  - product: matches how log stores key on ingest time, not the noisy embedded ts.
- Reuse `internal/columnar.Build` for row→batch (type inference gives int64
  status/size for free).

## 5. Acceptance checklist

- [x] `logs` parser with the six formats + `auto`, registered in components.
- [x] `none`/unknown keeps the raw line as `message`; no fields guessed.
- [x] Known formats extract documented fields; `message` normalized; no
      timestamp column ingested.
- [x] `auto` sniffs per line and falls back to `none`.
- [x] `configs/logging.yaml` uses `logs` (auto) + template on `message`; a
      known-format example config added.
- [x] Tests written first (a `test:` commit precedes implementation).
- [x] `make full-tests` green locally.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (malformed lines, empty batch, auto fallback, timestamp drop, custom message column)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
