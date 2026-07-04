# Spec: Benchmark log-template metrics + align harness/script to Parquet design

Status: ALL_OK

- **Slug / branch:** `feat/bench-log-templates`
- **Owner phase:** orchestrator
- **PLAN phase(s):** Phase 12 — Benchmarking / verification

## 1. Task

The benchmark last run did not surface log-template metrics (the "template X →
count Y" aggregates that are the whole point of the summary phase). Since PR-A,
summaries are **Parquet**, not JSON, and the metrics pipeline no longer has a
summary branch — but `prism-bench` still only reconciles JSON summaries and
`scripts/benchmark.sh` still emits the old JSON/`level`-grouped shape with the
pre-`logs`-parser config. Update the harness to read log-template metrics from
the summary Parquet (template + count columns) and report the top templates, and
update the script/k8s configs to the shipped three-phase logging design and the
summary-less metrics design.

## 2. Scope

- **In scope:**
  - `cmd/prism-bench`: read Parquet summaries whose schema has `template` +
    `count`; aggregate per-template counts across files; add report fields and
    render the top templates in the table + JSON. Tests (new `main_test.go`).
  - `scripts/benchmark.sh`: logs scenario → `logs` parser + three Parquet phases
    (raw/template/summary) matching `configs/logging.yaml`; metrics scenario →
    drop summary branch.
  - `deploy/k8s/prism-bench.yaml` + `run-cluster-bench.sh`: align to the same.
  - Rerun `make full-tests` and the local benchmarks; present results.
- **Out of scope:** new pipeline features; Flight in the benchmark path (durable
  Parquet is what we reconcile); in-cluster run if no cluster is reachable
  (fall back to local, note it).

## 3. Open questions  (resolved in Phase 0)

- [x] What are "log-template metrics"? → per-template counts from the summary
      Parquet (`template`→`count`), shown as top-N by count. (From the user's
      "log template X count Y" request.)
- [x] JSON summaries still supported? → yes, keep for back-compat; add Parquet.

## 4. Decision log

- Detect template summaries by **schema** (`template` + `count` columns), not by
  path, so it works regardless of directory layout or prefix.
  - ref: Parquet footer/schema read (apache/arrow-go `file.MetaData`) — schema is
    cheap to read without materializing row groups.
  - perf: only the matched summary files (tiny, one row per template) are
    materialized; raw/template phases are still footer-only row counts.
  - product: charts consume "template → count"; the benchmark should prove that
    aggregate exists and is correct, not just that bytes were written.

## 5. Acceptance checklist

- [x] `prism-bench` aggregates per-template counts from Parquet summaries and
      reports `template_groups`, `template_count_total`, and top templates.
- [x] JSON summary path still works (back-compat; `summaryStat` untouched).
- [x] `scripts/benchmark.sh` logs → three Parquet phases via `logs` parser;
      metrics → no summary branch.
- [x] k8s bench config/script aligned (three-phase logs, summary-less metrics).
- [x] Tests written first (`test:` commit `575c90f` precedes impl); `make
      full-tests` green.
- [x] Local + in-cluster benchmarks run; results presented with repro commands.

## 6. Mandatory review gates

- [x] **Gate 1 — Follows the guidelines**
- [x] **Gate 2 — Tests cover edge cases** (template summary Parquet,
      non-summary Parquet, mixed-tree aggregation; JSON path unchanged)
- [x] **Gate 3 — Docs & comments match** (script/config headers + k8s manifest
      describe the shipped three-phase/summary-less design)
- [x] **Gate 4 — Comments are atomic**
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

Verified end to end: local `scripts/benchmark.sh logs` and `metrics`, plus an
in-cluster run in `live-demo` (real demo-prometheus/clickhouse/elasticsearch
exporters + a 5k-line mixed log sample). Log-template metrics render (12
templates, top-N by count); metrics raw-only reconciled (79k rows, no summary);
in-pod prism held ~31m CPU / ~11 MiB.
