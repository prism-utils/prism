# Spec: time-range Parquet naming + summary-as-Parquet + metrics without summary

Status: READY

- **Slug / branch:** `feat/parquet-naming`
- **Owner phase:** orchestrator
- **PLAN phase(s):** Phase 9 — Outputs / packaging follow-up

## 1. Task

Every emitted artifact must be a Parquet file whose name encodes the window's
time range so a consumer can select files for a timestamp range without opening
them. Concretely: name files `<pipeline>-<phase>-<start>-<end>-<seq>.parquet`
where `phase` is the branch name (`raw` | `template` | `summary`) and
`start`/`end` are the window's UTC bounds in compact RFC3339-nano. Additionally,
the summary branch is now encoded as Parquet (not JSON), and the metrics
pipeline no longer produces a summary (raw Parquet only).

## 2. Scope

- **In scope:**
  - `internal/data`: add window/branch provenance to `RecordBatch`
    (`Start`,`End`) and `EncodedBlock` (`Pipeline`,`Branch`,`WindowStart`,`WindowEnd`).
  - `internal/buffer`: expose the window's start time so the runtime can stamp it.
  - `internal/pipeline`: stamp window start/end on the flushed batch, and
    stamp pipeline/branch/window on each `EncodedBlock` before output.
  - `internal/output/dir`: build the time-range file name from the block
    metadata; fall back to the legacy `<prefix><nanos>-<seq>` when metadata absent.
  - `configs/metrics.yaml`: drop the summary branch.
  - `configs/logging.yaml`: summary branch encodes Parquet; branch names become
    `raw`/`template`/`summary` (three-phase groundwork).
- **Out of scope:** the `logs` known-format parsers (PR-B), Arrow Flight (PR-C),
  benchmark changes (PR-D). No change to encoders' payload bytes.

## 3. Open questions  (resolved in Phase 0)

- [x] Q: Flight vs files? — A: additional `flight` output later; files stay (PR-C).
- [x] Q: k8s format meaning? — A: CRI (PR-B).
- [x] Q: naming scheme? — A: `<pipeline>-<phase>-<start>-<end>-<seq>.parquet`,
  compact UTC RFC3339-nano window bounds.

## 4. Decision log

- Encode the window time range in the file name (not only inside the file):
  - ref: Hive/Arrow dataset partitioning & time-partitioned layouts
    (https://arrow.apache.org/docs/python/dataset.html#partitioning) — consumers
    prune files by path metadata before reading footers.
  - perf: O(1) filename parse vs opening every Parquet footer to find the range;
    keeps range-scan planning cheap.
  - product: matches how object-store log/metric stores (Loki/Mimir chunks,
    S3 date-partitioned parquet) select files by time.
- Compact RFC3339-nano, `:`→`-`, UTC (`20260704T001122.5Z` style) so names are
  filesystem-safe and lexically sortable by time.
  - ref: RFC3339 / ISO8601 basic format — lexical sort == chronological sort.
  - perf: fixed-width, sortable; no parsing needed to order.
  - product: human-readable and tool-friendly (`ls` sorts by time).

## 5. Acceptance checklist

- [x] `data.RecordBatch` carries `Start`/`End`; `data.EncodedBlock` carries
      `Pipeline`/`Branch`/`WindowStart`/`WindowEnd`.
- [x] `buffer.Accumulator` exposes the current window start; pipeline stamps
      `Start`/`End` on each flushed window.
- [x] Pipeline stamps `Pipeline`/`Branch`/window on every `EncodedBlock`.
- [x] `output/dir` names files `<pipeline>-<phase>-<start>-<end>-<seq>.ext`,
      with a legacy fallback; names are filesystem-safe and time-sortable.
- [x] `configs/metrics.yaml` has no summary branch; `configs/logging.yaml`
      summary is Parquet with branch names raw/template/summary.
- [x] Tests written first (a `test:` commit precedes implementation).
- [x] `make full-tests` green locally.

## 6. Mandatory review gates

- [ ] **Gate 1 — Follows the guidelines**
- [ ] **Gate 2 — Tests cover edge cases** (zero window times → fallback; concurrent branch name collisions; empty window)
- [ ] **Gate 3 — Docs & comments match**
- [ ] **Gate 4 — Comments are atomic**
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
