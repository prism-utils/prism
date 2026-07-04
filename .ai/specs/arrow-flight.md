# Spec: Arrow Flight output + `prism collect` receiver

Status: ALL_OK

- **Slug / branch:** `feat/arrow-flight`
- **Owner phase:** orchestrator
- **PLAN phase(s):** Phase 9 — Outputs / transport

## 1. Task

Add Apache Arrow Flight as a columnar network transport so the same window
batches can be streamed to a server that ingests them directly into columnar
storage (no row-by-row re-parse), saving server CPU/memory. Deliver it as an
**additional** per-branch `flight` output (client `DoPut`) alongside the durable
parquet `dir` outputs, plus a minimal `prism collect` receiver (a Flight server)
that persists incoming batches to time-range-named Parquet, so the transport is
testable end-to-end.

## 2. Scope

- **In scope:**
  - `internal/encoder/arrow`: RecordBatch → Arrow IPC stream bytes.
  - `internal/output/flight`: read the IPC bytes and `DoPut` the records to a
    Flight endpoint; carry pipeline/branch/window in the `FlightDescriptor`.
  - `internal/collect`: a Flight server whose `DoPut` handler writes received
    records to Parquet via the existing `dir` naming (`<pipeline>-<phase>-<start>-<end>-<seq>.parquet`).
  - `prism collect -addr … -dir …` subcommand.
  - Register the `arrow` encoder + `flight` output; add an example config.
  - Dependency: `github.com/apache/arrow-go/v18/arrow/flight` (+ grpc).
- **Out of scope:** auth/TLS (insecure only for this cut), Flight `GetFlightInfo`/
  `DoGet` read path, benchmark changes (PR-D).

## 3. Open questions  (resolved in Phase 0)

- [x] Flight role → additional `flight` output + ship `prism collect`; files stay.
- [x] Replace files? → no; Flight coexists with parquet `dir`.

## 4. Decision log

- Real Flight (records over `DoPut`), not opaque bytes: the `arrow` encoder emits
  IPC, the `flight` output reframes it as FlightData records.
  - ref: Arrow Flight spec / DoPut
    (https://arrow.apache.org/docs/format/Flight.html) — the columnar wire
    protocol analytics servers ingest natively.
  - perf: columnar batches ingested without transform; grpc streaming with
    backpressure. IPC encode→reframe is cheap vs the network + server-parse it
    saves.
  - product: standard interop (DuckDB/others speak Flight); a shipped agent
    should speak the ecosystem's columnar transport.
- Receiver reuses the parquet encoder + `dir` output so artifacts are named
  identically to the local sink (range-selectable), avoiding a second naming
  scheme.
- grpc dependency accepted: Flight is grpc-based; there is no Flight without it.

## 5. Acceptance checklist

- [x] `arrow` encoder (IPC stream) with tests (roundtrip, empty batch).
- [x] `flight` output: validates `addr`; `DoPut`s records with a
      pipeline/branch/window descriptor; releases buffers.
- [x] `internal/collect` Flight server writes received batches to time-range
      Parquet using the shared `dir` naming.
- [x] `prism collect -addr -dir` subcommand.
- [x] `arrow` + `flight` registered; example `configs/metrics-flight.yaml`.
- [x] e2e: pipeline → `flight` → `collect` → Parquet on the receiver, verified.
- [x] Tests written first; `make full-tests` green.

## 6. Mandatory review gates

- [x] **Gate 1 — Follows the guidelines** (factory pattern, error wrapping,
      lifecycle hooks; `collect` reuses `output/dir` naming by design)
- [x] **Gate 2 — Tests cover edge cases** (empty window/block, descriptor
      roundtrip incl. nil/short/zero, full output→collect round-trip with
      allocator leak check, graceful-shutdown-returns-nil assertion)
- [x] **Gate 3 — Docs & comments match** (DESIGN §9 covers arrow/flight/collect)
- [x] **Gate 4 — Comments are atomic**
- [x] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

Bugbot review (branch `feat/arrow-flight`): functionally complete for scope.
Addressed before merge:
- Added direct output→collect integration tests (`TestOutput_Consume_RoundTrip`,
  `TestOutput_Consume_EmptyBlock`) with a `CheckedAllocator` leak assertion,
  covering the buffer-release and empty-block gaps.
- Locked in graceful shutdown: the receiver `Serve` returns nil after ctx
  cancel (per Flight `Server.Shutdown` contract), asserted in the test cleanup.
- Descriptor provenance fallbacks (client `unknown/0`, server `flight/raw`) are
  edge-only; the happy path always carries four segments (asserted in e2e).
Deferred (documented, out of scope for this cut): auth/TLS, Flight read path
(`DoGet`), reconnect on mid-run server death, streaming (non-buffered) ingest.
