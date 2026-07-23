# Spec: benchmark — resource-usage measurement (CPU / memory / disk I/O)

Status: IN_REVIEW

- **Slug / branch:** `feat/bench-resource-usage`
- **Owner phase:** orchestrator → developer
- **Relates to:** the ClickHouse benchmark (PR #42). Follow-up requested by the user.

## 1. Task

Extend the `bench/` harness to **sample actual resource usage while each workload
runs** — CPU, memory, and disk I/O (throughput + IOPS) — and add it to
`bench/RESULTS.md`, `bench/results.json`, and the `README.md` benchmark section.
Today we only report the *host spec* (allocated HW), which overstates footprint
because not all allocated HW is used. The method must be **easy and reproducible**
by anyone cloning the repo (Docker engine stats for the container; per-process
sampling for the embedded engine).

## 2. Scope

- **In scope** (`bench/`):
  - **New `bench/internal/monitor` package** with two samplers behind a small interface, each producing a `Usage` struct:
    - `Usage{ CPUCoresMean, CPUCoresPeak float64; RSSPeakBytes uint64; ReadBytes, WriteBytes uint64; ReadOps, WriteOps uint64; DurationSec float64 }` plus derived helpers (`ReadMiBPerSec`, `IOPS = (ReadOps+WriteOps)/DurationSec`).
    - **`ProcSampler`** — samples a target OS process (and children) at a fixed interval (≈50–100 ms) using `github.com/shirou/gopsutil/v3` (CPU%, RSS always; per-process I/O counters where the platform exposes them — Linux `/proc/<pid>/io`). CPU cores = cpu-time-delta / wallclock-delta.
    - **`DockerSampler`** — samples a container's cgroup stats via the **Docker Engine API** (`GET /containers/{id}/stats?stream=false`) over the Docker socket (resolve from `DOCKER_HOST`, else the platform default), parsing `cpu_stats`/`precpu_stats` (→ cores), `memory_stats.usage` (→ RSS), and `blkio_stats.io_service_bytes_recursive`/`io_serviced_recursive` (→ read/write bytes + ops → IOPS). Dependency-light (stdlib HTTP over the socket). Degrade gracefully: if the socket is unreachable, fall back to `docker stats --no-stream` for CPU/mem and mark IOPS `n/a`.
  - **Wire sampling into `bench/cmd/prism-bench`** — for each workload, start the sampler for the correct target before the timed section and stop it after, recording a `Usage` on the `Workload`:
    - **prism-store ingest** → sample the **store binary process** (add a `Pid()`/process accessor to the store driver).
    - **prism-store count / aggregation / logs LIKE** → the store uses an **embedded DuckDB engine inside the benchmark process**, so sample **this process** (`os.Getpid()`). Label this clearly (see honesty note): resource usage reflects the embedded engine executing the query, which is the store's actual architecture (no separate query server).
    - **all ClickHouse workloads** → sample the **ClickHouse container**.
    - Sample over the full K-run timed window (warm run excluded, consistent with latency).
  - **Extend the results schema** (`bench/internal/results`): add a `Usage` field to `Workload` (all fields `omitempty`), render a **"Resource usage" table** per workload/system in `RESULTS.md` (CPU cores mean/peak, peak RSS MiB, read+write MiB and MiB/s, IOPS where available), and mirror a compact version in `README.md`. Renderer must handle missing/`n/a` metrics cleanly.
  - **Docs (`bench/README.md`)**: document the measurement method, the sampling interval, the per-workload target for each system, why per-container/per-process attribution is used instead of host-level `node_exporter` (host-level conflates both systems + OS), and the honest caveats:
    - store queries are sampled on the embedded-engine process (the benchmark process), not a separate server — that IS the store's model;
    - per-process disk **IOPS** is available on **Linux** (`/proc/<pid>/io`); on macOS/Windows the process sampler reports CPU/mem and marks IOPS `n/a` — so the canonical full-I/O comparison (both systems incl. IOPS) is captured on Linux, while CPU/mem are captured on any host. The ClickHouse container reports IOPS on all Docker platforms (Linux VM cgroup).
  - **README benchmark section**: add the resource-usage table next to the latency table, keep the honest interpretation, and re-state the environment.
  - **Re-run `make bench`** and publish the real measured resource numbers alongside the timings.
- **Out of scope:** containerizing prism-store (its query engine is embedded by design); Prometheus/node_exporter wiring (documented as intentionally not used — per-process/per-container attribution is more accurate and needs no extra services); changing the workloads themselves or the fairness methodology from PR #42.

## 3. Open questions  (resolved before READY)

- [x] Docker engine stats, node_exporter, or Prometheus? → **Docker Engine API per-container** for ClickHouse + **per-process sampler** for the embedded store engine. Rationale: exact per-app attribution with no extra services; `node_exporter`/Prometheus measure the whole host and would conflate both systems + OS. Documented.
- [x] What do we sample for the store's queries, given they run in-process? → the **benchmark process** (the embedded engine is the store's real query path). Documented as such — no separate server exists to sample.
- [x] IOPS on macOS for the local process? → not exposed per-process by the OS; report CPU/mem there and mark IOPS `n/a`, and state that the full cross-system IOPS comparison is captured on Linux. Container IOPS works everywhere via Docker cgroup stats.
- [x] Report IOPS or throughput? → **both**: read/write MiB (+ MiB/s) always, and IOPS (ops/s) where op-counts are exposed. Throughput is the more meaningful disk metric for analytical scans; IOPS included per the request.
- [x] New dependency? → `gopsutil/v3` for portable per-process CPU/mem/IO (bench-only; must not enter the agent build). Docker side uses stdlib over the socket (no SDK dep).

## 4. Decision log  (Decision Protocol)

- **Per-container (Docker API) + per-process (gopsutil) sampling over host-level node_exporter.**
  - ref: Docker stats API — https://docs.docker.com/reference/api/engine/ (container `stats`); gopsutil — https://github.com/shirou/gopsutil ; cgroup blkio semantics.
  - perf: sampler overhead is negligible at 50–100 ms polling; product: attributes CPU/mem/I/O to the *specific* system under test, which host-level metrics cannot.
- **Sample the embedded-engine process for store queries.**
  - ref: the store's query path is an embedded DuckDB engine (`internal/store/engine`, #24/#26) — there is no separate query server process.
  - perf: n/a; product: measuring the embedding process is the only faithful way to attribute the store's query cost; documented so no one mistakes it for a server.
- **Report throughput always, IOPS where the platform exposes op-counts.**
  - ref: Linux `/proc/<pid>/io` (`syscr`/`syscw`/`read_bytes`/`write_bytes`); container `blkio_stats.io_serviced_recursive`.
  - perf: n/a; product: honest, portable disk metric; IOPS added on capable platforms.

## 5. Acceptance checklist  (developer checks these off)

- [x] `bench/internal/monitor` provides `ProcSampler` and `DockerSampler` behind a common interface, each returning `Usage` with CPU (mean/peak cores), peak RSS, read/write bytes+ops, and duration; unit-tested (a synthetic CPU/alloc burn asserts non-zero CPU/RSS for the process sampler; a fake stats source or a live container asserts the Docker parser math).
- [x] `prism-bench` samples the correct target per workload (store binary for ingest; benchmark process for store queries; ClickHouse container for all CH workloads) over the timed window; store driver exposes its process id.
- [x] `Workload.Usage` added to the schema; `RESULTS.md` renders a resource-usage table; `results.json` carries the usage; `README.md` benchmark section shows a compact resource table.
- [x] Renderer handles absent metrics (`n/a`) without breaking alignment; `results.RenderMarkdown` unit-tested for a row with and without IOPS.
- [x] `bench/README.md` documents the method, interval, per-workload targets, the node_exporter rationale, and the macOS-IOPS + embedded-engine caveats.
- [x] `make bench` re-run on this host; `RESULTS.md`/`results.json`/`README.md` updated with the REAL measured CPU/mem/I/O numbers; latency tables + correctness gates unchanged and still pass.
- [x] `gopsutil` is bench-only: `CGO_ENABLED=0 go build ./cmd/prism` still passes and does NOT import gopsutil (verify the agent import graph); `go build ./cmd/prism-store` passes; `make tidy` clean.
- [x] `make lint test` (`-race`) green; no committed datasets/blobs; sampler goroutines stop cleanly (no leaks).

## 6. Mandatory review gates  (reviewer owns)

- [ ] **Gate 1 — Guidelines:** samplers behind a small interface, no globals, goroutine lifecycle tied to context/stop, errors wrapped; Docker socket resolution handles `DOCKER_HOST`; graceful degradation documented in code; comments self-contained.
- [ ] **Gate 2 — Edge cases:** container missing/not-ready; Docker socket absent (fallback path); process exits mid-sample; zero-duration workload; a metric unsupported on the host → `n/a` (not a crash or a fake 0); sampler adds negligible skew to the latency numbers (sampling must not materially inflate the timings it runs alongside — verify timings are consistent with PR #42).
- [ ] **Gate 3 — Docs/comments match code:** RESULTS/README tables match the schema and the units rendered; the caveats (embedded-engine sampling, macOS IOPS n/a) are accurate; the published numbers came from the committed `make bench` run.
- [ ] **Gate 4 — Atomic comments** (§3.8): none reference another file/symbol.
- [ ] **Honesty/attribution audit:** confirm each workload samples the process/container that actually does the work; the store-query self-sampling is disclosed and not presented as a separate server; no metric is fabricated when unavailable.
- [ ] Full docs/REVIEW.md checklist; TESTING.md layering (monitor + renderer unit tests; full bench opt-in).

## 7. Reviewer notes

_(empty until first review)_
