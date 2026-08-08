# Spec: segment delete grace (compacted sources outlive open readers)

Status: READY

- **Slug / branch:** `fix/segment-delete-grace`
- **Owner phase:** orchestrator → developer
- **PLAN phase(s):** store merge/retention lifecycle + query visibility

## 1. Task

The Grafana DuckDB datasource (`motherduck-duckdb-datasource`) fails repeatedly
against a prod tenant with `IO Error: Cannot open file
".../logs/logs-template/tiers/L0/<id>.parquet": No such file or directory`.
Grafana reads the PVC directly with `read_parquet('…/tiers/**/*.parquet')`:
DuckDB expands the glob at bind time, then opens each file during execution,
and prism's merge deletes the compacted L0 sources in between. DuckDB has no
`ignore_missing` for `read_parquet`, so one vanished path fails the whole
query — prism's own Loki/`/sql` path survives this only because it re-stats its
file list (`filterExistingLogFiles`), which a raw glob cannot do. Fix it on the
write side: a merge input keeps its bytes at its original path for a
configurable grace window after it stops being live, so a reader that already
resolved the path can still open it.

## 2. Scope

- **In scope:**
  - `LOGS_DELETE_GRACE_SECONDS` (default **120**, `0` = delete immediately —
    today's behavior, kept as the emergency escape hatch).
  - Logs merge (`ExecuteLogMerge`) and metrics merge (`ExecuteMerge`): on
    success, sources are **retired**, not unlinked — a `<segment>.compacted`
    sidecar records the delete deadline and the segment bytes stay put.
  - Retired segments are not live anywhere: log landing/tier scans, metrics tier
    scans, the logs catalog (Loki + `/sql`), the metrics source list, and the
    manifest rebuild all skip a segment that carries a marker. They cannot be
    merge inputs again and prism never double-counts their rows.
  - Purge: the merge tick unlinks segment + marker once the deadline passes,
    and reaps orphan markers.
  - Wiring: chart values + statefulset env + golden fixture, `CONFIG.md`,
    `STORE.md`.
- **Out of scope:**
  - Changing what Grafana queries (the dashboards live outside this repo). A
    manifest-driven read path for Grafana is the durable fix and is a follow-up.
  - Rollup and retention deletes: those unlink files that were live until that
    moment, which is a different (and far rarer, daily-rate) event.
  - Landing windows deleted by `MAX_LOG_FILES` / `RETENTION_DAYS`.

## 3. Open questions  (resolved before READY)

- [x] Q: Rename compacted sources into a trash dir instead of holding them in
  place? — A: **No.** The failing reader already resolved the *old* path; a
  rename removes that path just as an unlink does, so it fixes nothing.
- [x] Q: Hardlink the bytes elsewhere and unlink the tier path? — A: **No**, for
  the same reason — the reader opens by path, not by inode.
- [x] Q: Hide the source from future merges by renaming it to a dotfile? — A:
  **No**, same rename problem. Mark it with a **sidecar** instead and leave the
  segment name untouched.
- [x] Q: Grace default? — A: **120s** (user), `0` disables.
- [x] Q: Same treatment for metrics merges? — A: **Yes** — the shipped example
  datasource globs `tiers/L0/*.parquet`, so the race is identical.
- [x] Q: Where does the purge run? — A: the **merge tick** (`MERGE_TICK_SECONDS`,
  default 60s). The retention tick is hourly, which would stretch a 120s grace
  to an hour of retained bytes.

## 4. Decision log  (Decision Protocol)

- **Hold compacted sources in place for a grace window (Lucene
  `IndexDeletionPolicy` model), rather than any rename/hardlink scheme.**
  - ref: https://github.com/duckdb/duckdb/discussions/13438 — `read_parquet` has
    no `ignore_missing`; the request is open since Aug 2024 with no
    implementation, so a reader-side fix is unavailable to Grafana. Reinforced
    by the reply noting a list of explicit paths is not even existence-checked.
  - ref: https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/index/IndexDeletionPolicy.html
    — the documented remedy for a store without "delete on last close" (their
    case: NFS) is a policy where "a commit is only removed once it has been
    stale for more than X minutes", to "give your readers time to refresh to
    the new commit before IndexWriter removes the old commits", explicitly
    trading storage for reader safety. This is the same trade, one config knob.
  - perf: bounded extra disk — at most one grace window of compaction output
    (grace ÷ merge tick ≈ 2 ticks of sources), released on the merge tick. No
    extra scan: the marker set is built from the `ReadDir` listing each scanner
    already performs, so the skip is a map lookup, not a `stat` per file.
  - product: the alternative shapes all break the reader that has already
    resolved a path (rename and hardlink-then-unlink both remove it), and the
    only reader-side fix does not exist in DuckDB. Holding the bytes is the one
    option that keeps an in-flight Grafana query working, and it degrades to
    today's behavior at `0` for an operator who needs the space back now.

- **Sidecar marker `<segment>.compacted`, not a queue file or a rename.**
  - ref: https://www.elastic.co/guide/en/elasticsearch/reference/8.19/near-real-time.html
    — ES separates *refresh* (what readers can see) from *merge* (what is on
    disk); prism already models the refresh half, and a marker is what lets the
    two halves disagree for a bounded window: on disk but not searchable.
  - perf: one small write per compacted source (already an I/O-heavy path), and
    a purge that reads a handful of bytes. A central queue file would need
    locking across ticks and would strand its entries whenever a directory is
    removed out from under it.
  - product: self-describing and self-healing — the marker lives beside the
    file it retires, so a copied, restored, or manually-pruned data directory
    stays consistent, and an operator can see what is being held and until when
    with `ls`. Corrupt or unreadable markers purge (fail toward reclaiming
    space, never toward a leak); an orphan marker is reaped.

- **Purge on the merge tick, not the retention tick.**
  - ref: same Lucene policy doc — the deletion policy is evaluated on the
    writer's own cadence (`onCommit`), not on a separate slow janitor, so the
    retained set stays proportional to the configured window.
  - perf: the merge tick already walks every tenant's tier directories, so the
    purge adds one `ReadDir` per segment directory to a pass that is dominated
    by DuckDB `COPY`.
  - product: with the hourly retention tick a 120s grace would hold bytes for
    up to an hour, which is a storage surprise an operator did not configure.

- **Accepted trade-off: rows are duplicated for readers that glob the tree,
  for the length of the grace window.**
  - ref: the same Lucene note — "while the snapshot is held, the files it
    references will not be deleted, which will consume additional disk space".
    Holding a source and publishing its merge output means both exist.
  - perf: no cost to prism's own queries — the catalog, the manifest and the
    metrics source list all skip retired segments, so `/sql`, Loki and PromQL
    count every row exactly once.
  - product: a raw `**/*.parquet` glob has no way to learn which files are
    retired, so a Grafana panel can show a compacted line twice for up to
    `LOGS_DELETE_GRACE_SECONDS`. That is strictly better than the panel
    erroring out entirely, it is bounded and configurable, and the durable fix
    (point Grafana at a manifest-driven relation instead of a glob) is a
    follow-up that this change does not block. Documented in `STORE.md` so the
    behavior is not a surprise.

## 5. Acceptance checklist  (developer checks these off)

- [ ] After a successful `ExecuteLogMerge`, every source is still openable at
      its original path, and a `<source>.compacted` marker records the deadline.
- [ ] Same for the metrics `ExecuteMerge`.
- [ ] `LOGS_DELETE_GRACE_SECONDS=0` restores immediate deletion (no marker left).
- [ ] Retired segments are not merge inputs again: `ScanLogLanding`,
      `ScanLogTier`, and `ScanTier` skip them.
- [ ] Retired segments are not searchable twice: the logs catalog, the metrics
      source list, and `RebuildManifest` skip them.
- [ ] The merge tick unlinks segment + marker once the grace expires, and reaps
      a marker whose segment is already gone.
- [ ] A marker with unreadable/corrupt contents purges rather than leaking.
- [ ] `LOGS_DELETE_GRACE_SECONDS` (default 120) is wired through main → lifecycle
      → both executors; chart values, statefulset env, and the golden fixture agree.
- [ ] `CONFIG.md` + `STORE.md` document the knob, the semantics, and the
      duplicate-rows-for-globbing-readers trade-off.
- [ ] Tests written first (a `test:` commit precedes implementation) — CONTRIBUTING.md §1
- [ ] `make lint test` green locally (+ `make full-tests` if I/O/encoding/wiring touched)

## 6. Mandatory review gates  (reviewer owns — unchecks with a reason on failure)

Definitions live in docs/REVIEW.md ("Mandatory gates"); do not restate them here.

- [ ] **Gate 1 — Follows the guidelines** (CONTRIBUTING.md + DESIGN.md)
- [ ] **Gate 2 — Tests cover edge cases** (TESTING.md: failure paths, boundaries, empty/oversized, cancellation, Validate rejection)
- [ ] **Gate 3 — Docs & comments match the task and the delivered code** (no drift)
- [ ] **Gate 4 — Comments are atomic** — none reference another code location (CONTRIBUTING.md §3.8)
- [ ] Full docs/REVIEW.md checklist passes

## 7. Reviewer notes

_(empty until first review)_
