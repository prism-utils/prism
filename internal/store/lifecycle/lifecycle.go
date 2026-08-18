package lifecycle

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/logmeta"
	"github.com/prism-utils/prism/internal/store/materialize"
	"github.com/prism-utils/prism/internal/store/merge"
	"github.com/prism-utils/prism/internal/store/metricsmeta"
	"github.com/prism-utils/prism/internal/store/promote"
	"github.com/prism-utils/prism/internal/store/rollup"
	"github.com/prism-utils/prism/internal/store/seed"
	"github.com/prism-utils/prism/internal/store/segformat"
	"github.com/prism-utils/prism/internal/store/stats"
)

// Job names the maintenance pass an observation belongs to. The set is closed
// so a metric label built from it stays bounded.
const (
	JobHotSnapshot = "hot_snapshot"
	JobFlush       = "flush"
	JobMerge       = "merge"
	JobPromote     = "promote"
	JobRetention   = "retention"
)

// Recorder receives lifecycle observations. File counts are handed over from
// scans a tick already performed, which is why an exporter built on this never
// walks the data directory at scrape time. Implementations must not block.
type Recorder interface {
	ObserveTick(job string, d time.Duration, err error)
	ObserveTickStart(job string)
	ObserveTierSegments(tenant string, files int)
	ObserveLogLandingFiles(tenant, artifact string, files int)
	ObserveCompactionSeconds(tenant string, seconds float64)
	ObservePromote(attempts, successes, retries int, bytes int64, tmpFiles int)
}

// nopRecorder keeps every tick path free of nil checks.
type nopRecorder struct{}

func (nopRecorder) ObserveTick(string, time.Duration, error)   {}
func (nopRecorder) ObserveTickStart(string)                    {}
func (nopRecorder) ObserveTierSegments(string, int)            {}
func (nopRecorder) ObserveLogLandingFiles(string, string, int) {}
func (nopRecorder) ObserveCompactionSeconds(string, float64)   {}
func (nopRecorder) ObservePromote(int, int, int, int64, int)   {}

// Config drives background merge and retention passes.
type Config struct {
	DataDir              string
	SegmentsPerTier      int
	MaxSegmentBytes      int64
	FloorBytes           int64
	RetentionDays        int
	MaxLogFiles          int // 0 = disabled
	RollupSteps          string
	MaxTier              int
	Threads              int
	MemoryLimit          string
	MergeSegmentFormat   segformat.Format
	DuckDBStorageVersion string
	// LogsRefreshInterval is the searchable lag budget for logs: once the oldest
	// buffered landing window reaches this age, the merge tick opens it into a
	// tier. Zero keeps refreshes count-triggered only.
	LogsRefreshInterval time.Duration
	// LogsRefreshMaxActions caps landing refreshes per artifact per merge tick.
	LogsRefreshMaxActions int
	// DeleteGrace holds a merged-away source at its original path for this long
	// before it is unlinked, so a reader that resolved the path while it was
	// still live can finish opening it. Zero deletes as soon as the merge
	// output is durable.
	DeleteGrace time.Duration
	// ColdDir is a second data root for compacted L1+ segments. Empty disables
	// promote and dual-root scans.
	ColdDir string
	// ColdAfter is how old a compacted segment's max timestamp must be before
	// it may leave the hot root. Zero means twelve hours.
	ColdAfter time.Duration
	// Materializations are named merge-time SQL artifacts. Empty is a no-op.
	Materializations materialize.File
	// Logger records per-tenant / per-file errors; nil uses slog.Default.
	Logger *slog.Logger
	// Recorder receives tick outcomes and file counts; nil discards them.
	Recorder Recorder
}

// Runner executes flush, merge, and retention on a ticker schedule.
type Runner struct {
	cfg   Config
	eng   *engine.Engine
	clock func() time.Time
	log   *slog.Logger
	rec   Recorder
}

// NewRunner builds a lifecycle runner.
func NewRunner(cfg *Config, eng *engine.Engine, now func() time.Time) *Runner {
	if cfg == nil {
		cfg = &Config{}
	}
	c := *cfg
	if now == nil {
		now = time.Now
	}
	if c.MaxTier <= 0 {
		c.MaxTier = 8
	}
	log := c.Logger
	if log == nil {
		log = slog.Default()
	}
	rec := c.Recorder
	if rec == nil {
		rec = nopRecorder{}
	}
	return &Runner{cfg: c, eng: eng, clock: now, log: log, rec: rec}
}

// observed runs one maintenance pass and reports how it went. Wall time is read
// from the wall clock rather than the injected clock so a test clock that never
// advances still yields honest durations.
func (r *Runner) observed(job string, pass func() error) error {
	r.rec.ObserveTickStart(job)
	start := time.Now()
	err := pass()
	r.rec.ObserveTick(job, time.Since(start), err)
	return err
}

// TickHotSnapshot exports near-real-time hot parquet snapshots for Grafana.
func (r *Runner) TickHotSnapshot() error {
	return r.observed(JobHotSnapshot, r.eng.ExportHotSnapshots)
}

// TickFlush rolls hot tables whose window elapsed and seals aged log coalesce
// buffers. Sealing shares the flush cadence because a buffer that never seals
// keeps its rows out of every query.
func (r *Runner) TickFlush() error {
	return r.observed(JobFlush, r.tickFlush)
}

func (r *Runner) tickFlush() error {
	if err := r.eng.FlushDue(); err != nil {
		return err
	}
	return r.eng.FlushLogCoalesce()
}

// TickMerge plans and executes one merge pass per tenant with tier segments.
// Per-tenant failures are logged and skipped so one bad tenant cannot block others.
func (r *Runner) TickMerge() error {
	return r.observed(JobMerge, r.tickMerge)
}

func (r *Runner) tickMerge() error {
	tenants, err := listTenants(r.cfg.DataDir)
	if err != nil {
		return err
	}
	planner := merge.NewPlanner(merge.PlannerConfig{
		SegmentsPerTier: r.cfg.SegmentsPerTier,
		// MaxMergeAtOnce 0 → derive from MaxSegmentBytes/FloorBytes so tiny
		// unsealed segments pack toward the seal budget in one action.
		MaxMergeAtOnce:        0,
		MaxSegmentBytes:       r.cfg.MaxSegmentBytes,
		FloorBytes:            r.cfg.FloorBytes,
		LogsRefreshInterval:   r.cfg.LogsRefreshInterval,
		LogsRefreshMaxActions: r.cfg.LogsRefreshMaxActions,
	})
	now := r.clock()
	for _, tenant := range tenants {
		// Reclaiming expired holds shares the merge cadence because that is what
		// creates them: on a slower janitor the retained bytes would outlive the
		// window an operator configured.
		if _, err := merge.PurgeCompacted(r.cfg.DataDir, tenant, r.cfg.MaxTier, now); err != nil {
			r.log.Error("purge compacted sources", "tenant", tenant, "err", err)
		}
		if layout.ColdEnabled(r.cfg.ColdDir) {
			if _, err := merge.PurgeCompacted(r.cfg.ColdDir, tenant, r.cfg.MaxTier, now); err != nil {
				r.log.Error("purge compacted cold sources", "tenant", tenant, "err", err)
			}
		}
		if err := r.mergeTenant(tenant, planner); err != nil {
			r.log.Error("merge tenant", "tenant", tenant, "err", err)
		}
		if err := r.mergeLogsTenant(tenant, planner); err != nil {
			r.log.Error("merge logs tenant", "tenant", tenant, "err", err)
		}
	}
	return r.tickPromote()
}

func (r *Runner) mergeTenant(tenant string, planner *merge.Planner) error {
	caps := merge.DuckDBCaps{Threads: r.cfg.Threads, MemoryLimit: r.cfg.MemoryLimit}
	segs, err := merge.ScanAllTiersRoots(r.cfg.DataDir, r.cfg.ColdDir, tenant, r.cfg.MaxTier, caps)
	if err != nil {
		return err
	}
	r.rec.ObserveTierSegments(tenant, len(segs))
	actions := planner.FindMerges(segs)
	if len(actions) == 0 {
		return nil
	}
	action := actions[0]
	x, err := merge.NewExecutor(merge.ExecutorConfig{
		DataDir:              r.cfg.DataDir,
		ColdDir:              r.cfg.ColdDir,
		Tenant:               tenant,
		Threads:              r.cfg.Threads,
		MemoryLimit:          r.cfg.MemoryLimit,
		SegmentFormat:        r.cfg.MergeSegmentFormat,
		DuckDBStorageVersion: r.cfg.DuckDBStorageVersion,
		DeleteGrace:          r.cfg.DeleteGrace,
	})
	if err != nil {
		return err
	}
	defer func() { _ = x.Close() }()
	now := r.clock()
	mergeStart := now
	out, err := x.ExecuteMerge(action, now)
	if err != nil {
		return err
	}
	if elapsed := r.clock().Sub(mergeStart).Seconds(); elapsed > 0 {
		if err := stats.AddCompactionCPUSeconds(r.cfg.DataDir, tenant, elapsed); err != nil {
			return err
		}
		r.rec.ObserveCompactionSeconds(tenant, elapsed)
	}
	if action.DestTier >= 1 {
		rb, err := rollup.NewBuilder(r.cfg.DataDir, tenant, rollup.ParseSteps(r.cfg.RollupSteps), rollup.BuilderConfig{
			Threads:     r.cfg.Threads,
			MemoryLimit: r.cfg.MemoryLimit,
		})
		if err != nil {
			return err
		}
		defer func() { _ = rb.Close() }()
		if err := rb.BuildFromMerge([]string{out.Path}, now); err != nil {
			return err
		}
	}
	return r.runMaterialize(tenant, out.Path, sourcePaths(action), action.DestTier, materialize.PlaneMetrics, now)
}

func sourcePaths(action merge.MergeAction) []string {
	return segmentPaths(action.Sources)
}

func segmentPaths(sources []merge.Segment) []string {
	out := make([]string, len(sources))
	for i, s := range sources {
		out[i] = s.Path
	}
	return out
}

func (r *Runner) runMaterialize(tenant, dest string, sources []string, destTier int, plane materialize.Plane, now time.Time) error {
	if len(r.cfg.Materializations.Materializations) == 0 {
		return nil
	}
	return materialize.Run(context.Background(), &materialize.RunConfig{
		DataDir:     r.cfg.DataDir,
		Tenant:      tenant,
		DestPath:    dest,
		SourcePaths: sources,
		DestTier:    destTier,
		Plane:       plane,
		Items:       r.cfg.Materializations.Materializations,
		RunJobs:     true,
		Now:         now,
		Threads:     r.cfg.Threads,
		MemoryLimit: r.cfg.MemoryLimit,
		DeleteGrace: r.cfg.DeleteGrace,
		Logger:      r.log,
	})
}

func (r *Runner) mergeLogsTenant(tenant string, planner *merge.Planner) error {
	artifacts, err := merge.ListLogArtifacts(r.cfg.DataDir, tenant)
	if err != nil {
		return err
	}
	for _, artifact := range artifacts {
		landing, err := merge.ScanLogLanding(r.cfg.DataDir, tenant, artifact)
		if err != nil {
			return err
		}
		r.rec.ObserveLogLandingFiles(tenant, artifact, len(landing))
		tiers, err := merge.ScanLogTiersRoots(r.cfg.DataDir, r.cfg.ColdDir, tenant, artifact, r.cfg.MaxTier)
		if err != nil {
			return err
		}
		actions := planner.FindLogMerges(r.clock(), landing, tiers)
		for _, action := range actions {
			action.Artifact = artifact
			x, err := merge.NewExecutor(merge.ExecutorConfig{
				DataDir:              r.cfg.DataDir,
				ColdDir:              r.cfg.ColdDir,
				Tenant:               tenant,
				Threads:              r.cfg.Threads,
				MemoryLimit:          r.cfg.MemoryLimit,
				SegmentFormat:        r.cfg.MergeSegmentFormat,
				DuckDBStorageVersion: r.cfg.DuckDBStorageVersion,
				DeleteGrace:          r.cfg.DeleteGrace,
			})
			if err != nil {
				return err
			}
			mergeStart := r.clock()
			out, err := x.ExecuteLogMerge(artifact, action, mergeStart)
			_ = x.Close()
			if err != nil {
				return err
			}
			if elapsed := r.clock().Sub(mergeStart).Seconds(); elapsed > 0 {
				if err := stats.AddCompactionCPUSeconds(r.cfg.DataDir, tenant, elapsed); err != nil {
					return err
				}
				r.rec.ObserveCompactionSeconds(tenant, elapsed)
			}
			if err := logmeta.Bump(r.cfg.DataDir, tenant); err != nil {
				return err
			}
			if err := logmeta.SyncManifestRoots(r.cfg.DataDir, r.cfg.ColdDir, tenant, artifact); err != nil {
				return err
			}
			// A refresh is what publishes buffered rows, so this is where their
			// label values enter the index; folding in just the new segment keeps
			// the next label query off a full rescan of every tier.
			if err := logmeta.MergeLabelIndexFromParquet(r.cfg.DataDir, tenant, out.Path); err != nil {
				return err
			}
			if err := r.runMaterialize(tenant, out.Path, segmentPaths(action.Sources), action.DestTier, materialize.PlaneLogs, mergeStart); err != nil {
				return err
			}
		}
	}
	return nil
}

// TickRetention deletes expired tier segments and rollup files.
// Per-tenant and per-file failures are logged and skipped so one bad
// tenant/file cannot block MAX_LOG_FILES or other tenants.
func (r *Runner) TickRetention() error {
	return r.observed(JobRetention, r.tickRetention)
}

func (r *Runner) tickRetention() error {
	tenants, err := listTenants(r.cfg.DataDir)
	if err != nil {
		return err
	}
	now := r.clock()
	retCfg := merge.RetentionConfig{RetentionDays: r.cfg.RetentionDays}
	retDays := r.cfg.RetentionDays
	if retDays <= 0 {
		retDays = 15
	}
	cutoff := now.Add(-time.Duration(retDays) * 24 * time.Hour)

	for _, tenant := range tenants {
		segs, err := merge.ScanAllTiersRoots(r.cfg.DataDir, r.cfg.ColdDir, tenant, r.cfg.MaxTier, merge.DuckDBCaps{
			Threads: r.cfg.Threads, MemoryLimit: r.cfg.MemoryLimit,
		})
		if err != nil {
			r.log.Error("retention scan tiers", "tenant", tenant, "err", err)
		} else {
			r.rec.ObserveTierSegments(tenant, len(segs))
			for _, del := range merge.Retention(segs, now, retCfg) {
				if err := removePath(del.Segment.Path); err != nil {
					r.log.Error("retention delete segment", "tenant", tenant, "path", del.Segment.Path, "err", err)
				}
			}
			if err := metricsmeta.SyncAfterChangeRoots(context.Background(), r.cfg.DataDir, r.cfg.ColdDir, tenant); err != nil {
				r.log.Error("retention metrics catalog", "tenant", tenant, "err", err)
			}
		}
		r.deleteExpiredRollups(tenant, cutoff)
		if err := r.retainLogsTenant(tenant, now); err != nil {
			r.log.Error("retention logs", "tenant", tenant, "err", err)
		}
	}
	return nil
}

func (r *Runner) retainLogsTenant(tenant string, now time.Time) error {
	artifacts, err := merge.ListLogArtifacts(r.cfg.DataDir, tenant)
	if err != nil {
		return err
	}
	var changed bool
	for _, artifact := range artifacts {
		landing, err := merge.ScanLogLanding(r.cfg.DataDir, tenant, artifact)
		if err != nil {
			return err
		}
		tiers, err := merge.ScanLogTiersRoots(r.cfg.DataDir, r.cfg.ColdDir, tenant, artifact, r.cfg.MaxTier)
		if err != nil {
			return err
		}
		// Age retention covers landing + cold tiers (RETENTION_DAYS).
		all := make([]merge.Segment, 0, len(landing)+len(tiers))
		all = append(all, landing...)
		all = append(all, tiers...)
		for _, del := range merge.LogRetention(all, now, merge.LogRetentionConfig{
			RetentionDays: r.cfg.RetentionDays,
		}) {
			if err := removePath(del.Segment.Path); err != nil {
				return err
			}
			changed = true
		}
		// MAX_LOG_FILES applies to the hot landing zone only. Applying it to
		// landing+tiers together deletes sealed cold segments first (they are
		// older), which collapses Grafana history to ~minutes under tiny
		// agent landings. Cold lifetime is RETENTION_DAYS (+ merge/segment caps).
		landingAfterAge, err := merge.ScanLogLanding(r.cfg.DataDir, tenant, artifact)
		if err != nil {
			return err
		}
		deleted := 0
		for _, del := range merge.LogRetention(landingAfterAge, now, merge.LogRetentionConfig{
			MaxLogFiles: r.cfg.MaxLogFiles,
		}) {
			if err := removePath(del.Segment.Path); err != nil {
				return err
			}
			deleted++
			changed = true
		}
		// Report what survived this pass: the gauge must show the post-retention
		// landing depth an operator compares against the cap.
		r.rec.ObserveLogLandingFiles(tenant, artifact, len(landingAfterAge)-deleted)
	}
	if changed {
		if err := logmeta.Bump(r.cfg.DataDir, tenant); err != nil {
			return err
		}
		for _, artifact := range artifacts {
			if err := logmeta.SyncManifestRoots(r.cfg.DataDir, r.cfg.ColdDir, tenant, artifact); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Runner) deleteExpiredRollups(tenant string, cutoff time.Time) {
	for _, step := range rollup.ParseSteps(r.cfg.RollupSteps) {
		dir := layout.RollupDir(r.cfg.DataDir, tenant, step.Name)
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			r.log.Error("retention list rollups", "tenant", tenant, "path", dir, "err", err)
			continue
		}
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".parquet" || e.Name()[0] == '.' {
				continue
			}
			path := filepath.Join(dir, e.Name())
			maxBucket, err := rollup.StatRollupMaxBucket(path)
			if err != nil {
				r.log.Error("retention rollup stat", "tenant", tenant, "path", path, "err", err)
				if remErr := removePath(path); remErr != nil {
					r.log.Error("retention delete corrupt rollup", "tenant", tenant, "path", path, "err", remErr)
				}
				continue
			}
			// Zero maxBucket means empty/unusable (NULL MAX); delete like expired.
			if maxBucket.IsZero() || maxBucket.Before(cutoff) {
				if err := removePath(path); err != nil {
					r.log.Error("retention delete rollup", "tenant", tenant, "path", path, "err", err)
				}
			}
		}
	}
}

func removePath(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func listTenants(dataDir string) ([]string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var tenants []string
	for _, e := range entries {
		if e.IsDir() {
			tenants = append(tenants, e.Name())
		}
	}
	return tenants, nil
}

func (r *Runner) tickPromote() error {
	if !promote.Enabled(r.cfg.ColdDir) {
		return nil
	}
	return r.observed(JobPromote, r.promoteAll)
}

func (r *Runner) promoteAll() error {
	tenants, err := listTenants(r.cfg.DataDir)
	if err != nil {
		return err
	}
	cfg := r.promoteConfig()
	var acc promote.Stats
	var first error
	for _, tenant := range tenants {
		if err := seed.EnsureLogsLayoutForTenant(r.cfg.ColdDir, tenant); err != nil {
			r.log.Error("seed cold logs layout", "tenant", tenant, "err", err)
			if first == nil {
				first = err
			}
			continue
		}
		st, err := promote.Tenant(&cfg, tenant)
		acc.Attempts += st.Attempts
		acc.Successes += st.Successes
		acc.Retries += st.Retries
		acc.Bytes += st.Bytes
		acc.TmpFiles += st.TmpFiles
		if err != nil {
			r.log.Error("promote tenant", "tenant", tenant, "err", err)
			if first == nil {
				first = err
			}
		}
	}
	r.rec.ObservePromote(acc.Attempts, acc.Successes, acc.Retries, acc.Bytes, acc.TmpFiles)
	return first
}

func (r *Runner) promoteConfig() promote.Config {
	return promote.Config{
		DataDir: r.cfg.DataDir,
		ColdDir: r.cfg.ColdDir,
		After:   r.cfg.ColdAfter,
		MaxTier: r.cfg.MaxTier,
		Grace:   r.cfg.DeleteGrace,
		Now:     r.clock,
		MaxTs:   r.fileMaxTs,
		AfterPromote: func(tenant string) error {
			return r.afterPromote(tenant)
		},
		HoldSource: merge.HoldPath,
	}
}

func (r *Runner) fileMaxTs(path string) (time.Time, bool) {
	if strings.Contains(filepath.ToSlash(path), "/logs/") {
		seg, err := merge.StatLogSegment(path, 1)
		if err != nil || seg.MaxTs.IsZero() {
			return time.Time{}, false
		}
		return seg.MaxTs.UTC(), true
	}
	_, maxNs, ok := metricsmeta.FileBounds(context.Background(), path)
	if !ok || maxNs == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, maxNs).UTC(), true
}

func (r *Runner) afterPromote(tenant string) error {
	if err := metricsmeta.SyncAfterChangeRoots(context.Background(), r.cfg.DataDir, r.cfg.ColdDir, tenant); err != nil {
		return err
	}
	if err := logmeta.Bump(r.cfg.DataDir, tenant); err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for _, root := range []string{r.cfg.DataDir, r.cfg.ColdDir} {
		artifacts, err := merge.ListLogArtifacts(root, tenant)
		if err != nil {
			return err
		}
		for _, artifact := range artifacts {
			if _, ok := seen[artifact]; ok {
				continue
			}
			seen[artifact] = struct{}{}
			if err := logmeta.SyncManifestRoots(r.cfg.DataDir, r.cfg.ColdDir, tenant, artifact); err != nil {
				return err
			}
		}
	}
	return nil
}

// FloorBytesFromHotWindow estimates Lucene floor segment bytes from flush cadence.
func FloorBytesFromHotWindow(window time.Duration) int64 {
	if window <= 0 {
		window = 10 * time.Minute
	}
	mins := window.Minutes()
	if mins < 1 {
		mins = 1
	}
	return int64(mins * 256 * 1024)
}

// TierRoot returns the tiers directory for a tenant.
func TierRoot(dataDir, tenant string) string {
	return filepath.Join(dataDir, tenant, "tiers")
}
