package lifecycle

import (
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/logmeta"
	"github.com/elk-utilities/prism/internal/store/merge"
	"github.com/elk-utilities/prism/internal/store/rollup"
	"github.com/elk-utilities/prism/internal/store/segformat"
	"github.com/elk-utilities/prism/internal/store/stats"
)

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
	// Logger records per-tenant / per-file errors; nil uses slog.Default.
	Logger *slog.Logger
}

// Runner executes flush, merge, and retention on a ticker schedule.
type Runner struct {
	cfg   Config
	eng   *engine.Engine
	clock func() time.Time
	log   *slog.Logger
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
	return &Runner{cfg: c, eng: eng, clock: now, log: log}
}

// TickHotSnapshot exports near-real-time hot parquet snapshots for Grafana.
func (r *Runner) TickHotSnapshot() error {
	return r.eng.ExportHotSnapshots()
}

// TickFlush rolls hot tables whose window elapsed.
func (r *Runner) TickFlush() error {
	return r.eng.FlushDue()
}

// TickMerge plans and executes one merge pass per tenant with tier segments.
// Per-tenant failures are logged and skipped so one bad tenant cannot block others.
func (r *Runner) TickMerge() error {
	tenants, err := listTenants(r.cfg.DataDir)
	if err != nil {
		return err
	}
	planner := merge.NewPlanner(merge.PlannerConfig{
		SegmentsPerTier: r.cfg.SegmentsPerTier,
		// MaxMergeAtOnce 0 → derive from MaxSegmentBytes/FloorBytes so tiny
		// unsealed segments pack toward the seal budget in one action.
		MaxMergeAtOnce:  0,
		MaxSegmentBytes: r.cfg.MaxSegmentBytes,
		FloorBytes:      r.cfg.FloorBytes,
	})
	for _, tenant := range tenants {
		if err := r.mergeTenant(tenant, planner); err != nil {
			r.log.Error("merge tenant", "tenant", tenant, "err", err)
		}
		if err := r.mergeLogsTenant(tenant, planner); err != nil {
			r.log.Error("merge logs tenant", "tenant", tenant, "err", err)
		}
	}
	return nil
}

func (r *Runner) mergeTenant(tenant string, planner *merge.Planner) error {
	caps := merge.DuckDBCaps{Threads: r.cfg.Threads, MemoryLimit: r.cfg.MemoryLimit}
	segs, err := merge.ScanAllTiers(r.cfg.DataDir, tenant, r.cfg.MaxTier, caps)
	if err != nil {
		return err
	}
	actions := planner.FindMerges(segs)
	if len(actions) == 0 {
		return nil
	}
	action := actions[0]
	x, err := merge.NewExecutor(merge.ExecutorConfig{
		DataDir:              r.cfg.DataDir,
		Tenant:               tenant,
		Threads:              r.cfg.Threads,
		MemoryLimit:          r.cfg.MemoryLimit,
		SegmentFormat:        r.cfg.MergeSegmentFormat,
		DuckDBStorageVersion: r.cfg.DuckDBStorageVersion,
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
	return nil
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
		tiers, err := merge.ScanLogTiers(r.cfg.DataDir, tenant, artifact, r.cfg.MaxTier)
		if err != nil {
			return err
		}
		actions := planner.FindLogMerges(landing, tiers)
		for _, action := range actions {
			action.Artifact = artifact
			x, err := merge.NewExecutor(merge.ExecutorConfig{
				DataDir:              r.cfg.DataDir,
				Tenant:               tenant,
				Threads:              r.cfg.Threads,
				MemoryLimit:          r.cfg.MemoryLimit,
				SegmentFormat:        r.cfg.MergeSegmentFormat,
				DuckDBStorageVersion: r.cfg.DuckDBStorageVersion,
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
			}
			_ = out
			if err := logmeta.Bump(r.cfg.DataDir, tenant); err != nil {
				return err
			}
			if err := logmeta.SyncManifest(r.cfg.DataDir, tenant, artifact); err != nil {
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
		segs, err := merge.ScanAllTiers(r.cfg.DataDir, tenant, r.cfg.MaxTier, merge.DuckDBCaps{
			Threads: r.cfg.Threads, MemoryLimit: r.cfg.MemoryLimit,
		})
		if err != nil {
			r.log.Error("retention scan tiers", "tenant", tenant, "err", err)
		} else {
			for _, del := range merge.Retention(segs, now, retCfg) {
				if err := removePath(del.Segment.Path); err != nil {
					r.log.Error("retention delete segment", "tenant", tenant, "path", del.Segment.Path, "err", err)
				}
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
		tiers, err := merge.ScanLogTiers(r.cfg.DataDir, tenant, artifact, r.cfg.MaxTier)
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
		for _, del := range merge.LogRetention(landingAfterAge, now, merge.LogRetentionConfig{
			MaxLogFiles: r.cfg.MaxLogFiles,
		}) {
			if err := removePath(del.Segment.Path); err != nil {
				return err
			}
			changed = true
		}
	}
	if changed {
		if err := logmeta.Bump(r.cfg.DataDir, tenant); err != nil {
			return err
		}
		for _, artifact := range artifacts {
			if err := logmeta.SyncManifest(r.cfg.DataDir, tenant, artifact); err != nil {
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
