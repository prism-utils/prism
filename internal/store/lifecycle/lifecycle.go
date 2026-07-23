package lifecycle

import (
	"os"
	"path/filepath"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/merge"
	"github.com/elk-utilities/prism/internal/store/rollup"
	"github.com/elk-utilities/prism/internal/store/stats"
)

// Config drives background merge and retention passes.
type Config struct {
	DataDir         string
	SegmentsPerTier int
	MaxSegmentBytes int64
	FloorBytes      int64
	RetentionDays   int
	RollupSteps     string
	MaxTier         int
}

// Runner executes flush, merge, and retention on a ticker schedule.
type Runner struct {
	cfg   Config
	eng   *engine.Engine
	clock func() time.Time
}

// NewRunner builds a lifecycle runner.
func NewRunner(cfg Config, eng *engine.Engine, now func() time.Time) *Runner {
	if now == nil {
		now = time.Now
	}
	if cfg.MaxTier <= 0 {
		cfg.MaxTier = 8
	}
	return &Runner{cfg: cfg, eng: eng, clock: now}
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
func (r *Runner) TickMerge() error {
	tenants, err := listTenants(r.cfg.DataDir)
	if err != nil {
		return err
	}
	planner := merge.NewPlanner(merge.PlannerConfig{
		SegmentsPerTier: r.cfg.SegmentsPerTier,
		MaxMergeAtOnce:  r.cfg.SegmentsPerTier,
		MaxSegmentBytes: r.cfg.MaxSegmentBytes,
		FloorBytes:      r.cfg.FloorBytes,
	})
	for _, tenant := range tenants {
		if err := r.mergeTenant(tenant, planner); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) mergeTenant(tenant string, planner *merge.Planner) error {
	segs, err := merge.ScanAllTiers(r.cfg.DataDir, tenant, r.cfg.MaxTier)
	if err != nil {
		return err
	}
	actions := planner.FindMerges(segs)
	if len(actions) == 0 {
		return nil
	}
	action := actions[0]
	x, err := merge.NewExecutor(merge.ExecutorConfig{DataDir: r.cfg.DataDir, Tenant: tenant})
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
		rb, err := rollup.NewBuilder(r.cfg.DataDir, tenant, rollup.ParseSteps(r.cfg.RollupSteps))
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

// TickRetention deletes expired tier segments and rollup files.
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
		segs, err := merge.ScanAllTiers(r.cfg.DataDir, tenant, r.cfg.MaxTier)
		if err != nil {
			return err
		}
		for _, del := range merge.Retention(segs, now, retCfg) {
			if err := os.Remove(del.Segment.Path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if err := r.deleteExpiredRollups(tenant, cutoff); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) deleteExpiredRollups(tenant string, cutoff time.Time) error {
	for _, step := range rollup.ParseSteps(r.cfg.RollupSteps) {
		dir := layout.RollupDir(r.cfg.DataDir, tenant, step.Name)
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".parquet" || e.Name()[0] == '.' {
				continue
			}
			path := filepath.Join(dir, e.Name())
			maxBucket, err := rollup.StatRollupMaxBucket(path)
			if err != nil {
				return err
			}
			if maxBucket.Before(cutoff) {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
		}
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
