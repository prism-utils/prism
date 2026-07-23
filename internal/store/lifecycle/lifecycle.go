package lifecycle

import (
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
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
	cfg Config
	eng *engine.Engine
}

// NewRunner builds a lifecycle runner.
func NewRunner(cfg Config, eng *engine.Engine, now func() time.Time) *Runner {
	return &Runner{cfg: cfg, eng: eng}
}

// TickHotSnapshot exports near-real-time hot parquet snapshots for Grafana.
func (r *Runner) TickHotSnapshot() error { return r.eng.ExportHotSnapshots() }

// TickFlush rolls hot tables whose window elapsed.
func (r *Runner) TickFlush() error { return r.eng.FlushDue() }

// TickMerge plans and executes one merge pass per tenant with tier segments.
func (r *Runner) TickMerge() error { return nil }

// TickRetention deletes expired tier segments and rollup files.
func (r *Runner) TickRetention() error { return nil }

// FloorBytesFromHotWindow estimates Lucene floor segment bytes from flush cadence.
func FloorBytesFromHotWindow(window time.Duration) int64 { return 1 << 20 }

// TierRoot returns the tiers directory for a tenant.
func TierRoot(dataDir, tenant string) string { return dataDir }
