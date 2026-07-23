package merge

// PlannerConfig mirrors Lucene TieredMergePolicy knobs for the parquet tier store.
type PlannerConfig struct {
	SegmentsPerTier int
	MaxMergeAtOnce  int
	MaxSegmentBytes int64
	FloorBytes      int64
}

// DefaultPlannerConfig returns production defaults.
func DefaultPlannerConfig() PlannerConfig {
	return PlannerConfig{SegmentsPerTier: 6, MaxMergeAtOnce: 6, MaxSegmentBytes: 2 << 30, FloorBytes: 1 << 20}
}

// Planner selects tier merges without DuckDB.
type Planner struct {
	cfg PlannerConfig
}

// NewPlanner returns a merge planner with defaulted configuration.
func NewPlanner(cfg PlannerConfig) *Planner {
	return &Planner{cfg: cfg}
}

// FindMerges returns at most one merge action per source tier (no cascade).
func (p *Planner) FindMerges(segments []Segment) []MergeAction {
	return nil
}
