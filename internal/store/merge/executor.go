package merge

import (
	"fmt"
	"time"
)

// ExecutorConfig holds merge execution parameters.
type ExecutorConfig struct {
	DataDir      string
	Tenant       string
	RowGroupSize int
}

// Executor runs planned merges via DuckDB COPY.
type Executor struct {
	cfg ExecutorConfig
}

// NewExecutor opens a temporary in-process DuckDB for merge COPY operations.
func NewExecutor(cfg ExecutorConfig) (*Executor, error) {
	return &Executor{cfg: cfg}, fmt.Errorf("merge: not implemented")
}

// Close releases the DuckDB connection.
func (x *Executor) Close() error { return nil }

// ExecuteMerge merges sources into L{DestTier} with rows ordered by ts.
func (x *Executor) ExecuteMerge(action MergeAction, now time.Time) (Segment, error) {
	return Segment{}, fmt.Errorf("merge: not implemented")
}

// StatSegment reads parquet metadata for min/max ts and byte size.
func StatSegment(path string, tier int) (Segment, error) {
	return Segment{}, fmt.Errorf("merge: not implemented")
}

// ScanTier lists segments in a tier directory with stats.
func ScanTier(dataDir, tenant string, tier int) ([]Segment, error) {
	return nil, nil
}

// ScanAllTiers returns segments from L0..Lmax present on disk.
func ScanAllTiers(dataDir, tenant string, maxTier int) ([]Segment, error) {
	return nil, nil
}
