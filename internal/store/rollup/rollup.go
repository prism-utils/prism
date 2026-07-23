package rollup

import (
	"database/sql"
	"fmt"
	"time"
)

// Step is a downsampling interval (e.g. 1m, 5m, 1h).
type Step struct {
	Name     string
	Interval string
}

// ParseSteps parses ROLLUP_STEPS env form "1m,5m,1h".
func ParseSteps(raw string) []Step { return nil }

// Builder materializes rollup parquet files from merged tier inputs.
type Builder struct {
	db *sql.DB
}

// NewBuilder creates a rollup builder.
func NewBuilder(dataDir, tenant string, steps []Step) (*Builder, error) {
	return nil, fmt.Errorf("rollup: not implemented")
}

// Close releases the DuckDB connection.
func (b *Builder) Close() error { return nil }

// BuildFromMerge writes rollup segments for each step from the merged source paths.
func (b *Builder) BuildFromMerge(sourcePaths []string, now time.Time) error {
	return fmt.Errorf("rollup: not implemented")
}

// StatRollupMaxBucket returns the latest bucket timestamp in a rollup parquet file.
func StatRollupMaxBucket(path string) (time.Time, error) {
	return time.Time{}, fmt.Errorf("rollup: not implemented")
}

// AggregateRaw computes reference aggregates over raw rows for tests.
func AggregateRaw(db *sql.DB, paths []string, interval string) (map[string]AggRow, error) {
	return nil, fmt.Errorf("rollup: not implemented")
}

// AggRow is one rollup aggregate bucket for tests.
type AggRow struct {
	Bucket time.Time
	Name   string
	Avg    float64
	Min    float64
	Max    float64
	Count  int64
	Sum    float64
}

// Key returns a stable map key for an aggregate row.
func (r *AggRow) Key() string { return "" }

// ReadRollup reads a rollup parquet into an AggRow map.
func ReadRollup(db *sql.DB, path string) (map[string]AggRow, error) {
	return nil, fmt.Errorf("rollup: not implemented")
}
