// Package store drives prism-store HTTP ingest, compaction, and query workloads
// for the benchmark harness.
package store

import (
	"context"
	"time"

	"github.com/elk-utilities/prism/bench/internal/gen"
)

// Config holds store benchmark settings.
type Config struct {
	DataDir    string
	Tenant     string
	ListenAddr string
	StoreBin   string
}

// Driver runs ingest and query workloads against prism-store.
type Driver interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	IngestMetricsHTTP(ctx context.Context, windows []string) error
	Compact(ctx context.Context) error
	WriteLogsTier(ctx context.Context, path string, rows []gen.LogRow) error
	CountMetrics(ctx context.Context) (int64, error)
	AggregateMetrics(ctx context.Context) error
	CountLogsLike(ctx context.Context, logsGlob string, start, end time.Time) (int64, error)
	DuckDBVersion(ctx context.Context) (string, error)
}

// New returns a platform driver (CGO build uses DuckDB-backed implementation).
func New(cfg Config) (Driver, error) {
	return newDriver(cfg)
}
