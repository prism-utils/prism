// Package store drives prism-store HTTP ingest, compaction, and query workloads
// for the benchmark harness.
package store

import (
	"context"
	"time"

	"github.com/elk-utilities/prism/bench/internal/caps"
	"github.com/elk-utilities/prism/bench/internal/gen"
)

// RBACConfig holds OIDC/RBAC env for the API benchmark profile.
type RBACConfig struct {
	PolicyFile string
	Issuer     string
	JWKSFile   string
	Audience   string
}

// Config holds store benchmark settings.
type Config struct {
	DataDir    string
	Tenant     string
	ListenAddr string
	StoreBin   string
	Budget     caps.Budget
	RBAC       *RBACConfig
	Token      string
}

// Driver runs ingest and query workloads against prism-store.
type Driver interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	StopServer(ctx context.Context) error
	StartServer(ctx context.Context) error
	IngestMetricsHTTP(ctx context.Context, windows []string) error
	Compact(ctx context.Context) error
	WriteLogsTier(ctx context.Context, path string, rows []gen.LogRow) error
	CountMetrics(ctx context.Context) (int64, error)
	CountMetricsAPI(ctx context.Context) (int64, error)
	AggregateMetrics(ctx context.Context) error
	AggregateMetricsAPI(ctx context.Context) error
	CountMetricsArrowAPI(ctx context.Context) (int64, error)
	AggregateMetricsArrowAPI(ctx context.Context) error
	ScanMetricsArrowAPI(ctx context.Context, sqlText string) (int64, error)
	ScanMetricsJSONAPI(ctx context.Context, sqlText string) (int64, error)
	CountLogsLike(ctx context.Context, logsGlob string, start, end time.Time) (int64, error)
	DuckDBVersion(ctx context.Context) (string, error)
	BaseURL() string
	// Pid returns the OS process id of the running prism-store binary, or 0 if none.
	Pid() int
}

// New returns a platform driver (CGO build uses DuckDB-backed implementation).
func New(cfg Config) (Driver, error) { //nolint:gocritic // Config matches existing driver constructor style.
	return newDriver(cfg)
}
