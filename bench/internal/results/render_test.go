package results_test

import (
	"strings"
	"testing"

	"github.com/elk-utilities/prism/bench/internal/results"
	"github.com/stretchr/testify/require"
)

func TestRenderMarkdown_containsWorkloadTable(t *testing.T) {
	doc := results.RenderMarkdown(&results.Report{
		Environment: results.Environment{
			OS:                "darwin",
			Arch:              "arm64",
			CPUModel:          "Apple M1",
			RAMGiB:            16,
			ClickHouseVersion: "24.8.1.1",
			DuckDBVersion:     "v1.1.3",
			GitCommit:         "abc1234",
			MetricsRows:       1_000_000,
			LogsRows:          1_000_000,
		},
		LikeCountStore:         10_000,
		LikeCountClickHouse:    10_000,
		MetricsCountStore:      1_000_000,
		MetricsCountClickHouse: 1_000_000,
		Workloads: []results.Workload{
			{Name: "ingest", System: "prism-store", WallSeconds: 12.5, RowsPerSec: 80000, Rows: 1_000_000},
			{Name: "ingest", System: "clickhouse", WallSeconds: 8.2, RowsPerSec: 120000, Rows: 1_000_000},
			{Name: "count", System: "prism-store", P50Ms: 45, P95Ms: 52, MinMs: 41},
			{Name: "count", System: "clickhouse", P50Ms: 12, P95Ms: 15, MinMs: 10},
		},
	})
	require.Contains(t, doc, "prism-store vs ClickHouse")
	require.Contains(t, doc, "Apple M1")
	require.Contains(t, doc, "abc1234")
	require.Contains(t, doc, "10,000")
	require.Contains(t, doc, "1,000,000")
	require.True(t, strings.Contains(doc, "ingest") && strings.Contains(doc, "count"))
}
