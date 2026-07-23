package results_test

import (
	"strings"
	"testing"

	"github.com/elk-utilities/prism/bench/internal/monitor"
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

func TestRenderMarkdown_benchResultsChartPaths(t *testing.T) {
	doc := results.RenderMarkdown(&results.Report{
		Environment: results.Environment{
			ChartPaths: []string{
				"bench/charts/cpu-cores.svg",
				"bench/charts/memory-rss.svg",
			},
		},
	})
	require.Contains(t, doc, "![cpu-cores.svg](charts/cpu-cores.svg)")
	require.Contains(t, doc, "![memory-rss.svg](charts/memory-rss.svg)")
	require.NotContains(t, doc, "](bench/charts/")
}

func TestRenderMarkdownRoot_keepsBenchChartPaths(t *testing.T) {
	doc := results.RenderMarkdownRoot(&results.Report{
		Environment: results.Environment{
			ChartPaths: []string{"bench/charts/cpu-cores.svg"},
		},
	})
	require.Contains(t, doc, "![cpu-cores.svg](bench/charts/cpu-cores.svg)")
}

func TestRenderMarkdown_resourceUsageWithAndWithoutIOPS(t *testing.T) {
	ro, wo := uint64(500), uint64(300)
	withIOPS := monitor.Usage{
		CPUCoresMean: 1.2,
		CPUCoresPeak: 2.5,
		RSSPeakBytes: 256 * 1024 * 1024,
		ReadBytes:    50 * 1024 * 1024,
		WriteBytes:   10 * 1024 * 1024,
		ReadOps:      &ro,
		WriteOps:     &wo,
		DurationSec:  2.0,
	}
	noIOPS := monitor.Usage{
		CPUCoresMean: 0.8,
		CPUCoresPeak: 1.1,
		RSSPeakBytes: 128 * 1024 * 1024,
		ReadBytes:    5 * 1024 * 1024,
		WriteBytes:   2 * 1024 * 1024,
		DurationSec:  1.0,
	}
	doc := results.RenderMarkdown(&results.Report{
		Environment: results.Environment{OS: "darwin", Arch: "arm64", CPUModel: "test", RAMGiB: 16},
		Workloads: []results.Workload{
			{Name: "count", System: "clickhouse", P50Ms: 1, P95Ms: 2, MinMs: 1, Usage: &withIOPS},
			{Name: "count", System: "prism-store", P50Ms: 1, P95Ms: 2, MinMs: 1, Usage: &noIOPS},
		},
	})
	require.Contains(t, doc, "## Resource usage")
	require.Contains(t, doc, "400")
	require.Contains(t, doc, "n/a")
	lines := strings.Split(doc, "\n")
	var resourceRows []string
	inSection := false
	for _, line := range lines {
		if strings.HasPrefix(line, "## Resource usage") {
			inSection = true
			continue
		}
		if inSection && strings.HasPrefix(line, "## ") {
			break
		}
		if inSection && strings.HasPrefix(line, "| count |") {
			resourceRows = append(resourceRows, line)
		}
	}
	require.Len(t, resourceRows, 2)
	require.True(t, strings.Contains(resourceRows[0], "|") && strings.Contains(resourceRows[1], "|"))
	pipeCount := strings.Count(resourceRows[0], "|")
	require.Equal(t, strings.Count(resourceRows[1], "|"), pipeCount, "rows must align")
}
