package results_test

import (
	"testing"

	"github.com/elk-utilities/prism/bench/internal/results"
	"github.com/stretchr/testify/require"
)

func TestArtifactPaths_baseline(t *testing.T) {
	p := results.ArtifactPaths("/repo", "")
	require.Equal(t, "/repo/bench/results.json", p.JSON)
	require.Equal(t, "/repo/bench/RESULTS.md", p.Markdown)
	require.Equal(t, "/repo/bench/results-timeseries.json", p.Timeseries)
	require.Equal(t, "/repo/bench/charts", p.ChartsDir)
	require.Equal(t, "bench/charts/", p.ChartsPrefix)
	require.Equal(t, "bench/charts/cpu-cores.svg", results.ChartRel(&p, "cpu-cores.svg"))
}

func TestArtifactPaths_apiProfile(t *testing.T) {
	p := results.ArtifactPaths("/repo", "api")
	require.Equal(t, "/repo/bench/results-api.json", p.JSON)
	require.Equal(t, "/repo/bench/RESULTS-api.md", p.Markdown)
	require.Equal(t, "/repo/bench/results-timeseries-api.json", p.Timeseries)
	require.Equal(t, "/repo/bench/charts-api", p.ChartsDir)
	require.Equal(t, "bench/charts-api/", p.ChartsPrefix)
	require.Equal(t, "bench/charts-api/cpu-cores.svg", results.ChartRel(&p, "cpu-cores.svg"))
}

func TestArtifactPaths_apiArrowProfile(t *testing.T) {
	p := results.ArtifactPaths("/repo", "api-arrow")
	require.Equal(t, "/repo/bench/results-api-arrow.json", p.JSON)
	require.Equal(t, "/repo/bench/RESULTS-api-arrow.md", p.Markdown)
	require.Equal(t, "/repo/bench/results-timeseries-api-arrow.json", p.Timeseries)
	require.Equal(t, "/repo/bench/charts-api-arrow", p.ChartsDir)
	require.Equal(t, "bench/charts-api-arrow/", p.ChartsPrefix)
	require.Equal(t, "bench/charts-api-arrow/cpu-cores.svg", results.ChartRel(&p, "cpu-cores.svg"))
}

func TestArtifactPaths_apiArrowHotProfile(t *testing.T) {
	p := results.ArtifactPaths("/repo", "api-arrow-hot")
	require.Equal(t, "/repo/bench/results-api-arrow-hot.json", p.JSON)
	require.Equal(t, "/repo/bench/RESULTS-api-arrow-hot.md", p.Markdown)
	require.Equal(t, "/repo/bench/results-timeseries-api-arrow-hot.json", p.Timeseries)
	require.Equal(t, "/repo/bench/charts-api-arrow-hot", p.ChartsDir)
	require.Equal(t, "bench/charts-api-arrow-hot/", p.ChartsPrefix)
	require.Equal(t, "bench/charts-api-arrow-hot/cpu-cores.svg", results.ChartRel(&p, "cpu-cores.svg"))
}

func TestRenderMarkdown_apiProfile(t *testing.T) {
	doc := results.RenderMarkdown(&results.Report{
		Environment: results.Environment{
			Profile:     "api",
			OS:          "linux",
			Arch:        "amd64",
			CPUModel:    "test",
			RAMGiB:      8,
			MetricsRows: 1000,
			LogsRows:    1000,
			ChartPaths:  []string{"bench/charts-api/cpu-cores.svg"},
		},
		MetricsCountClickHouse: 1000,
		MetricsCountStore:      1000,
		LikeCountStore:         10,
		LikeCountClickHouse:    10,
	})
	require.Contains(t, doc, "RBAC + HTTP")
	require.Contains(t, doc, "HTTP `/sql`")
	require.Contains(t, doc, "ClickHouse uses its native protocol")
	require.Contains(t, doc, "logs LIKE remains engine-level")
	require.Contains(t, doc, "![cpu-cores.svg](charts-api/cpu-cores.svg)")
	require.Contains(t, doc, "make bench-api")
	require.Contains(t, doc, "count** and **aggregation** sample the `prism-store` binary")
}

func TestRenderMarkdown_baselineSamplingNoteUnchanged(t *testing.T) {
	doc := results.RenderMarkdown(&results.Report{
		Environment: results.Environment{OS: "darwin", Arch: "arm64", CPUModel: "x", RAMGiB: 1},
	})
	require.Contains(t, doc, "Store **count**, **aggregation**, and **logs LIKE** sample the benchmark process")
	require.NotContains(t, doc, "make bench-api")
}
