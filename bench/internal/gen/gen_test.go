package gen_test

import (
	"testing"

	"github.com/elk-utilities/prism/bench/internal/gen"
	"github.com/stretchr/testify/require"
)

func TestGenerate_fixedSeed_deadlineCount(t *testing.T) {
	cfg := gen.Config{
		Seed:        42,
		MetricsRows: 10_000,
		LogsRows:    10_000,
	}
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)

	want := gen.ExpectedDeadlineCount(cfg.LogsRows)
	require.Equal(t, want, ds.DeadlineCount())
	require.Equal(t, want, int64(100))

	got2, err := gen.Generate(cfg)
	require.NoError(t, err)
	require.Equal(t, ds.DeadlineCount(), got2.DeadlineCount())
}

func TestGenerate_fixedSeed_metricCardinality(t *testing.T) {
	cfg := gen.Config{
		Seed:        99,
		MetricsRows: 50_000,
		LogsRows:    1_000,
	}
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)
	require.Len(t, ds.MetricNames(), gen.MetricCardinality)
}
