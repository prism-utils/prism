package gen_test

import (
	"testing"

	"github.com/elk-utilities/prism/bench/internal/gen"
	"github.com/stretchr/testify/require"
)

func TestQueryRange_coversAllLogs_atScale(t *testing.T) {
	cfg := gen.ScaleConfig(1)
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)

	start, end := ds.QueryRange()
	require.False(t, start.IsZero())
	require.True(t, ds.Logs[0].Ts.Equal(start) || !ds.Logs[0].Ts.Before(start))
	last := ds.Logs[len(ds.Logs)-1].Ts
	require.True(t, last.Before(end), "last log %v end %v", last, end)
}
