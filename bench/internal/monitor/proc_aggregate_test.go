package monitor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAggregateProcSamples_emptySlice(t *testing.T) {
	t.Parallel()
	u := aggregateProcSamples(nil, 1.5, false)
	require.Equal(t, 1.5, u.DurationSec)
	require.Zero(t, u.CPUCoresMean)
	require.Zero(t, u.RSSPeakBytes)
	require.False(t, u.IOAvailable())
}

func TestAggregateProcSamples_zeroDuration(t *testing.T) {
	t.Parallel()
	u := aggregateProcSamples([]procSample{
		{cpuCores: 0.5, rssBytes: 100},
	}, 0, false)
	require.Zero(t, u.DurationSec)
	require.InDelta(t, 0.5, u.CPUCoresMean, 0.001)
	require.Equal(t, uint64(100), u.RSSPeakBytes)
	require.False(t, u.IOAvailable())
}

func TestAggregateProcSamples_processGonePartialSamples(t *testing.T) {
	t.Parallel()
	u := aggregateProcSamples([]procSample{
		{cpuCores: 1.2, rssBytes: 512 * 1024 * 1024},
		{cpuCores: 0, rssBytes: 0},
		{cpuCores: 0.3, rssBytes: 64 * 1024 * 1024},
	}, 0.05, false)
	require.InDelta(t, 0.5, u.CPUCoresMean, 0.001)
	require.InDelta(t, 1.2, u.CPUCoresPeak, 0.001)
	require.Equal(t, uint64(512*1024*1024), u.RSSPeakBytes)
	require.False(t, u.IOAvailable())
	require.Zero(t, u.ReadBytes)
	require.Zero(t, u.WriteBytes)
}

func TestAggregateProcSamples_linuxIOWithoutActivity(t *testing.T) {
	t.Parallel()
	u := aggregateProcSamples([]procSample{
		{cpuCores: 0.1, rssBytes: 1024, readB: 0, writeB: 0, readOps: 0, writeOps: 0},
	}, 1.0, true)
	require.False(t, u.IOAvailable())
}
