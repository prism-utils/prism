package monitor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDiffContainerCPU_fromCumulativeCounters(t *testing.T) {
	t.Parallel()
	prev := dockerCumulative{
		cpuTotalUsageNS: 1_000_000_000,
		readB:           1000,
		writeB:          2000,
	}
	cur := dockerCumulative{
		cpuTotalUsageNS: 1_200_000_000,
		readB:           5000,
		writeB:          8000,
		rssBytes:        512 * 1024 * 1024,
		blkioOK:         true,
	}
	wall := 100 * time.Millisecond
	s := diffDockerSample(prev, cur, wall, time.Now())
	require.InDelta(t, 2.0, s.cpuCores, 0.01, "200ms CPU in 100ms wall = 2 cores")
	require.Equal(t, uint64(512*1024*1024), s.rssBytes)
	require.Equal(t, uint64(4000), s.readB)
	require.Equal(t, uint64(6000), s.writeB)
}

func TestDiffContainerCPU_firstSampleZeroCPU(t *testing.T) {
	t.Parallel()
	cur := dockerCumulative{cpuTotalUsageNS: 5_000_000_000, rssBytes: 100}
	s := diffDockerSample(dockerCumulative{}, cur, time.Second, time.Now())
	require.Zero(t, s.cpuCores)
	require.Equal(t, uint64(100), s.rssBytes)
}

func TestAggregateSeriesByPhase_idleAndIngest(t *testing.T) {
	t.Parallel()
	base := time.Unix(1700000000, 0).UTC()
	series := []SamplePoint{
		{At: base, Phase: PhaseIdle, CPUCores: 0.1, RSSBytes: 100 * 1024 * 1024},
		{At: base.Add(time.Second), Phase: PhaseIdle, CPUCores: 0.2, RSSBytes: 110 * 1024 * 1024},
		{At: base.Add(6 * time.Second), Phase: PhaseIngest, CPUCores: 1.5, RSSBytes: 200 * 1024 * 1024},
		{At: base.Add(7 * time.Second), Phase: PhaseIngest, CPUCores: 2.0, RSSBytes: 220 * 1024 * 1024},
	}
	idle := AggregatePhase(series, PhaseIdle)
	require.InDelta(t, 0.15, idle.CPUCoresMean, 0.01)
	require.InDelta(t, 0.2, idle.CPUCoresPeak, 0.01)
	require.Equal(t, uint64(110*1024*1024), idle.RSSPeakBytes)

	ingest := AggregatePhase(series, PhaseIngest)
	require.InDelta(t, 1.75, ingest.CPUCoresMean, 0.01)
	require.InDelta(t, 2.0, ingest.CPUCoresPeak, 0.01)
}

func TestAggregateSeriesByPhase_shortPhaseStillOneSample(t *testing.T) {
	t.Parallel()
	base := time.Unix(1700000000, 0).UTC()
	series := []SamplePoint{
		{At: base, Phase: PhaseCount, CPUCores: 0.9, RSSBytes: 50 * 1024 * 1024},
	}
	u := AggregatePhase(series, PhaseCount)
	require.InDelta(t, 0.9, u.CPUCoresMean, 0.01)
	require.InDelta(t, 0.9, u.CPUCoresPeak, 0.01)
}
