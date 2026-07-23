package monitor_test

import (
	"testing"

	"github.com/elk-utilities/prism/bench/internal/monitor"
	"github.com/stretchr/testify/require"
)

func TestUsageHelpers(t *testing.T) {
	ro, wo := uint64(100), uint64(50)
	u := monitor.Usage{
		RSSPeakBytes: 20 * 1024 * 1024,
		ReadBytes:    10 * 1024 * 1024,
		WriteBytes:   5 * 1024 * 1024,
		ReadOps:      &ro,
		WriteOps:     &wo,
		DurationSec:  2.0,
	}
	require.InDelta(t, 20.0, u.RSSPeakMiB(), 0.01)
	require.InDelta(t, 7.5, u.TotalMiBPerSec(), 0.01)
	iops, ok := u.IOPS()
	require.True(t, ok)
	require.InDelta(t, 75.0, iops, 0.01)
}
