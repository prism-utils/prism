package monitor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const sampleStatsJSON = `{
  "cpu_stats": {
    "cpu_usage": {
      "total_usage": 3000000000,
      "percpu_usage": [1000000000, 1000000000, 1000000000, 1000000000]
    },
    "system_cpu_usage": 12000000000,
    "online_cpus": 4
  },
  "precpu_stats": {
    "cpu_usage": {
      "total_usage": 1000000000
    },
    "system_cpu_usage": 8000000000
  },
  "memory_stats": {
    "usage": 536870912
  },
  "blkio_stats": {
    "io_service_bytes_recursive": [
      {"major": 8, "minor": 0, "op": "Read", "value": 1048576},
      {"major": 8, "minor": 0, "op": "Write", "value": 2097152}
    ],
    "io_serviced_recursive": [
      {"major": 8, "minor": 0, "op": "Read", "value": 128},
      {"major": 8, "minor": 0, "op": "Write", "value": 64}
    ]
  }
}`

func TestParseDockerStatsSample_math(t *testing.T) {
	cpu, rss, readB, writeB, readOps, writeOps, blkioOK, err := parseDockerStatsSample([]byte(sampleStatsJSON))
	require.NoError(t, err)
	require.True(t, blkioOK)
	require.InDelta(t, 2.0, cpu, 0.01)
	require.Equal(t, uint64(536870912), rss)
	require.Equal(t, uint64(1048576), readB)
	require.Equal(t, uint64(2097152), writeB)
	require.Equal(t, uint64(128), readOps)
	require.Equal(t, uint64(64), writeOps)
}

func TestAggregateDockerSamples_deltaIO(t *testing.T) {
	u := aggregateDockerSamples([]dockerSample{
		{cpuCores: 1.0, rssBytes: 100, readB: 1000, writeB: 2000, readOps: 10, writeOps: 5, blkioOK: true},
		{cpuCores: 2.5, rssBytes: 200, readB: 5000, writeB: 8000, readOps: 50, writeOps: 25, blkioOK: true},
	}, 2.0)
	require.InDelta(t, 1.75, u.CPUCoresMean, 0.01)
	require.InDelta(t, 2.5, u.CPUCoresPeak, 0.01)
	require.Equal(t, uint64(200), u.RSSPeakBytes)
	require.Equal(t, uint64(4000), u.ReadBytes)
	require.Equal(t, uint64(6000), u.WriteBytes)
	require.True(t, u.IOAvailable())
	iops, ok := u.IOPS()
	require.True(t, ok)
	require.InDelta(t, 30.0, iops, 0.01)
}
