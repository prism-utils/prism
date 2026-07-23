package monitor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCPUPerc(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want float64
	}{
		{name: "percent string", in: "12.50%", want: 0.125},
		{name: "integer percent", in: "100%", want: 1.0},
		{name: "zero", in: "0.00%", want: 0},
		{name: "empty", in: "", want: 0},
		{name: "whitespace", in: "  ", want: 0},
		{name: "malformed", in: "not-a-number%", want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.InDelta(t, tc.want, parseCPUPerc(tc.in), 0.0001)
		})
	}
}

func TestParseMemUsageBytes(t *testing.T) {
	t.Parallel()
	const gib = uint64(1024 * 1024 * 1024)
	tests := []struct {
		name string
		in   string
		want uint64
	}{
		{name: "gib pair", in: "1.5GiB / 3GiB", want: uint64(1.5 * float64(gib))},
		{name: "mib used only", in: "512MiB / 1GiB", want: 512 * 1024 * 1024},
		{name: "empty", in: "", want: 0},
		{name: "malformed", in: "not memory", want: 0},
		{name: "no slash", in: "256MiB", want: 256 * 1024 * 1024},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, parseMemUsageBytes(tc.in))
		})
	}
}

func TestParseDockerCLIStatsLine(t *testing.T) {
	t.Parallel()
	const gib = uint64(1024 * 1024 * 1024)

	t.Run("valid json", func(t *testing.T) {
		t.Parallel()
		sample, err := parseDockerCLIStatsLine(`{"CPUPerc":"12.50%","MemUsage":"1.5GiB / 3GiB"}`)
		require.NoError(t, err)
		require.InDelta(t, 0.125, sample.cpuCores, 0.0001)
		require.Equal(t, uint64(1.5*float64(gib)), sample.rssBytes)
	})

	t.Run("empty output", func(t *testing.T) {
		t.Parallel()
		_, err := parseDockerCLIStatsLine("")
		require.Error(t, err)
	})

	t.Run("malformed json", func(t *testing.T) {
		t.Parallel()
		_, err := parseDockerCLIStatsLine("{not json")
		require.Error(t, err)
	})

	t.Run("missing fields yields zero metrics", func(t *testing.T) {
		t.Parallel()
		sample, err := parseDockerCLIStatsLine(`{}`)
		require.NoError(t, err)
		require.Zero(t, sample.cpuCores)
		require.Zero(t, sample.rssBytes)
	})
}
