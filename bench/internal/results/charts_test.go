package results

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/bench/internal/monitor"
	"github.com/stretchr/testify/require"
)

func TestWriteCPUChart_producesWellFormedSVG(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	base := time.Unix(1700000000, 0).UTC()
	phases := []PhaseSpan{
		{Name: monitor.PhaseIdle, Start: base, End: base.Add(5 * time.Second)},
		{Name: monitor.PhaseIngest, Start: base.Add(5 * time.Second), End: base.Add(20 * time.Second)},
	}
	store := []monitor.SamplePoint{
		{At: base, Phase: monitor.PhaseIdle, CPUCores: 0.1, RSSBytes: 100 << 20},
		{At: base.Add(10 * time.Second), Phase: monitor.PhaseIngest, CPUCores: 1.5, RSSBytes: 200 << 20},
	}
	ch := []monitor.SamplePoint{
		{At: base, Phase: monitor.PhaseIdle, CPUCores: 0.05, RSSBytes: 80 << 20},
		{At: base.Add(10 * time.Second), Phase: monitor.PhaseIngest, CPUCores: 1.2, RSSBytes: 150 << 20},
	}
	path := filepath.Join(dir, "cpu.svg")
	err := WriteCPUChart(path, store, ch, phases)
	require.NoError(t, err)
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, b)
	text := string(b)
	require.True(t, strings.HasPrefix(text, "<?xml") || strings.HasPrefix(text, "<svg") || strings.Contains(text, "<svg"))
	require.Contains(t, text, "svg")
}

func TestWriteMemoryChart_producesNonEmptySVG(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	base := time.Unix(1700000000, 0).UTC()
	phases := []PhaseSpan{{Name: monitor.PhaseIdle, Start: base, End: base.Add(5 * time.Second)}}
	store := []monitor.SamplePoint{{At: base, Phase: monitor.PhaseIdle, RSSBytes: 100 << 20}}
	ch := []monitor.SamplePoint{{At: base, Phase: monitor.PhaseIdle, RSSBytes: 80 << 20}}
	path := filepath.Join(dir, "mem.svg")
	require.NoError(t, WriteMemoryChart(path, store, ch, phases))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Positive(t, info.Size())
}
