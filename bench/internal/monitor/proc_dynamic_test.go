package monitor_test

import (
	"context"
	"os/exec"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elk-utilities/prism/bench/internal/monitor"
	"github.com/stretchr/testify/require"
)

func TestProcSamplerFunc_pidZeroYieldsZeroUsage(t *testing.T) {
	s := monitor.NewProcSamplerFunc(func() int { return 0 })
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(120 * time.Millisecond)
	cancel()
	usage := s.Stop()

	require.Equal(t, 0.0, usage.CPUCoresPeak)
	require.Equal(t, uint64(0), usage.RSSPeakBytes)
	require.Greater(t, usage.DurationSec, 0.0)
}

func TestProcSamplerFunc_followsPIDChanges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process tree sampling differs on Windows CI")
	}

	child1 := startBusyChild(t)
	t.Cleanup(func() { _ = child1.Process.Kill() })

	var current atomic.Int64
	current.Store(int64(child1.Process.Pid))

	s := monitor.NewProcSamplerFunc(func() int { return int(current.Load()) })
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(350 * time.Millisecond)

	require.NoError(t, child1.Process.Kill())
	_, _ = child1.Process.Wait()

	child2 := startBusyChild(t)
	t.Cleanup(func() { _ = child2.Process.Kill() })
	current.Store(int64(child2.Process.Pid))

	time.Sleep(350 * time.Millisecond)
	cancel()
	usage := s.Stop()

	require.Greater(t, usage.CPUCoresPeak, 0.01, "expected non-zero CPU after pid switch")
	require.Greater(t, usage.RSSPeakBytes, uint64(1024), "expected non-zero RSS after pid switch")
}

func TestProcStreamSamplerFunc_followsPIDChanges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process tree sampling differs on Windows CI")
	}

	child1 := startBusyChild(t)
	t.Cleanup(func() { _ = child1.Process.Kill() })

	var current atomic.Int64
	current.Store(int64(child1.Process.Pid))

	stream := monitor.NewProcStreamSamplerFunc(func() int { return int(current.Load()) })
	ctx, cancel := context.WithCancel(context.Background())
	stream.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	require.NoError(t, child1.Process.Kill())
	_, _ = child1.Process.Wait()

	child2 := startBusyChild(t)
	t.Cleanup(func() { _ = child2.Process.Kill() })
	current.Store(int64(child2.Process.Pid))

	time.Sleep(200 * time.Millisecond)
	stream.ForceSample(ctx)
	pts := stream.Stop()
	cancel()

	require.NotEmpty(t, pts)
	var peakCPU float64
	for _, p := range pts {
		if p.CPUCores > peakCPU {
			peakCPU = p.CPUCores
		}
	}
	require.Greater(t, peakCPU, 0.0, "stream sampler should observe CPU on switched pid")
}

func startBusyChild(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "bash", "-c", "while true; do :; done")
	require.NoError(t, cmd.Start())
	return cmd
}
