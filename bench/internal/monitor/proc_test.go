package monitor_test

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/prism-utils/prism/bench/internal/monitor"
	"github.com/stretchr/testify/require"
)

func TestProcSampler_burnCPUAndAlloc(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process tree sampling differs on Windows CI")
	}
	done := make(chan struct{})
	go func() {
		buf := make([][]byte, 0, 64)
		for {
			select {
			case <-done:
				return
			default:
				for i := 0; i < 2000; i++ {
					_ = heavyCPU()
				}
				buf = append(buf, make([]byte, 256*1024))
				if len(buf) > 128 {
					buf = buf[1:]
				}
			}
		}
	}()

	s := monitor.NewProcSampler(os.Getpid())
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(400 * time.Millisecond)
	cancel()
	usage := s.Stop()
	close(done)

	require.Greater(t, usage.CPUCoresPeak, 0.01, "expected measurable CPU during burn")
	require.Greater(t, usage.RSSPeakBytes, uint64(8*1024*1024), "expected RSS growth from allocations")
	require.Greater(t, usage.DurationSec, 0.0)
	if runtime.GOOS != "linux" {
		require.False(t, usage.IOAvailable(), "per-process IOPS unavailable off Linux")
	}
}

func heavyCPU() float64 {
	x := 1.0001
	for i := 0; i < 500; i++ {
		x = x*x + 0.0001
	}
	return x
}
