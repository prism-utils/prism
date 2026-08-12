// Package timing provides warm-and-repeat latency measurement helpers.
package timing

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prism-utils/prism/bench/internal/monitor"
)

const defaultRuns = 5

// Stats holds p50, p95, and min over K timed runs in milliseconds.
type Stats struct {
	P50Ms float64
	P95Ms float64
	MinMs float64
}

// QueryOutcome pairs latency stats with resource usage from the timed window.
type QueryOutcome struct {
	Stats Stats
	Usage monitor.Usage
}

// RunQuery warms once then executes fn K times, returning latency stats in ms.
func RunQuery(runs int, fn func() error) (Stats, error) {
	out, err := RunQueryMonitored(runs, fn, nil)
	if err != nil {
		return Stats{}, err
	}
	return out.Stats, nil
}

// RunQueryMonitored warms once, samples during K timed runs, then returns stats and usage.
func RunQueryMonitored(runs int, fn func() error, sampler monitor.Sampler) (QueryOutcome, error) {
	if runs <= 0 {
		runs = defaultRuns
	}
	if err := fn(); err != nil {
		return QueryOutcome{}, fmt.Errorf("timing: warm run: %w", err)
	}
	if sampler != nil {
		sampler.Start(context.Background())
	}
	durations := make([]float64, 0, runs)
	for i := 0; i < runs; i++ {
		start := time.Now()
		if err := fn(); err != nil {
			if sampler != nil {
				_ = sampler.Stop()
			}
			return QueryOutcome{}, fmt.Errorf("timing: run %d: %w", i+1, err)
		}
		durations = append(durations, float64(time.Since(start).Microseconds())/1000.0)
	}
	var usage monitor.Usage
	if sampler != nil {
		usage = sampler.Stop()
	}
	sort.Float64s(durations)
	return QueryOutcome{
		Stats: Stats{
			P50Ms: durations[len(durations)*50/100],
			P95Ms: durations[len(durations)*95/100],
			MinMs: durations[0],
		},
		Usage: usage,
	}, nil
}

// WallRun times fn once and returns elapsed seconds.
func WallRun(fn func() error) (float64, error) {
	start := time.Now()
	if err := fn(); err != nil {
		return 0, err
	}
	return time.Since(start).Seconds(), nil
}

// WallRunMonitored times fn once while sampler is active.
func WallRunMonitored(ctx context.Context, fn func() error, sampler monitor.Sampler) (float64, monitor.Usage, error) {
	start := time.Now()
	if sampler != nil {
		sampler.Start(ctx)
	}
	if err := fn(); err != nil {
		var usage monitor.Usage
		if sampler != nil {
			usage = sampler.Stop()
		}
		return 0, usage, err
	}
	sec := time.Since(start).Seconds()
	var usage monitor.Usage
	if sampler != nil {
		usage = sampler.Stop()
	}
	return sec, usage, nil
}
