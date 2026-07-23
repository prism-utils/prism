// Package timing provides warm-and-repeat latency measurement helpers.
package timing

import (
	"fmt"
	"sort"
	"time"
)

const defaultRuns = 5

// Stats holds p50, p95, and min over K timed runs in milliseconds.
type Stats struct {
	P50Ms float64
	P95Ms float64
	MinMs float64
}

// RunQuery warms once then executes fn K times, returning latency stats in ms.
func RunQuery(runs int, fn func() error) (Stats, error) {
	if runs <= 0 {
		runs = defaultRuns
	}
	if err := fn(); err != nil {
		return Stats{}, fmt.Errorf("timing: warm run: %w", err)
	}
	durations := make([]float64, 0, runs)
	for i := 0; i < runs; i++ {
		start := time.Now()
		if err := fn(); err != nil {
			return Stats{}, fmt.Errorf("timing: run %d: %w", i+1, err)
		}
		durations = append(durations, float64(time.Since(start).Microseconds())/1000.0)
	}
	sort.Float64s(durations)
	return Stats{
		P50Ms: durations[len(durations)*50/100],
		P95Ms: durations[len(durations)*95/100],
		MinMs: durations[0],
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
