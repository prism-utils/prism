package lifecycle

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const logsRefreshTenant = "user-logrefresh01-apps"

// seedLandingWindows writes n landing windows one minute apart starting at base.
func seedLandingWindows(t *testing.T, dataDir, tenant, artifact string, base time.Time, n int) {
	t.Helper()
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		testparquet.WriteLogsRawFile(t, filepath.Join(landing, layout.SegmentName(at)), []testparquet.LogRow{
			{Message: "line", Format: "none"},
		})
	}
}

// refreshRunner drives merges with a one-pack-per-two-files budget: the derived
// max-merge-at-once is MaxSegmentBytes/FloorBytes, so a drain needs one action
// per pair of landing files.
func refreshRunner(t *testing.T, dataDir string, now func() time.Time, interval time.Duration, maxActions, segmentsPerTier int) *Runner {
	t.Helper()
	eng := engine.New(engine.Config{DataDir: dataDir}, now)
	t.Cleanup(func() { _ = eng.Close() })
	return NewRunner(&Config{
		DataDir:               dataDir,
		SegmentsPerTier:       segmentsPerTier,
		MaxSegmentBytes:       1 << 30,
		FloorBytes:            1 << 29,
		RetentionDays:         15,
		MaxTier:               8,
		LogsRefreshInterval:   interval,
		LogsRefreshMaxActions: maxActions,
	}, eng, now)
}

func TestTickMergeAgeTriggersLandingRefreshBelowSegmentsPerTier(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	now := base.Add(5 * time.Minute)

	seedLandingWindows(t, dataDir, logsRefreshTenant, artifact, base, 2)
	runner := refreshRunner(t, dataDir, func() time.Time { return now }, time.Minute, 8, 6)

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}

	landing := layout.LogsLandingDir(dataDir, logsRefreshTenant, artifact)
	if got, err := countDirParquet(landing); err != nil || got != 0 {
		t.Fatalf("landing files = %d (err %v), want 0 after age-triggered refresh", got, err)
	}
	l0 := layout.LogsTierDir(dataDir, logsRefreshTenant, artifact, 0)
	if got, err := countDirParquet(l0); err != nil || got != 1 {
		t.Fatalf("L0 files = %d (err %v), want 1 refreshed segment", got, err)
	}
}

func TestTickMergeAgeBelowIntervalLeavesLandingBuffered(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	now := base.Add(30 * time.Second)

	seedLandingWindows(t, dataDir, logsRefreshTenant, artifact, base, 1)
	runner := refreshRunner(t, dataDir, func() time.Time { return now }, time.Minute, 8, 6)

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}

	landing := layout.LogsLandingDir(dataDir, logsRefreshTenant, artifact)
	if got, err := countDirParquet(landing); err != nil || got != 1 {
		t.Fatalf("landing files = %d (err %v), want the window still buffered", got, err)
	}
	l0 := layout.LogsTierDir(dataDir, logsRefreshTenant, artifact, 0)
	if got, err := countDirParquet(l0); err != nil || got != 0 {
		t.Fatalf("L0 files = %d (err %v), want 0 before the interval elapses", got, err)
	}
}

func TestTickMergeDrainsMultipleLandingRefreshesPerTick(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	now := base.Add(time.Hour)

	seedLandingWindows(t, dataDir, logsRefreshTenant, artifact, base, 8)
	runner := refreshRunner(t, dataDir, func() time.Time { return now }, time.Minute, 8, 2)

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}

	landing := layout.LogsLandingDir(dataDir, logsRefreshTenant, artifact)
	if got, err := countDirParquet(landing); err != nil || got != 0 {
		t.Fatalf("landing files = %d (err %v), want a fully drained landing zone", got, err)
	}
	l0 := layout.LogsTierDir(dataDir, logsRefreshTenant, artifact, 0)
	got, err := countDirParquet(l0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 4 {
		t.Fatalf("L0 files = %d, want 4 refreshes (8 landing files, 2 per action) in one tick", got)
	}
}

func TestTickMergeLandingDrainStopsAtMaxActions(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	now := base.Add(time.Hour)

	seedLandingWindows(t, dataDir, logsRefreshTenant, artifact, base, 8)
	runner := refreshRunner(t, dataDir, func() time.Time { return now }, time.Minute, 3, 2)

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}

	l0 := layout.LogsTierDir(dataDir, logsRefreshTenant, artifact, 0)
	if got, err := countDirParquet(l0); err != nil || got != 3 {
		t.Fatalf("L0 files = %d (err %v), want the 3-action cap", got, err)
	}
	landing := layout.LogsLandingDir(dataDir, logsRefreshTenant, artifact)
	if got, err := countDirParquet(landing); err != nil || got != 2 {
		t.Fatalf("landing files = %d (err %v), want 2 left for the next tick", got, err)
	}
}

func TestTickFlushSealsCoalescedLogBuffer(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	eng := engine.New(engine.Config{
		DataDir:           dataDir,
		LogCoalesceMaxAge: time.Minute,
	}, clock)
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{DataDir: dataDir, MaxTier: 8}, eng, clock)

	tmp := filepath.Join(t.TempDir(), "chunk.parquet")
	testparquet.WriteLogsRawFile(t, tmp, []testparquet.LogRow{{Message: "buffered", Format: "none"}})
	body, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.LandLogWindow(logsRefreshTenant, artifact, bytes.NewReader(body)); err != nil {
		t.Fatalf("land coalesced: %v", err)
	}

	landing := layout.LogsLandingDir(dataDir, logsRefreshTenant, artifact)
	if got, err := countDirParquet(landing); err != nil || got != 0 {
		t.Fatalf("landing files = %d (err %v), want 0 while the buffer is open", got, err)
	}

	now = now.Add(2 * time.Minute)
	if err := runner.TickFlush(); err != nil {
		t.Fatalf("TickFlush: %v", err)
	}
	if got, err := countDirParquet(landing); err != nil || got != 1 {
		t.Fatalf("landing files = %d (err %v), want the aged coalesce buffer sealed by the flush tick", got, err)
	}
	if _, err := os.Stat(filepath.Join(landing, ".pending")); !os.IsNotExist(err) {
		t.Fatalf("sealed buffer must leave no .pending directory, stat err = %v", err)
	}
}

func TestTickFlushWithoutCoalesceConfigured(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	eng := engine.New(engine.Config{DataDir: dataDir}, clock)
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{DataDir: dataDir, MaxTier: 8}, eng, clock)

	if err := runner.TickFlush(); err != nil {
		t.Fatalf("TickFlush with coalesce disabled: %v", err)
	}
}
