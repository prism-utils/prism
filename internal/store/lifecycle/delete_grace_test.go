package lifecycle

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
)

const deleteGraceTenant = "user-logrefresh01-apps"

func graceRunner(t *testing.T, dataDir string, now func() time.Time, grace time.Duration) *Runner {
	t.Helper()
	eng := engine.New(engine.Config{DataDir: dataDir}, now)
	t.Cleanup(func() { _ = eng.Close() })
	return NewRunner(&Config{
		DataDir:               dataDir,
		SegmentsPerTier:       6,
		MaxSegmentBytes:       1 << 30,
		FloorBytes:            1 << 29,
		RetentionDays:         15,
		MaxTier:               8,
		LogsRefreshInterval:   time.Minute,
		LogsRefreshMaxActions: 8,
		DeleteGrace:           grace,
	}, eng, now)
}

func countCompactedMarkers(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && layout.IsCompactedMarker(e.Name()) {
			n++
		}
	}
	return n
}

// The reported production failure: Grafana expands a glob over the tier tree,
// then opens the files it found while a refresh deletes them. Holding the
// sources keeps those paths openable long enough for the reader to finish.
func TestTickMergeHoldsRefreshedSourcesThenPurgesAfterGrace(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	now := base.Add(5 * time.Minute)

	seedLandingWindows(t, dataDir, deleteGraceTenant, artifact, base, 2)
	runner := graceRunner(t, dataDir, func() time.Time { return now }, 120*time.Second)

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}

	landing := layout.LogsLandingDir(dataDir, deleteGraceTenant, artifact)
	l0 := layout.LogsTierDir(dataDir, deleteGraceTenant, artifact, 0)
	if got, err := countDirParquet(landing); err != nil || got != 2 {
		t.Fatalf("landing files = %d (err %v), want both sources held after the refresh", got, err)
	}
	if got := countCompactedMarkers(t, landing); got != 2 {
		t.Fatalf("markers = %d, want one per held source", got)
	}
	if got, err := countDirParquet(l0); err != nil || got != 1 {
		t.Fatalf("L0 files = %d (err %v), want the refresh output published right away", got, err)
	}

	// A held source is not a merge input: ticking again inside the window must
	// not refresh it a second time.
	now = now.Add(30 * time.Second)
	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge inside the grace window: %v", err)
	}
	if got, err := countDirParquet(landing); err != nil || got != 2 {
		t.Fatalf("landing files = %d (err %v), want the sources still held", got, err)
	}
	if got, err := countDirParquet(l0); err != nil || got != 1 {
		t.Fatalf("L0 files = %d (err %v), want no second refresh of held sources", got, err)
	}

	now = now.Add(3 * time.Minute)
	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge after the grace window: %v", err)
	}
	if got, err := countDirParquet(landing); err != nil || got != 0 {
		t.Fatalf("landing files = %d (err %v), want the expired sources purged", got, err)
	}
	if got := countCompactedMarkers(t, landing); got != 0 {
		t.Fatalf("markers = %d, want them purged with their segments", got)
	}
	if got, err := countDirParquet(l0); err != nil || got != 1 {
		t.Fatalf("L0 files = %d (err %v), want the refresh output untouched by the purge", got, err)
	}
}

func TestTickMergeWithoutGraceDeletesSourcesOnTheSpot(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	now := base.Add(5 * time.Minute)

	seedLandingWindows(t, dataDir, deleteGraceTenant, artifact, base, 2)
	runner := graceRunner(t, dataDir, func() time.Time { return now }, 0)

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}
	landing := layout.LogsLandingDir(dataDir, deleteGraceTenant, artifact)
	if got, err := countDirParquet(landing); err != nil || got != 0 {
		t.Fatalf("landing files = %d (err %v), want 0 with the grace disabled", got, err)
	}
	if got := countCompactedMarkers(t, landing); got != 0 {
		t.Fatalf("markers = %d, want none with the grace disabled", got)
	}
}

// A tenant that stopped merging still has its expired holds reclaimed, since
// the merge tick walks every tenant whether or not it plans an action.
func TestTickMergePurgesExpiredHoldWithoutPlanningAMerge(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	tierDir := layout.LogsTierDir(dataDir, deleteGraceTenant, artifact, 0)
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}
	held := filepath.Join(tierDir, "1786140844863329878-aaaaaaaa.parquet")
	if err := os.WriteFile(held, []byte("segment"), 0o600); err != nil {
		t.Fatal(err)
	}
	expired := strconv.FormatInt(now.Add(-time.Minute).Unix(), 10) + "\n"
	if err := os.WriteFile(layout.CompactedMarker(held), []byte(expired), 0o600); err != nil {
		t.Fatal(err)
	}

	runner := graceRunner(t, dataDir, func() time.Time { return now }, 120*time.Second)
	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}
	if _, err := os.Stat(held); !os.IsNotExist(err) {
		t.Fatalf("expired hold still present, stat err = %v", err)
	}
	if got := countCompactedMarkers(t, tierDir); got != 0 {
		t.Fatalf("markers = %d, want the expired one reaped", got)
	}
}
