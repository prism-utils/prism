package lifecycle

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const (
	failsafeBadTenant  = "user-failsafe01-apps"
	failsafeGoodTenant = "user-failsafe02-apps"
)

func TestTickRetentionEmptyRollupDoesNotBlockLogFileCap(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	// Poison rollup on bad tenant (#95): empty parquet → NULL max(bucket).
	rollupDir := layout.RollupDir(dataDir, failsafeBadTenant, "1m")
	if err := os.MkdirAll(rollupDir, 0o750); err != nil {
		t.Fatal(err)
	}
	emptyPath := filepath.Join(rollupDir, "empty.parquet")
	testparquet.WriteEmptyRollup(t, emptyPath)

	// Healthy tenant with too many log files.
	const maxFiles = 3
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, failsafeGoodTenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxFiles+4; i++ {
		at := now.Add(-time.Duration(i) * time.Hour)
		testparquet.WriteLogsRawFile(t, filepath.Join(landing, layout.SegmentName(at)), []testparquet.LogRow{
			{Message: "line", Format: "none"},
		})
	}

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })

	runner := NewRunner(&Config{
		DataDir:       dataDir,
		RetentionDays: 15,
		MaxLogFiles:   maxFiles,
		RollupSteps:   "1m",
		MaxTier:       8,
	}, eng, func() time.Time { return now })

	if err := runner.TickRetention(); err != nil {
		t.Fatalf("TickRetention must not abort on empty rollup: %v", err)
	}
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("empty/unusable rollup should be deleted, still present: %v", err)
	}
	got, err := countLogParquet(dataDir, failsafeGoodTenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if got > maxFiles {
		t.Fatalf("MAX_LOG_FILES not enforced after empty rollup: count=%d want<=%d", got, maxFiles)
	}
}

func TestTickMergeContinuesAfterTenantError(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	artifact := "logs-raw"

	// Bad tenant: logs path is a file → ListLogArtifacts / ReadDir fails.
	badLogs := filepath.Join(dataDir, failsafeBadTenant, "logs")
	if err := os.MkdirAll(filepath.Dir(badLogs), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(badLogs, []byte("not-a-dir"), 0o640); err != nil {
		t.Fatal(err)
	}

	landing := layout.LogsLandingDir(dataDir, failsafeGoodTenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	const segmentsPerTier = 6
	for i := 0; i < segmentsPerTier; i++ {
		at := now.Add(time.Duration(i) * time.Minute)
		testparquet.WriteLogsRawFile(t, filepath.Join(landing, layout.SegmentName(at)), []testparquet.LogRow{
			{Message: "x", Format: "none"},
		})
	}

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })

	runner := NewRunner(&Config{
		DataDir:         dataDir,
		SegmentsPerTier: segmentsPerTier,
		MaxSegmentBytes: 1 << 30,
		FloorBytes:      1 << 20,
		MaxTier:         8,
	}, eng, func() time.Time { return now })

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge must continue after bad tenant: %v", err)
	}
	l0, err := countDirParquet(layout.LogsTierDir(dataDir, failsafeGoodTenant, artifact, 0))
	if err != nil {
		t.Fatal(err)
	}
	if l0 < 1 {
		t.Fatalf("good tenant should still merge to L0, got %d files", l0)
	}
}

func TestFlushDueContinuesAfterTenantError(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	now := start
	clock := func() time.Time { return now }
	hotWindow := time.Minute

	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: hotWindow}, clock)
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	for _, tenant := range []string{failsafeBadTenant, failsafeGoodTenant} {
		path := testparquet.WriteWindow(t, dir, tenant+".parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
		})
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Ingest(tenant, f); err != nil {
			_ = f.Close()
			t.Fatalf("ingest %s: %v", tenant, err)
		}
		_ = f.Close()
	}

	badL0 := filepath.Join(dataDir, failsafeBadTenant, "tiers", "L0")
	if err := os.MkdirAll(filepath.Dir(badL0), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(badL0, []byte("blocked"), 0o640); err != nil {
		t.Fatal(err)
	}

	now = start.Add(hotWindow)
	if err := eng.FlushDue(); err != nil {
		t.Fatalf("FlushDue must continue after bad tenant: %v", err)
	}
	goodL0 := layout.TierDir(dataDir, failsafeGoodTenant, 0)
	entries, err := os.ReadDir(goodL0)
	if err != nil {
		t.Fatalf("good tenant L0 missing: %v", err)
	}
	if len(entries) < 1 {
		t.Fatalf("good tenant should have flushed an L0 segment")
	}
}

func TestExportHotSnapshotsContinuesAfterTenantError(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	path := testparquet.WriteWindow(t, t.TempDir(), "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Ingest(failsafeGoodTenant, f); err != nil {
		_ = f.Close()
		t.Fatalf("ingest: %v", err)
	}
	_ = f.Close()

	if err := os.MkdirAll(filepath.Join(dataDir, "INVALID"), 0o750); err != nil {
		t.Fatal(err)
	}

	if err := eng.ExportHotSnapshots(); err != nil {
		t.Fatalf("ExportHotSnapshots must continue after bad tenant: %v", err)
	}
	snap := filepath.Join(dataDir, failsafeGoodTenant, "hot", "current.parquet")
	if _, err := os.Stat(snap); err != nil {
		t.Fatalf("good tenant snapshot missing: %v", err)
	}
}
