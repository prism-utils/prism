package lifecycle

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/stats"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const lifecycleTenant = "user-6f3a9c2b-apps"

func TestLifecycleIngestFlushMergeRollupsAndRetention(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	hotWindow := time.Minute

	now := start
	autoAdvance := false
	clock := func() time.Time {
		if autoAdvance {
			now = now.Add(time.Second)
		}
		return now
	}

	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: hotWindow}, clock)
	t.Cleanup(func() { _ = eng.Close() })

	runner := NewRunner(Config{
		DataDir:         dataDir,
		SegmentsPerTier: 6,
		MaxSegmentBytes: 1 << 30,
		FloorBytes:      FloorBytesFromHotWindow(hotWindow),
		RetentionDays:   15,
		RollupSteps:     "1m,5m,1h",
		MaxTier:         8,
	}, eng, clock)

	dir := t.TempDir()
	for i := 0; i < 6; i++ {
		path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: float64(i + 1), TimestampMs: 0},
		})
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eng.Ingest(lifecycleTenant, f); err != nil {
			_ = f.Close()
			t.Fatalf("ingest %d: %v", i, err)
		}
		_ = f.Close()
		now = now.Add(hotWindow)
		if err := runner.TickFlush(); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}

	l0Dir := layout.TierDir(dataDir, lifecycleTenant, 0)
	l0Entries, err := os.ReadDir(l0Dir)
	if err != nil {
		t.Fatalf("list L0: %v", err)
	}
	if len(l0Entries) != 6 {
		t.Fatalf("want 6 L0 segments after 6 flushes, got %d", len(l0Entries))
	}

	autoAdvance = true
	if err := runner.TickMerge(); err != nil {
		t.Fatalf("merge: %v", err)
	}

	l1Dir := layout.TierDir(dataDir, lifecycleTenant, 1)
	l1Entries, err := os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("list L1: %v", err)
	}
	if len(l1Entries) != 1 {
		t.Fatalf("want 1 L1 segment after merge, got %d", len(l1Entries))
	}
	l1Path := filepath.Join(l1Dir, l1Entries[0].Name())

	if _, err := os.Stat(l0Dir); err == nil {
		remaining, _ := os.ReadDir(l0Dir)
		if len(remaining) != 0 {
			t.Fatalf("L0 sources should be deleted after merge, got %d files", len(remaining))
		}
	}

	db, err := eng.DB(lifecycleTenant)
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	if err := assertGapFreeOrderedByTs(t, db, l1Path, 6); err != nil {
		t.Fatal(err)
	}

	for _, step := range []string{"1m", "5m", "1h"} {
		rollupDir := layout.RollupDir(dataDir, lifecycleTenant, step)
		entries, err := os.ReadDir(rollupDir)
		if err != nil {
			t.Fatalf("rollups/%s: %v", step, err)
		}
		if len(entries) != 1 {
			t.Fatalf("want 1 rollup file in %s, got %d", step, len(entries))
		}
	}

	cpu, err := stats.CompactionCPUSeconds(dataDir, lifecycleTenant)
	if err != nil {
		t.Fatalf("metering read: %v", err)
	}
	if cpu <= 0 {
		t.Fatalf("compactionCpuSeconds should be incremented after merge, got %v", cpu)
	}

	retentionNow := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	oldPath := filepath.Join(dataDir, lifecycleTenant, "tiers", "L0", "expired.parquet")
	testparquet.WriteSegmentWithTs(t, oldPath, retentionNow.Add(-16*24*time.Hour), "old", 1)
	keepPath := filepath.Join(dataDir, lifecycleTenant, "tiers", "L0", "boundary.parquet")
	testparquet.WriteSegmentWithTs(t, keepPath, retentionNow.Add(-15*24*time.Hour), "keep", 1)

	autoAdvance = false
	now = retentionNow
	if err := runner.TickRetention(); err != nil {
		t.Fatalf("retention: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("16-day-old segment should be deleted")
	}
	if _, err := os.Stat(keepPath); err != nil {
		t.Fatalf("15-day boundary segment should be kept: %v", err)
	}
}

func assertGapFreeOrderedByTs(t *testing.T, db *sql.DB, path string, wantRows int) error {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "SELECT ts FROM read_parquet(?) ORDER BY ts", path)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var prev time.Time
	count := 0
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			return err
		}
		if !prev.IsZero() && ts.Before(prev) {
			t.Fatalf("rows not ordered by ts")
		}
		prev = ts
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != wantRows {
		t.Fatalf("want %d rows in merged segment, got %d", wantRows, count)
	}
	return nil
}

func TestRetentionRollupFiles(t *testing.T) {
	dataDir := t.TempDir()
	tenant := lifecycleTenant
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

	oldRollup := layout.RollupDir(dataDir, tenant, "1m")
	if err := os.MkdirAll(oldRollup, 0o750); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(oldRollup, "old.parquet")
	testparquet.WriteRollupBucket(t, oldPath, now.Add(-16*24*time.Hour), "m", 1.0)

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })

	runner := NewRunner(Config{
		DataDir:       dataDir,
		RetentionDays: 15,
		RollupSteps:   "1m",
		MaxTier:       8,
	}, eng, func() time.Time { return now })

	if err := runner.TickRetention(); err != nil {
		t.Fatalf("retention: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("expired rollup should be deleted")
	}
}

func TestRemovePathAlreadyGoneNoError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.parquet")
	if err := removePath(path); err != nil {
		t.Fatalf("remove missing path: %v", err)
	}
}

func TestTickRetentionSecondPassNoError(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	expired := filepath.Join(dataDir, lifecycleTenant, "tiers", "L0", "old.parquet")
	testparquet.WriteSegmentWithTs(t, expired, now.Add(-16*24*time.Hour), "old", 1)

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })

	runner := NewRunner(Config{
		DataDir:       dataDir,
		RetentionDays: 15,
		MaxTier:       8,
	}, eng, func() time.Time { return now })

	if err := runner.TickRetention(); err != nil {
		t.Fatalf("first retention: %v", err)
	}
	if err := runner.TickRetention(); err != nil {
		t.Fatalf("second retention pass must tolerate already-removed targets: %v", err)
	}
}
