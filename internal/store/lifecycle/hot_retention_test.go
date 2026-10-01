package lifecycle

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestTickRetentionDeletesExpiredHotSnapshotAndEngineRows(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-hotret01-apps"
	retentionNow := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	clk := retentionNow.Add(-8 * 24 * time.Hour)
	clock := func() time.Time { return clk }
	eng := engine.New(engine.Config{
		DataDir:          dataDir,
		HotWindow:        time.Hour,
		HotSegmentFormat: segformat.DuckDB,
	}, clock)
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:       dataDir,
		RetentionDays: 7,
		MaxTier:       8,
	}, eng, clock)

	ingestMetricsWindow(t, eng, tenant)
	if err := eng.ExportHotSnapshot(tenant); err != nil {
		t.Fatalf("export: %v", err)
	}
	snap := filepath.Join(dataDir, tenant, "hot", "current.duckdb")
	if _, err := os.Stat(snap); err != nil {
		t.Fatalf("snapshot missing before retention: %v", err)
	}

	clk = retentionNow
	if err := runner.TickRetention(); err != nil {
		t.Fatalf("TickRetention: %v", err)
	}

	n, err := eng.HotRowCount(tenant)
	if err != nil {
		t.Fatalf("HotRowCount: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 expired hot rows, got %d", n)
	}
	tsList, err := eng.QueryHotTs(tenant)
	if err != nil {
		t.Fatalf("QueryHotTs: %v", err)
	}
	if len(tsList) != 0 {
		t.Fatalf("want no hot timestamps, got %v", tsList)
	}
	if _, err := os.Stat(snap); !os.IsNotExist(err) {
		t.Fatalf("expired snapshot should be gone, stat err = %v", err)
	}
	if _, err := os.Stat(snap + ".wal"); !os.IsNotExist(err) {
		t.Fatalf("expired snapshot wal should be gone, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, tenant, "engine.duckdb")); err != nil {
		t.Fatalf("engine catalog must remain: %v", err)
	}
	assertNoL0(t, dataDir, tenant)
}

func TestTickRetentionKeepsHotSnapshotInsideWindow(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-hotret02-apps"
	retentionNow := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	cutoff := retentionNow.Add(-7 * 24 * time.Hour)
	inside := retentionNow.Add(-3 * 24 * time.Hour)
	clk := cutoff
	clock := func() time.Time { return clk }
	eng := engine.New(engine.Config{
		DataDir:          dataDir,
		HotWindow:        30 * 24 * time.Hour,
		HotSegmentFormat: segformat.DuckDB,
	}, clock)
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:       dataDir,
		RetentionDays: 7,
		MaxTier:       8,
	}, eng, clock)

	ingestMetricsWindow(t, eng, tenant)
	clk = inside
	ingestMetricsWindow(t, eng, tenant)
	if err := eng.ExportHotSnapshot(tenant); err != nil {
		t.Fatalf("export: %v", err)
	}

	clk = retentionNow
	if err := runner.TickRetention(); err != nil {
		t.Fatalf("TickRetention: %v", err)
	}

	n, err := eng.HotRowCount(tenant)
	if err != nil {
		t.Fatalf("HotRowCount: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 in-window hot rows, got %d", n)
	}
	got, err := eng.QueryHotTs(tenant)
	if err != nil {
		t.Fatalf("QueryHotTs: %v", err)
	}
	want := []time.Time{cutoff.UTC(), inside.UTC()}
	if len(got) != len(want) {
		t.Fatalf("want timestamps %v, got %v", want, got)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("timestamp %d: want %s, got %s", i, want[i], got[i])
		}
	}
	snap := filepath.Join(dataDir, tenant, "hot", "current.duckdb")
	if _, err := os.Stat(snap); err != nil {
		t.Fatalf("in-window snapshot missing: %v", err)
	}
	assertNoL0(t, dataDir, tenant)

	t.Run("mixed expired and inside", func(t *testing.T) {
		mixedDir := t.TempDir()
		mixedTenant := "user-hotret02b-apps"
		mixedClk := cutoff.Add(-time.Hour)
		mixedClock := func() time.Time { return mixedClk }
		mixedEng := engine.New(engine.Config{
			DataDir:          mixedDir,
			HotWindow:        30 * 24 * time.Hour,
			HotSegmentFormat: segformat.DuckDB,
		}, mixedClock)
		t.Cleanup(func() { _ = mixedEng.Close() })
		mixedRunner := NewRunner(&Config{
			DataDir:       mixedDir,
			RetentionDays: 7,
			MaxTier:       8,
		}, mixedEng, mixedClock)

		ingestMetricsWindow(t, mixedEng, mixedTenant)
		mixedClk = inside
		ingestMetricsWindow(t, mixedEng, mixedTenant)
		if err := mixedEng.ExportHotSnapshot(mixedTenant); err != nil {
			t.Fatalf("export: %v", err)
		}

		mixedClk = retentionNow
		if err := mixedRunner.TickRetention(); err != nil {
			t.Fatalf("TickRetention: %v", err)
		}

		n, err := mixedEng.HotRowCount(mixedTenant)
		if err != nil {
			t.Fatalf("HotRowCount: %v", err)
		}
		if n != 1 {
			t.Fatalf("want 1 remaining in-window row, got %d", n)
		}
		got, err := mixedEng.QueryHotTs(mixedTenant)
		if err != nil {
			t.Fatalf("QueryHotTs: %v", err)
		}
		if len(got) != 1 || !got[0].Equal(inside.UTC()) {
			t.Fatalf("want only inside ts %s, got %v", inside.UTC(), got)
		}
		if _, err := os.Stat(filepath.Join(mixedDir, mixedTenant, "hot", "current.duckdb")); err != nil {
			t.Fatalf("rewritten snapshot missing: %v", err)
		}
		assertNoL0(t, mixedDir, mixedTenant)
	})
}

func TestTickRetentionZeroIngestStillDeletes(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-hotret03-apps"
	retentionNow := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	expired := retentionNow.Add(-8 * 24 * time.Hour)
	clk := retentionNow
	clock := func() time.Time { return clk }
	eng := engine.New(engine.Config{
		DataDir:          dataDir,
		HotWindow:        time.Hour,
		HotSegmentFormat: segformat.DuckDB,
	}, clock)
	t.Cleanup(func() { _ = eng.Close() })

	plantHotRow(t, eng, tenant, "hot_current", expired)
	plantHotRow(t, eng, tenant, "hot_prev", expired)
	if err := eng.ExportHotSnapshot(tenant); err != nil {
		t.Fatalf("export: %v", err)
	}

	runner := NewRunner(&Config{
		DataDir:       dataDir,
		RetentionDays: 7,
		MaxTier:       8,
	}, eng, clock)
	if err := runner.TickRetention(); err != nil {
		t.Fatalf("TickRetention: %v", err)
	}

	n, err := eng.HotRowCount(tenant)
	if err != nil {
		t.Fatalf("HotRowCount: %v", err)
	}
	if n != 0 {
		t.Fatalf("want 0 hot_current rows after zero-ingest retention, got %d", n)
	}
	if got := hotTableCount(t, eng, tenant, "hot_prev"); got != 0 {
		t.Fatalf("want 0 hot_prev rows after zero-ingest retention, got %d", got)
	}
	snap := filepath.Join(dataDir, tenant, "hot", "current.duckdb")
	if _, err := os.Stat(snap); !os.IsNotExist(err) {
		t.Fatalf("expired snapshot should be gone, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, tenant, "engine.duckdb")); err != nil {
		t.Fatalf("engine catalog must remain: %v", err)
	}
	assertNoL0(t, dataDir, tenant)
}

func TestTickRetentionStillDeletesExpiredL0(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-hotret04-apps"
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	l0 := layout.TierDir(dataDir, tenant, 0)
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	expired := filepath.Join(l0, "expired.parquet")
	testparquet.WriteSegmentWithTs(t, expired, now.Add(-8*24*time.Hour), "old", 1)
	keep := filepath.Join(l0, "boundary.parquet")
	testparquet.WriteSegmentWithTs(t, keep, now.Add(-7*24*time.Hour), "keep", 1)

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:       dataDir,
		RetentionDays: 7,
		MaxTier:       8,
	}, eng, func() time.Time { return now })

	if err := runner.TickRetention(); err != nil {
		t.Fatalf("TickRetention: %v", err)
	}
	if _, err := os.Stat(expired); !os.IsNotExist(err) {
		t.Fatalf("expired L0 should be deleted, stat err = %v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("boundary L0 should be kept: %v", err)
	}
}

func ingestMetricsWindow(t *testing.T, eng *engine.Engine, tenant string) {
	t.Helper()
	path := testparquet.WriteWindow(t, t.TempDir(), "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := eng.Ingest(tenant, f); err != nil {
		t.Fatalf("ingest: %v", err)
	}
}

func plantHotRow(t *testing.T, eng *engine.Engine, tenant, table string, ts time.Time) {
	t.Helper()
	db, err := eng.DB(tenant)
	if err != nil {
		t.Fatalf("DB: %v", err)
	}
	q := `INSERT INTO ` + table + ` ("__name__", labels, value, timestamp_ms, ts) VALUES ('up', '{}', 1.0, 0, ?)`
	if _, err := db.ExecContext(context.Background(), q, ts.UTC()); err != nil {
		t.Fatalf("plant %s: %v", table, err)
	}
}

func hotTableCount(t *testing.T, eng *engine.Engine, tenant, table string) int64 {
	t.Helper()
	var n int64
	err := eng.WithRead(tenant, func(db *sql.DB) error {
		return db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&n)
	})
	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func assertNoL0(t *testing.T, dataDir, tenant string) {
	t.Helper()
	dir := layout.TierDir(dataDir, tenant, 0)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("list L0: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("retention must not flush hot to L0, got %d files", len(entries))
	}
}
