package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/segformat"
	"github.com/elk-utilities/prism/internal/store/testparquet"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

func TestHotSnapshotDuckDBExportAtomicNoWAL(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	clk := start
	e := New(Config{
		DataDir:              t.TempDir(),
		HotWindow:            time.Hour,
		HotSegmentFormat:     segformat.DuckDB,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	}, func() time.Time { return clk })
	t.Cleanup(func() { _ = e.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 42},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := e.ExportHotSnapshot(testTenant); err != nil {
		t.Fatalf("export: %v", err)
	}

	snap := filepath.Join(e.cfg.DataDir, testTenant, "hot", "current.duckdb")
	if _, err := os.Stat(snap); err != nil {
		t.Fatalf("hot/current.duckdb missing: %v", err)
	}
	if _, err := os.Stat(snap + ".wal"); !os.IsNotExist(err) {
		t.Fatalf("unexpected sibling wal after checkpointed export: %v", err)
	}
	legacy := filepath.Join(e.cfg.DataDir, testTenant, "hot", "current.parquet")
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatal("current.parquet should be removed when hot format is duckdb")
	}

	n := countAttachedMetrics(t, snap)
	if n != 1 {
		t.Fatalf("duckdb hot rows=%d, want 1", n)
	}
}

func TestConcurrentHotSnapshotDuckDBSameTenant(t *testing.T) {
	// A dashboard refresh fires many queries at once and each one exports the
	// tenant's hot snapshot first. Two DuckDB exports on the same tenant would
	// ATTACH the same alias and temp file on the same connection, so they must
	// be serialized rather than raced.
	start := time.Unix(1700000000, 0).UTC()
	clk := start
	e := New(Config{
		DataDir:              t.TempDir(),
		HotWindow:            time.Hour,
		HotSegmentFormat:     segformat.DuckDB,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	}, func() time.Time { return clk })
	t.Cleanup(func() { _ = e.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 42},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	const exporters = 16
	var wg sync.WaitGroup
	errs := make(chan error, exporters)
	for i := 0; i < exporters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- e.ExportHotSnapshot(testTenant)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent duckdb export: %v", err)
		}
	}

	snap := filepath.Join(e.cfg.DataDir, testTenant, "hot", "current.duckdb")
	if n := countAttachedMetrics(t, snap); n != 1 {
		t.Fatalf("duckdb hot rows=%d, want 1", n)
	}
	leftovers, _ := filepath.Glob(filepath.Join(e.cfg.DataDir, testTenant, "hot", "*.tmp"))
	if len(leftovers) != 0 {
		t.Fatalf("temp files should not remain: %v", leftovers)
	}
}

func TestFlushL0EmitsDuckDBWhenMergeFormatDuckDB(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	clk := start
	e := New(Config{
		DataDir:              t.TempDir(),
		HotWindow:            10 * time.Minute,
		MergeSegmentFormat:   segformat.DuckDB,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	}, func() time.Time { return clk })
	t.Cleanup(func() { _ = e.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	clk = start.Add(11 * time.Minute)
	if err := e.FlushDue(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	segs, err := ListL0(e.cfg.DataDir, testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("L0 count=%d, want 1", len(segs))
	}
	if filepath.Ext(segs[0]) != ".duckdb" {
		t.Fatalf("L0 ext=%q, want .duckdb", filepath.Ext(segs[0]))
	}
	if _, err := os.Stat(segs[0] + ".wal"); !os.IsNotExist(err) {
		t.Fatalf("unexpected L0 wal: %v", err)
	}
	if countAttachedMetrics(t, segs[0]) != 1 {
		t.Fatal("L0 duckdb should contain flushed rows")
	}
}

func countAttachedMetrics(t *testing.T, path string) int {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	alias := "chk"
	attach := "ATTACH '" + filepath.ToSlash(path) + "' AS " + alias + " (READ_ONLY)"
	if _, err := db.ExecContext(ctx, attach); err != nil {
		t.Fatalf("attach: %v", err)
	}
	var n int
	q := "SELECT COUNT(*) FROM " + alias + "." + segformat.MetricsTable
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}
