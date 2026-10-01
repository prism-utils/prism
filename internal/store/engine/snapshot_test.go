package engine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/testparquet"
	"go.uber.org/goleak"
)

func TestHotSnapshotExportWithinInterval(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, now := testEngine(t, start, time.Hour)

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 42},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	snapPath := filepath.Join(e.cfg.DataDir, testTenant, "hot", "current.parquet")
	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Fatalf("snapshot should not exist before first export tick")
	}

	if err := e.ExportHotSnapshots(); err != nil {
		t.Fatalf("export: %v", err)
	}
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("snapshot missing after export: %v", err)
	}

	path2 := testparquet.WriteWindow(t, dir, "w2.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 2, TimestampMs: 43},
	})
	*now = start.Add(5 * time.Second)
	if _, err := e.Ingest(testTenant, readFile(t, path2)); err != nil {
		t.Fatalf("ingest2: %v", err)
	}
	if err := e.ExportHotSnapshots(); err != nil {
		t.Fatalf("export2: %v", err)
	}
	te, _ := e.open(testTenant)
	var cnt int
	if err := te.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM read_parquet(?)", snapPath).Scan(&cnt); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("want 2 rows in snapshot, got %d", cnt)
	}
}

func TestHotSnapshotRowsSortedByTs(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, now := testEngine(t, start, time.Hour)
	dir := t.TempDir()

	for i := 0; i < 3; i++ {
		*now = start.Add(time.Duration(i) * time.Minute)
		path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: float64(i), TimestampMs: int64(i)},
		})
		if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	if err := e.ExportHotSnapshots(); err != nil {
		t.Fatalf("export: %v", err)
	}

	snapPath := filepath.Join(e.cfg.DataDir, testTenant, "hot", "current.parquet")
	te, _ := e.open(testTenant)
	rows, err := te.db.QueryContext(context.Background(), "SELECT ts FROM read_parquet(?) ORDER BY ts", snapPath)
	if err != nil {
		t.Fatalf("query snapshot ts: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var prev time.Time
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !prev.IsZero() && ts.Before(prev) {
			t.Fatalf("snapshot rows not sorted by ts: %v before %v", ts, prev)
		}
		prev = ts
	}
}

func TestLegacyImportIdempotentAndStampsTsFromFilename(t *testing.T) {
	dataDir := t.TempDir()
	tenant := testTenant
	legacyDir := filepath.Join(dataDir, tenant, "metrics-raw")
	if err := os.MkdirAll(legacyDir, 0o750); err != nil {
		t.Fatal(err)
	}

	ingestNs := int64(1700000123456789000)
	legacyName := "metrics-raw-1700000123456789000-window.parquet"
	legacyPath := filepath.Join(legacyDir, legacyName)
	testparquet.WriteFile(t, legacyPath, []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})

	start := time.Unix(1700000000, 0).UTC()
	e := New(Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = e.Close() })

	if err := e.importLegacyMetricsRaw(tenant); err != nil {
		t.Fatalf("first import: %v", err)
	}
	l0, err := ListL0(dataDir, tenant)
	if err != nil {
		t.Fatalf("list L0: %v", err)
	}
	if len(l0) != 1 {
		t.Fatalf("want 1 L0 segment after legacy import, got %d", len(l0))
	}

	te, err := e.open(tenant)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var ts time.Time
	if err := te.db.QueryRowContext(context.Background(), "SELECT ts FROM read_parquet(?)", l0[0]).Scan(&ts); err != nil {
		t.Fatalf("read ts: %v", err)
	}
	wantTs := time.Unix(0, ingestNs).UTC()
	if !ts.Equal(wantTs) {
		t.Fatalf("want ts from filename %v, got %v", wantTs, ts)
	}

	if err := e.importLegacyMetricsRaw(tenant); err != nil {
		t.Fatalf("second import: %v", err)
	}
	l0Again, _ := ListL0(dataDir, tenant)
	if len(l0Again) != 1 {
		t.Fatalf("legacy import not idempotent: got %d L0 segments", len(l0Again))
	}
	if _, err := os.Stat(legacyPath); err != nil {
		t.Fatalf("legacy file should remain for operator cleanup: %v", err)
	}
}

func TestHotSnapshotNoPartialTmpLeft(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, _ := testEngine(t, start, time.Hour)
	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := e.ExportHotSnapshots(); err != nil {
		t.Fatalf("export: %v", err)
	}
	leftovers, _ := filepath.Glob(filepath.Join(e.cfg.DataDir, testTenant, "hot", "*.tmp"))
	if len(leftovers) != 0 {
		t.Fatalf("partial tmp file(s) should not remain after export: %v", leftovers)
	}
}

func TestConcurrentHotSnapshotSameTenant(t *testing.T) {
	// Concurrent exports for one tenant must not clobber a shared temp file.
	start := time.Unix(1700000000, 0).UTC()
	e, _ := testEngine(t, start, time.Hour)
	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
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
			t.Fatalf("concurrent export: %v", err)
		}
	}
	final := filepath.Join(e.cfg.DataDir, testTenant, "hot", "current.parquet")
	if _, err := os.Stat(final); err != nil {
		t.Fatalf("final snapshot missing: %v", err)
	}
	leftovers, _ := filepath.Glob(filepath.Join(e.cfg.DataDir, testTenant, "hot", "*.tmp"))
	if len(leftovers) != 0 {
		t.Fatalf("temp files should not remain: %v", leftovers)
	}
}

func TestHotSnapshotDoesNotBlockFlushDue(t *testing.T) {
	defer goleak.VerifyNone(t)

	start := time.Unix(1700000000, 0).UTC()
	clk := start
	e := New(Config{DataDir: t.TempDir(), HotWindow: 10 * time.Minute}, func() time.Time { return clk })
	defer func() { _ = e.Close() }()

	path := testparquet.WriteWindow(t, t.TempDir(), "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	clk = start.Add(10 * time.Minute)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unlock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		exportBarrier = nil
		unlock()
	})
	exportBarrier = func() {
		close(entered)
		<-release
	}

	exportDone := make(chan error, 1)
	go func() { exportDone <- e.ExportHotSnapshot(testTenant) }()

	select {
	case <-entered:
	case err := <-exportDone:
		t.Fatalf("export returned before barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("export did not reach barrier")
	}

	flushDone := make(chan error, 1)
	go func() { flushDone <- e.FlushDue() }()

	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("FlushDue: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FlushDue blocked behind hot snapshot")
	}

	segments, err := ListL0(e.cfg.DataDir, testTenant)
	if err != nil {
		t.Fatalf("list L0: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("want 1 L0 segment before releasing snapshot barrier, got %d", len(segments))
	}

	unlock()
	select {
	case err := <-exportDone:
		if err != nil {
			t.Fatalf("export: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("export did not finish after barrier release")
	}
}

func TestOverlappingSnapshotsStillSingleExport(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, _ := testEngine(t, start, time.Hour)
	ingestOneWindow(t, e, testTenant)

	entered := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	var releaseOnce sync.Once
	unlock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		exportBarrier = nil
		unlock()
	})
	exportBarrier = func() {
		if runs.Add(1) == 1 {
			close(entered)
		}
		<-release
	}

	const overlap = 8
	begin := make(chan struct{})
	errs := make(chan error, overlap)
	var wg sync.WaitGroup
	wg.Add(overlap)
	for i := 0; i < overlap; i++ {
		go func() {
			defer wg.Done()
			<-begin
			errs <- e.ExportHotSnapshot(testTenant)
		}()
	}
	close(begin)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not reach barrier")
	}

	unlock()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("export: %v", err)
		}
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("want 1 export, got %d", got)
	}
	final := filepath.Join(e.cfg.DataDir, testTenant, "hot", "current.parquet")
	if _, err := os.Stat(final); err != nil {
		t.Fatalf("final snapshot missing: %v", err)
	}
}
