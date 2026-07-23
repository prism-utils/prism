package engine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/testparquet"
)

func TestFlushEmptyHotPrevNoL0FileScheduleCleared(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, now := testEngine(t, start, time.Minute)
	dir := t.TempDir()

	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	te, err := e.open(testTenant)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := te.db.ExecContext(context.Background(), "DELETE FROM hot_current"); err != nil {
		t.Fatalf("clear hot_current: %v", err)
	}

	*now = start.Add(time.Minute)
	if err := e.FlushDue(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	segs, err := ListL0(e.cfg.DataDir, testTenant)
	if err != nil {
		t.Fatalf("list L0: %v", err)
	}
	if len(segs) != 0 {
		t.Fatalf("want no L0 segment for empty hot_prev, got %d", len(segs))
	}
	if c, _ := e.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot_current should be empty after flush, got %d rows", c)
	}

	e.mu.Lock()
	_, scheduled := e.flushAt[testTenant]
	e.mu.Unlock()
	if scheduled {
		t.Fatal("flush schedule should be cleared")
	}
}

func TestFirstFlushAbsentHotCurrentNoErrorNoFile(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	dataDir := t.TempDir()
	e := New(Config{DataDir: dataDir, HotWindow: time.Minute}, func() time.Time { return start })
	t.Cleanup(func() { _ = e.Close() })

	if err := os.MkdirAll(filepath.Join(dataDir, testTenant), 0o750); err != nil {
		t.Fatal(err)
	}

	if err := e.FlushDue(); err != nil {
		t.Fatalf("flush due with no schedule: %v", err)
	}

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	te, err := e.open(testTenant)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := te.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS hot_current"); err != nil {
		t.Fatalf("drop hot_current: %v", err)
	}
	if _, err := te.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS hot_prev"); err != nil {
		t.Fatalf("drop hot_prev: %v", err)
	}

	if err := e.flushTenant(testTenant); err != nil {
		t.Fatalf("first flush absent hot_current: %v", err)
	}

	segs, err := ListL0(dataDir, testTenant)
	if err != nil {
		t.Fatalf("list L0: %v", err)
	}
	if len(segs) != 0 {
		t.Fatalf("want no L0 file, got %d segments", len(segs))
	}
	if c, _ := e.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot_current should exist empty, got %d rows", c)
	}
}

func TestLegacyImportAbsentMetricsRawDirWritesMarker(t *testing.T) {
	dataDir := t.TempDir()
	tenant := testTenant
	if err := os.MkdirAll(filepath.Join(dataDir, tenant), 0o750); err != nil {
		t.Fatal(err)
	}

	start := time.Unix(1700000000, 0).UTC()
	e := New(Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = e.Close() })

	if err := e.importLegacyMetricsRaw(tenant); err != nil {
		t.Fatalf("import without metrics-raw dir: %v", err)
	}
	marker := filepath.Join(dataDir, tenant, legacyImportMarker)
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker missing: %v", err)
	}
	l0, err := ListL0(dataDir, tenant)
	if err != nil {
		t.Fatalf("list L0: %v", err)
	}
	if len(l0) != 0 {
		t.Fatalf("want no L0 segments, got %d", len(l0))
	}

	if err := e.importLegacyMetricsRaw(tenant); err != nil {
		t.Fatalf("second import should be no-op: %v", err)
	}
	if _, err := e.open(tenant); err != nil {
		t.Fatalf("open after marker: %v", err)
	}
}

func TestConcurrentIngestAndFlushRace(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	dataDir := t.TempDir()
	var mu sync.Mutex
	clk := start
	e := New(Config{DataDir: dataDir, HotWindow: 50 * time.Millisecond}, func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clk
	})
	t.Cleanup(func() { _ = e.Close() })

	dir := t.TempDir()
	const workers = 8
	const ingestsPerWorker = 5
	paths := make([]string, workers*ingestsPerWorker)
	for i := range paths {
		paths[i] = testparquet.WriteWindow(t, dir, "w"+string(rune('a'+i%26))+".parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: float64(i), TimestampMs: int64(i)},
		})
	}

	var errCount atomic.Int32
	var wg sync.WaitGroup
	stopAdvance := make(chan struct{})

	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopAdvance:
				return
			case <-tick.C:
				mu.Lock()
				clk = clk.Add(15 * time.Millisecond)
				mu.Unlock()
			}
		}
	}()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < ingestsPerWorker; i++ {
				f, err := os.Open(paths[base*ingestsPerWorker+i])
				if err != nil {
					errCount.Add(1)
					return
				}
				if _, err := e.Ingest(testTenant, f); err != nil {
					errCount.Add(1)
					_ = f.Close()
					return
				}
				_ = f.Close()
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := e.FlushDue(); err != nil {
				errCount.Add(1)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(stopAdvance)

	mu.Lock()
	clk = start.Add(time.Hour)
	mu.Unlock()
	if err := e.FlushDue(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	if n := errCount.Load(); n != 0 {
		t.Fatalf("concurrent ingest/flush errors: %d", n)
	}

	wantRows := int64(workers * ingestsPerWorker)
	hot, err := e.HotRowCount(testTenant)
	if err != nil {
		t.Fatalf("hot row count: %v", err)
	}
	segs, err := ListL0(dataDir, testTenant)
	if err != nil {
		t.Fatalf("list L0: %v", err)
	}

	var l0Rows int64
	te, err := e.open(testTenant)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, seg := range segs {
		var cnt int64
		if err := te.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM read_parquet(?)", seg).Scan(&cnt); err != nil {
			t.Fatalf("count L0 segment: %v", err)
		}
		l0Rows += cnt
	}
	if hot+l0Rows != wantRows {
		t.Fatalf("row accounting: hot=%d l0=%d want %d", hot, l0Rows, wantRows)
	}

	if _, err := e.Ingest(testTenant, readFile(t, paths[0])); err != nil {
		t.Fatalf("post-race ingest failed (possible closed db): %v", err)
	}
}
