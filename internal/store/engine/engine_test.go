package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/testparquet"
)

const testTenant = "user-6f3a9c2b-apps"

func testEngine(t *testing.T, start time.Time, hotWindow time.Duration) (*Engine, *time.Time) {
	t.Helper()
	clk := start
	e := New(Config{DataDir: t.TempDir(), HotWindow: hotWindow}, func() time.Time { return clk })
	t.Cleanup(func() { _ = e.Close() })
	return e, &clk
}

func TestIngestEmptyWindowNoOp(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, _ := testEngine(t, start, time.Minute)

	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	n, err := e.Ingest(testTenant, strings.NewReader(""))
	if err != nil {
		t.Fatalf("ingest empty: %v", err)
	}
	_ = f
	if n != 0 {
		t.Fatalf("want 0 rows, got %d", n)
	}
	if c, _ := e.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot table should be empty, got %d rows", c)
	}
}

func TestIngestStampsTsEvenWhenTimestampMsZero(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, now := testEngine(t, start, time.Hour)
	*now = start

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})

	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	tsList, err := e.QueryHotTs(testTenant)
	if err != nil {
		t.Fatalf("query ts: %v", err)
	}
	if len(tsList) != 1 {
		t.Fatalf("want 1 row, got %d", len(tsList))
	}
	if !tsList[0].Equal(start) {
		t.Fatalf("want ingest ts %v, got %v", start, tsList[0])
	}
}

func TestFlushAfterHotWindowCreatesOneL0SegmentSortedByTs(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, now := testEngine(t, start, 10*time.Minute)
	dir := t.TempDir()

	for i := 0; i < 3; i++ {
		*now = start.Add(time.Duration(i) * time.Minute)
		path := testparquet.WriteWindow(t, dir, "w"+string(rune('a'+i))+".parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: float64(i), TimestampMs: 0},
		})
		if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}

	*now = start.Add(10 * time.Minute)
	if err := e.FlushDue(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	segments, err := ListL0(e.cfg.DataDir, testTenant)
	if err != nil {
		t.Fatalf("list L0: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("want exactly 1 L0 segment, got %d", len(segments))
	}

	if c, _ := e.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot_current should be empty after flush, got %d", c)
	}

	te, _ := e.open(testTenant)
	var cnt int
	if err := te.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM read_parquet(?)", segments[0]).Scan(&cnt); err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if cnt != 3 {
		t.Fatalf("want 3 rows in L0, got %d", cnt)
	}
	rows, err := te.db.QueryContext(context.Background(), "SELECT ts FROM read_parquet(?) ORDER BY ts", segments[0])
	if err != nil {
		t.Fatalf("query segment ts: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var prev time.Time
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !prev.IsZero() && ts.Before(prev) {
			t.Fatalf("rows not sorted by ts: %v before %v", ts, prev)
		}
		prev = ts
	}
}

func TestMultipleWindowsThenSingleFlush(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, now := testEngine(t, start, 10*time.Minute)
	dir := t.TempDir()

	for i := 0; i < 5; i++ {
		path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
			{Name: "metric", Labels: "{}", Value: 1, TimestampMs: 0},
		})
		if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	*now = start.Add(10 * time.Minute)
	if err := e.FlushDue(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	segs, _ := ListL0(e.cfg.DataDir, testTenant)
	if len(segs) != 1 {
		t.Fatalf("want 1 L0 after one window interval, got %d", len(segs))
	}
}

func TestTenantIsolationPaths(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	e, _ := testEngine(t, start, time.Hour)
	e.cfg.DataDir = dataDir

	a := "user-aaaaaaaa-apps"
	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := e.Ingest(a, readFile(t, path)); err != nil {
		t.Fatalf("ingest a: %v", err)
	}

	dbPathA := filepath.Join(dataDir, a, "engine.duckdb")
	dbPathB := filepath.Join(dataDir, "user-bbbbbbbb-apps", "engine.duckdb")
	if _, err := os.Stat(dbPathA); err != nil {
		t.Fatalf("tenant A db missing: %v", err)
	}
	if _, err := os.Stat(dbPathB); !os.IsNotExist(err) {
		t.Fatalf("tenant B db should not exist yet")
	}
}

func TestLRUEvictsOldestTenant(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	dataDir := t.TempDir()
	e := New(Config{DataDir: dataDir, HotWindow: time.Hour, MaxOpenTenants: 2}, func() time.Time { return start })
	t.Cleanup(func() { _ = e.Close() })

	dir := t.TempDir()
	tenants := []string{"user-aaaaaaa1-apps", "user-aaaaaaa2-apps", "user-aaaaaaa3-apps"}
	for _, tenant := range tenants {
		path := testparquet.WriteWindow(t, dir, tenant+".parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
		})
		if _, err := e.Ingest(tenant, readFile(t, path)); err != nil {
			t.Fatalf("ingest %s: %v", tenant, err)
		}
	}

	if e.lru.order.Len() != 2 {
		t.Fatalf("want 2 open tenants, got %d", e.lru.order.Len())
	}

	path := testparquet.WriteWindow(t, dir, "reopen.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 2, TimestampMs: 0},
	})
	if _, err := e.Ingest(tenants[0], readFile(t, path)); err != nil {
		t.Fatalf("re-ingest evicted tenant: %v", err)
	}
	if c, _ := e.HotRowCount(tenants[0]); c != 2 {
		t.Fatalf("want 2 rows after reopen, got %d", c)
	}
}

func TestCloseEvictsAllTenants(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	e, _ := testEngine(t, start, time.Hour)
	dir := t.TempDir()

	for _, tenant := range []string{"user-bbbbbbb1-apps", "user-bbbbbbb2-apps"} {
		path := testparquet.WriteWindow(t, dir, tenant+".parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
		})
		if _, err := e.Ingest(tenant, readFile(t, path)); err != nil {
			t.Fatalf("ingest %s: %v", tenant, err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if e.lru.order.Len() != 0 {
		t.Fatalf("want 0 open tenants after close, got %d", e.lru.order.Len())
	}

	path := testparquet.WriteWindow(t, dir, "after-close.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	if _, err := e.Ingest("user-bbbbbbb1-apps", readFile(t, path)); err != nil {
		t.Fatalf("ingest after close: %v", err)
	}
}

func readFile(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

var _ *sql.DB
