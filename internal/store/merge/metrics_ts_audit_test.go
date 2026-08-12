package merge

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/testparquet"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// TestExecuteMergePreservesPerRowTimestamps documents the metrics audit: merge
// keeps each sample's ts / timestamp_ms (ORDER BY ts SELECT *), and must not
// stamp from the segment filename or wall-clock.
func TestExecuteMergePreservesPerRowTimestamps(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-metricts01-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	wantTS := map[string]time.Time{}
	for i := 0; i < 3; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		// Filename window deliberately differs from per-row ts.
		path := filepath.Join(l0, layout.SegmentName(base.Add(time.Hour)))
		if i > 0 {
			path = filepath.Join(l0, layout.SegmentName(base.Add(time.Hour+time.Duration(i)*time.Second)))
		}
		name := "up"
		testparquet.WriteSegmentWithTs(t, path, ts, name, float64(i))
		wantTS[path] = ts
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	mergeNow := base.Add(3 * time.Hour)
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, mergeNow)
	if err != nil {
		t.Fatalf("ExecuteMerge: %v", err)
	}

	got := readMetricTimestamps(t, out.Path)
	if len(got) != 3 {
		t.Fatalf("row count = %d, want 3", len(got))
	}
	for _, ts := range got {
		found := false
		for _, want := range wantTS {
			if ts.Equal(want) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("merged ts %v not in preserved source timestamps %v", ts, wantTS)
		}
		if ts.Equal(mergeNow) {
			t.Fatal("merged row stamped with merge wall-clock")
		}
	}
}

func readMetricTimestamps(t *testing.T, path string) []time.Time {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(context.Background(),
		"SELECT ts FROM read_parquet('"+layout.ToSlash(path)+"') ORDER BY ts")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []time.Time
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			t.Fatal(err)
		}
		out = append(out, ts.UTC())
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
