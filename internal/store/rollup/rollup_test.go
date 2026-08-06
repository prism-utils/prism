package rollup

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/testparquet"
)

func TestRollupAggregatesMatchDirectAggregation(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var paths []string
	values := []float64{1, 2, 3, 4, 5}
	for i, v := range values {
		path := filepath.Join(l0, string(rune('a'+i))+".parquet")
		testparquet.WriteSegmentWithTs(t, path, base.Add(time.Duration(i)*time.Minute), "metric", v)
		paths = append(paths, path)
	}

	b, err := NewBuilder(dataDir, tenant, []Step{{Name: "1m", Interval: "1 minute"}}, BuilderConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	now := base.Add(time.Hour)
	if err := b.BuildFromMerge(paths, now); err != nil {
		t.Fatalf("build rollup: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(dataDir, tenant, "rollups", "1m"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 rollup file, got %d", len(entries))
	}
	rollupPath := filepath.Join(dataDir, tenant, "rollups", "1m", entries[0].Name())

	rawAgg, err := AggregateRaw(b.db, paths, "1 minute")
	if err != nil {
		t.Fatalf("raw agg: %v", err)
	}
	rollupAgg, err := ReadRollup(b.db, rollupPath)
	if err != nil {
		t.Fatalf("read rollup: %v", err)
	}
	if len(rawAgg) != len(rollupAgg) {
		t.Fatalf("bucket count mismatch: raw=%d rollup=%d", len(rawAgg), len(rollupAgg))
	}
	for k, want := range rawAgg {
		got, ok := rollupAgg[k]
		if !ok {
			t.Fatalf("missing rollup bucket %s", k)
		}
		if !closeF(got.Avg, want.Avg) || !closeF(got.Min, want.Min) || !closeF(got.Max, want.Max) ||
			got.Count != want.Count || !closeF(got.Sum, want.Sum) {
			t.Fatalf("rollup mismatch for %s: got %+v want %+v", k, got, want)
		}
	}
}

func closeF(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func TestStatRollupMaxBucketEmptyReturnsZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.parquet")
	testparquet.WriteEmptyRollup(t, path)

	maxBucket, err := StatRollupMaxBucket(path)
	if err != nil {
		t.Fatalf("empty rollup must not Scan-error: %v", err)
	}
	if !maxBucket.IsZero() {
		t.Fatalf("empty rollup max bucket = %v, want zero time", maxBucket)
	}
}

func TestNewBuilder_appliesDuckDBCaps(t *testing.T) {
	t.Parallel()
	b, err := NewBuilder(t.TempDir(), "tenant", []Step{{Name: "1m", Interval: "1 minute"}}, BuilderConfig{
		Threads:     2,
		MemoryLimit: "128MB",
	})
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	defer func() { _ = b.Close() }()

	var threads, memLimit string
	if err := b.db.QueryRowContext(context.Background(), "SELECT current_setting('threads')").Scan(&threads); err != nil {
		t.Fatalf("threads: %v", err)
	}
	if err := b.db.QueryRowContext(context.Background(), "SELECT current_setting('memory_limit')").Scan(&memLimit); err != nil {
		t.Fatalf("memory_limit: %v", err)
	}
	if threads != "2" {
		t.Fatalf("threads = %q, want 2", threads)
	}
	if memLimit == "" {
		t.Fatal("memory_limit empty, want configured cap")
	}
}
