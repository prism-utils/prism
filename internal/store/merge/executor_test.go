package merge

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestExecutorMergeCreatesOrderedSegmentAndDeletesInputs(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 3; i++ {
		path := filepath.Join(l0, pathID(i)+".parquet")
		ts := base.Add(time.Duration(i) * time.Minute)
		testparquet.WriteSegmentWithTs(t, path, ts, "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer func() { _ = x.Close() }()

	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, now)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if out.Tier != 1 {
		t.Fatalf("want tier 1, got %d", out.Tier)
	}
	for _, s := range sources {
		if _, err := os.Stat(s.Path); !os.IsNotExist(err) {
			t.Fatalf("input %s should be deleted", s.Path)
		}
	}
	if _, err := os.Stat(out.Path); err != nil {
		t.Fatalf("output missing: %v", err)
	}
}

func TestNewExecutor_appliesDuckDBCaps(t *testing.T) {
	t.Parallel()
	x, err := NewExecutor(ExecutorConfig{
		DataDir:     t.TempDir(),
		Tenant:      "t",
		Threads:     2,
		MemoryLimit: "128MB",
	})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer func() { _ = x.Close() }()

	var threads, memLimit string
	if err := x.db.QueryRowContext(context.Background(), "SELECT current_setting('threads')").Scan(&threads); err != nil {
		t.Fatalf("threads: %v", err)
	}
	if err := x.db.QueryRowContext(context.Background(), "SELECT current_setting('memory_limit')").Scan(&memLimit); err != nil {
		t.Fatalf("memory_limit: %v", err)
	}
	if threads != "2" {
		t.Fatalf("threads = %q, want 2", threads)
	}
	if memLimit == "" {
		t.Fatal("memory_limit empty, want configured cap")
	}
}

func TestStatSegment_appliesDuckDBCaps(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(l0, "a.parquet")
	testparquet.WriteSegmentWithTs(t, path, base, "up", 1)

	x, err := NewExecutor(ExecutorConfig{
		DataDir:     dataDir,
		Tenant:      tenant,
		Threads:     3,
		MemoryLimit: "256MB",
	})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer func() { _ = x.Close() }()

	seg, err := StatSegment(path, 0, DuckDBCaps{Threads: 3, MemoryLimit: "256MB"})
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if seg.Tier != 0 {
		t.Fatalf("tier = %d, want 0", seg.Tier)
	}

	var threads string
	if err := x.db.QueryRowContext(context.Background(), "SELECT current_setting('threads')").Scan(&threads); err != nil {
		t.Fatalf("executor threads: %v", err)
	}
	if n, _ := strconv.Atoi(threads); n != 3 {
		t.Fatalf("executor threads = %q, want 3", threads)
	}
	_ = seg
}

func TestExecuteMergeMissingSourceReturnsError(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	path := filepath.Join(l0, "a.parquet")
	testparquet.WriteSegmentWithTs(t, path, base, "up", 1)
	present, err := StatSegment(path, 0, DuckDBCaps{})
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	missing := Segment{Tier: 0, Path: filepath.Join(l0, "gone.parquet"), Bytes: 10, MinTs: base, MaxTs: base}

	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer func() { _ = x.Close() }()

	_, err = x.ExecuteMerge(MergeAction{Sources: []Segment{present, missing}, DestTier: 1}, base)
	if err == nil {
		t.Fatal("want error when a merge source file is absent before COPY")
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("present source must remain when merge fails early: %v", statErr)
	}
	l1Dir := filepath.Join(dataDir, tenant, "tiers", "L1")
	if entries, _ := os.ReadDir(l1Dir); len(entries) != 0 {
		t.Fatalf("no L1 output should land when COPY fails, got %d files", len(entries))
	}
}
