package lifecycle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/merge"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestTickMergeCatchupWhenLuceneEmpty(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	aged := now.Add(-20 * time.Minute)
	// Hours apart so Lucene time-adjacency cannot form a pack even if the
	// count trigger were met; SegmentsPerTier stays at 6 so two files never
	// trigger Lucene.
	l0a := filepath.Join(layout.TierDir(dataDir, lifecycleTenant, 0), "a.parquet")
	l0b := filepath.Join(layout.TierDir(dataDir, lifecycleTenant, 0), "b.parquet")
	testparquet.WriteSegmentWithTs(t, l0a, aged, "up", 1)
	testparquet.WriteSegmentWithTs(t, l0b, aged.Add(-2*time.Hour), "up", 2)

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:           dataDir,
		SegmentsPerTier:   6,
		MaxSegmentBytes:   1 << 30,
		MaxTier:           8,
		CompactAgeCatchup: true,
	}, eng, func() time.Time { return now })

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}
	l1Dir := layout.TierDir(dataDir, lifecycleTenant, 1)
	entries, err := os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("list L1: %v", err)
	}
	var l1 int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".parquet") && !strings.HasSuffix(e.Name(), ".tmp") {
			l1++
		}
	}
	if l1 < 1 {
		t.Fatal("catch-up must write at least one L1 parquet when Lucene is empty")
	}
}

func TestTickMergeCatchupDisabledSkipsAgedPair(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	aged := now.Add(-20 * time.Minute)
	l0a := filepath.Join(layout.TierDir(dataDir, lifecycleTenant, 0), "a.parquet")
	l0b := filepath.Join(layout.TierDir(dataDir, lifecycleTenant, 0), "b.parquet")
	testparquet.WriteSegmentWithTs(t, l0a, aged, "up", 1)
	testparquet.WriteSegmentWithTs(t, l0b, aged.Add(-2*time.Hour), "up", 2)

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:           dataDir,
		SegmentsPerTier:   6,
		MaxSegmentBytes:   1 << 30,
		MaxTier:           8,
		CompactAgeCatchup: false,
	}, eng, func() time.Time { return now })

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}
	if _, err := os.Stat(layout.TierDir(dataDir, lifecycleTenant, 1)); !os.IsNotExist(err) {
		t.Fatal("catch-up off must not create L1")
	}
}

func TestEnqueueCompactBeatsLucene(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	base := now.Add(-time.Hour)
	var paths []string
	for i := 0; i < 6; i++ {
		p := filepath.Join(layout.TierDir(dataDir, lifecycleTenant, 0), string(rune('a'+i))+".parquet")
		testparquet.WriteSegmentWithTs(t, p, base.Add(time.Duration(i)*time.Minute), "up", float64(i))
		paths = append(paths, p)
	}

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:           dataDir,
		SegmentsPerTier:   6,
		MaxSegmentBytes:   1 << 30,
		FloorBytes:        1,
		MaxTier:           8,
		CompactAgeCatchup: true,
		DeleteGrace:       time.Hour,
	}, eng, func() time.Time { return now })

	stA, err := os.Stat(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	stB, err := os.Stat(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	runner.EnqueueCompact(lifecycleTenant, merge.MergeAction{
		Sources: []merge.Segment{
			{Tier: 0, Path: paths[0], Bytes: stA.Size(), MinTs: base, MaxTs: base},
			{Tier: 0, Path: paths[1], Bytes: stB.Size(), MinTs: base.Add(time.Minute), MaxTs: base.Add(time.Minute)},
		},
		DestTier: 1,
	})

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}

	l1Dir := layout.TierDir(dataDir, lifecycleTenant, 1)
	l1, err := os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("L1: %v", err)
	}
	n1 := 0
	for _, e := range l1 {
		if strings.HasSuffix(e.Name(), ".parquet") {
			n1++
		}
	}
	if n1 != 1 {
		t.Fatalf("enqueue must produce exactly one L1, got %d", n1)
	}
	live := 0
	l0Dir := layout.TierDir(dataDir, lifecycleTenant, 0)
	entries, _ := os.ReadDir(l0Dir)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		if _, err := os.Stat(filepath.Join(l0Dir, e.Name()+".compacted")); err == nil {
			continue
		}
		live++
	}
	if live != 4 {
		t.Fatalf("want 4 live L0 left after enqueue of 2, got %d", live)
	}
}

func TestNamedPolicyRunsWhenCatchupOff(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	aged := now.Add(-20 * time.Minute)
	l0a := filepath.Join(layout.TierDir(dataDir, lifecycleTenant, 0), "a.parquet")
	l0b := filepath.Join(layout.TierDir(dataDir, lifecycleTenant, 0), "b.parquet")
	testparquet.WriteSegmentWithTs(t, l0a, aged, "up", 1)
	testparquet.WriteSegmentWithTs(t, l0b, aged.Add(-time.Minute), "up", 2)

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:           dataDir,
		SegmentsPerTier:   6,
		MaxSegmentBytes:   1 << 30,
		MaxTier:           8,
		CompactAgeCatchup: false,
		CompactFile: merge.File{Policies: []merge.Policy{{
			Name:       "recent",
			Tier:       0,
			OlderThan:  "15m",
			MaxSources: 32,
			MaxBytes:   "256Mi",
			Every:      "45m",
			Bucket:     "none",
		}}},
	}, eng, func() time.Time { return now })

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}
	l1Dir := layout.TierDir(dataDir, lifecycleTenant, 1)
	entries, err := os.ReadDir(l1Dir)
	if err != nil {
		t.Fatalf("list L1: %v", err)
	}
	if len(entries) < 1 {
		t.Fatal("due named policy must compact when Lucene and catch-up are idle")
	}
}
