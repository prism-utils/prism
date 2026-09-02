package merge

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
)

func TestFindLogMergesSplitsSameTypePacks(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-sametype01-apps"
	artifact := "logs-raw"
	landing := filepath.Join(dataDir, tenant, "logs", artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var segs []Segment
	for i := 0; i < 3; i++ {
		p := filepath.Join(landing, layout.SegmentName(base.Add(time.Duration(i)*time.Second)))
		writeTinyLogParquet(t, p, "p")
		s, err := StatLogSegment(p, -1)
		if err != nil {
			t.Fatal(err)
		}
		segs = append(segs, s)
	}
	for i := 0; i < 3; i++ {
		p := filepath.Join(landing, layout.SegmentNameFormat(base.Add(time.Duration(i+10)*time.Second), "duckdb"))
		writeLandingLogsDuckDB(t, p, "d")
		s, err := StatLogSegment(p, -1)
		if err != nil {
			t.Fatal(err)
		}
		segs = append(segs, s)
	}
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier:       3,
		MaxMergeAtOnce:        8,
		MaxSegmentBytes:       1 << 30,
		FloorBytes:            1,
		LogsRefreshInterval:   time.Minute,
		LogsRefreshMaxActions: 8,
	})
	actions := p.FindLogMerges(base.Add(time.Hour), segs, nil)
	if len(actions) < 2 {
		t.Fatalf("want separate parquet and duckdb packs, got %d", len(actions))
	}
	sawParquet, sawDuck := false, false
	for _, a := range actions {
		ext := filepath.Ext(a.Sources[0].Path)
		for _, s := range a.Sources {
			if filepath.Ext(s.Path) != ext {
				t.Fatalf("mixed pack: %s with %s", a.Sources[0].Path, s.Path)
			}
		}
		switch ext {
		case ".parquet":
			sawParquet = true
		case ".duckdb":
			sawDuck = true
		}
	}
	if !sawParquet || !sawDuck {
		t.Fatalf("want one pack per payload, parquet=%v duckdb=%v", sawParquet, sawDuck)
	}
}

func TestFindLogMergesAgedL0ForcePack(t *testing.T) {
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  8,
		MaxSegmentBytes: 1000,
		FloorBytes:      10,
		ColdAfter:       12 * time.Hour,
	})
	now := fixtureBase
	var tiers []Segment
	for i := 0; i < 2; i++ {
		off := -24*time.Hour + time.Duration(i)*time.Minute
		tiers = append(tiers, seg(0, "old"+pathID(i)+".parquet", 10, off, off))
	}
	actions := p.FindLogMerges(now, nil, tiers)
	if len(actions) != 1 {
		t.Fatalf("aged L0 below SegmentsPerTier must still pack, got %d", len(actions))
	}
	if actions[0].DestTier != 1 {
		t.Fatalf("want L0→L1, DestTier=%d", actions[0].DestTier)
	}

	young := []Segment{seg(0, "y0.parquet", 10, -time.Hour, -time.Hour), seg(0, "y1.parquet", 10, -time.Minute, -time.Minute)}
	if got := p.FindLogMerges(now, nil, young); len(got) != 0 {
		t.Fatalf("young L0 below count trigger must not pack, got %d", len(got))
	}
}

func TestRepairLogSegmentExtensions(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-repair01-apps"
	artifact := "logs-raw"
	dir := filepath.Join(dataDir, tenant, "logs", artifact, "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	wrong := filepath.Join(dir, layout.SegmentNameFormat(time.Unix(1, 0), "parquet"))
	writeLandingLogsDuckDB(t, wrong, "line")
	skip := layout.MergeSkipMarker(wrong)
	if err := os.WriteFile(skip, []byte("attempts=5\nreason=too-large\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	n, err := RepairLogSegmentExtensions(dataDir, tenant)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if n < 1 {
		t.Fatal("repair must rename mismatched extension")
	}
	if _, err := os.Stat(wrong); !os.IsNotExist(err) {
		t.Fatal("old parquet name must be gone")
	}
	want := wrong[:len(wrong)-len(".parquet")] + ".duckdb"
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("renamed duckdb missing: %v", err)
	}
	if _, err := os.Stat(skip); !os.IsNotExist(err) {
		t.Fatal("too-large skip sidecar must be cleared after rename")
	}
}
