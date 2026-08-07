package merge

import (
	"testing"
	"time"
)

func TestFindLogTierMergePacksSparseTimestamps(t *testing.T) {
	// L0 files from merge ticks are minutes apart; Lucene adjacency would
	// refuse to pack them. Logs tiers must pack time-ordered like landing.
	const maxBytes int64 = 500
	const perFile int64 = 20
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  100,
		MaxSegmentBytes: maxBytes,
		FloorBytes:      10,
	})
	var tiers []Segment
	for i := 0; i < 40; i++ {
		off := time.Duration(i) * 6 * time.Minute // typical merge-tick spacing
		tiers = append(tiers, seg(0, pathID(i%26)+pathID((i/26)%26), perFile, off, off))
	}
	actions := p.FindLogMerges(nil, tiers)
	if len(actions) != 1 {
		t.Fatalf("want 1 L0 pack-to-max merge, got %d", len(actions))
	}
	wantN := int(maxBytes / perFile)
	if len(actions[0].Sources) != wantN {
		t.Fatalf("want %d sources packing to max, got %d", wantN, len(actions[0].Sources))
	}
	if actions[0].DestTier != 1 {
		t.Fatalf("want DestTier 1, got %d", actions[0].DestTier)
	}
}

func TestFindLogMergesReturnsLandingAndTier(t *testing.T) {
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  100,
		MaxSegmentBytes: 1000,
		FloorBytes:      10,
	})
	var landing []Segment
	for i := 0; i < 8; i++ {
		off := time.Duration(i) * time.Minute
		landing = append(landing, seg(-1, "l"+pathID(i), 10, off, off+time.Second))
	}
	var tiers []Segment
	for i := 0; i < 8; i++ {
		off := time.Duration(i) * 6 * time.Minute
		tiers = append(tiers, seg(0, "t"+pathID(i), 10, off, off))
	}
	actions := p.FindLogMerges(landing, tiers)
	if len(actions) != 2 {
		t.Fatalf("want landing + L0 actions, got %d", len(actions))
	}
	if actions[0].DestTier != 0 {
		t.Fatalf("first action should be landing→L0, DestTier=%d", actions[0].DestTier)
	}
	if actions[1].DestTier != 1 {
		t.Fatalf("second action should be L0→L1, DestTier=%d", actions[1].DestTier)
	}
}
