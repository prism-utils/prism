package merge

import (
	"testing"
	"time"
)

func seg(tier int, id string, bytes int64, minOff, maxOff time.Duration) Segment {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return Segment{
		Tier:  tier,
		Path:  id,
		Bytes: bytes,
		MinTs: base.Add(minOff),
		MaxTs: base.Add(maxOff),
	}
}

func testPlanner() *Planner {
	return NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  6,
		MaxSegmentBytes: 200,
		FloorBytes:      10,
	})
}

func TestFindMergesSixSameTierProducesOneMerge(t *testing.T) {
	p := testPlanner()
	var segs []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i*10) * time.Minute
		segs = append(segs, seg(0, pathID(i), 20, off, off+9*time.Minute))
	}
	actions := p.FindMerges(segs)
	if len(actions) != 1 {
		t.Fatalf("want 1 merge, got %d", len(actions))
	}
	if actions[0].DestTier != 1 {
		t.Fatalf("want dest tier 1, got %d", actions[0].DestTier)
	}
	if len(actions[0].Sources) != 6 {
		t.Fatalf("want 6 sources, got %d", len(actions[0].Sources))
	}
}

func TestFindMergesEmptyNoAction(t *testing.T) {
	if actions := testPlanner().FindMerges(nil); len(actions) != 0 {
		t.Fatalf("empty input should produce no merge, got %v", actions)
	}
}

func TestFindMergesAllSealedTierNoAction(t *testing.T) {
	p := testPlanner()
	var segs []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i*10) * time.Minute
		segs = append(segs, seg(0, pathID(i), 200, off, off+9*time.Minute))
	}
	if actions := p.FindMerges(segs); len(actions) != 0 {
		t.Fatalf("all-sealed tier should not merge, got %v", actions)
	}
}

func TestFindMergesOverlappingRangesBreakChain(t *testing.T) {
	p := testPlanner()
	var segs []Segment
	for i := 0; i < 6; i++ {
		segs = append(segs, seg(0, pathID(i), 20, 0, 10*time.Minute))
	}
	if actions := p.FindMerges(segs); len(actions) != 0 {
		t.Fatalf("overlapping ranges must not form a 6-segment chain, got %v", actions)
	}
}

func TestFindMergesShrinksToSingleSource(t *testing.T) {
	p := testPlanner()
	var segs []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i*10) * time.Minute
		segs = append(segs, seg(0, pathID(i), 150, off, off+9*time.Minute))
	}
	actions := p.FindMerges(segs)
	if len(actions) != 1 {
		t.Fatalf("want 1 merge, got %d", len(actions))
	}
	if len(actions[0].Sources) != 1 {
		t.Fatalf("output cap should shrink to 1 source, got %d", len(actions[0].Sources))
	}
	if actions[0].Sources[0].Bytes != 150 {
		t.Fatalf("want lone 150-byte source, got %d", actions[0].Sources[0].Bytes)
	}
}

func TestFindMergesLessThanSixNoMerge(t *testing.T) {
	p := testPlanner()
	var segs []Segment
	for i := 0; i < 5; i++ {
		off := time.Duration(i*10) * time.Minute
		segs = append(segs, seg(0, pathID(i), 20, off, off+9*time.Minute))
	}
	if actions := p.FindMerges(segs); len(actions) != 0 {
		t.Fatalf("want no merge for 5 segments, got %v", actions)
	}
}

func TestFindMergesWouldExceedMaxBytesMergeFewer(t *testing.T) {
	p := testPlanner()
	segs := []Segment{
		seg(0, "a", 80, 0, time.Minute),
		seg(0, "b", 80, time.Minute, 2*time.Minute),
		seg(0, "c", 80, 2*time.Minute, 3*time.Minute),
		seg(0, "d", 80, 3*time.Minute, 4*time.Minute),
		seg(0, "e", 80, 4*time.Minute, 5*time.Minute),
		seg(0, "f", 80, 5*time.Minute, 6*time.Minute),
	}
	actions := p.FindMerges(segs)
	if len(actions) != 1 {
		t.Fatalf("want 1 merge, got %d", len(actions))
	}
	sum := int64(0)
	for _, s := range actions[0].Sources {
		sum += s.Bytes
	}
	if sum > 200 {
		t.Fatalf("merged size %d exceeds max 200", sum)
	}
	if len(actions[0].Sources) >= 6 {
		t.Fatalf("expected fewer than 6 sources when capped, got %d", len(actions[0].Sources))
	}
}

func TestFindMergesSealedSegmentNeverMerged(t *testing.T) {
	p := testPlanner()
	segs := []Segment{
		seg(0, "sealed", 200, 0, time.Minute),
	}
	for i := 0; i < 5; i++ {
		off := time.Duration(i+1) * time.Hour
		segs = append(segs, seg(0, pathID(i), 20, off, off+time.Minute))
	}
	if actions := p.FindMerges(segs); len(actions) != 0 {
		t.Fatalf("sealed + 5 others should not merge, got %v", actions)
	}
}

func TestFindMergesFloorRoundingGroupsSizeLevels(t *testing.T) {
	p := testPlanner()
	var segs []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i) * time.Minute
		segs = append(segs, seg(0, pathID(i), 5, off, off+30*time.Second))
	}
	if actions := p.FindMerges(segs); len(actions) != 1 {
		t.Fatalf("floor-rounded tiny segments should merge, got %d actions", len(actions))
	}
}

func TestFindMergesTimeAdjacencyRespected(t *testing.T) {
	p := testPlanner()
	segs := []Segment{
		seg(0, "a", 20, 0, time.Minute),
		seg(0, "b", 20, time.Minute, 2*time.Minute),
		seg(0, "c", 20, 2*time.Minute, 3*time.Minute),
		seg(0, "d", 20, 5*time.Hour, 5*time.Hour+time.Minute),
		seg(0, "e", 20, 5*time.Hour+time.Minute, 5*time.Hour+2*time.Minute),
		seg(0, "f", 20, 5*time.Hour+2*time.Minute, 5*time.Hour+3*time.Minute),
	}
	if actions := p.FindMerges(segs); len(actions) != 0 {
		t.Fatalf("non-adjacent groups of 3 should not merge, got %v", actions)
	}
}

func TestFindMergesNoCascade(t *testing.T) {
	p := testPlanner()
	var l0, l1 []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i*10) * time.Minute
		l0 = append(l0, seg(0, "l0-"+pathID(i), 20, off, off+9*time.Minute))
		l1Off := time.Duration(i) * time.Hour
		l1 = append(l1, seg(1, "l1-"+pathID(i), 120, l1Off, l1Off+time.Hour))
	}
	actions := p.FindMerges(append(l0, l1...))
	if len(actions) != 1 {
		t.Fatalf("no cascade: want 1 action, got %d", len(actions))
	}
	if actions[0].Sources[0].Tier != 0 {
		t.Fatalf("expected tier-0 merge first, got tier %d", actions[0].Sources[0].Tier)
	}
}

func TestFindMergesDeterministic(t *testing.T) {
	p := testPlanner()
	var segs []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i*10) * time.Minute
		segs = append(segs, seg(0, pathID(i), 20, off, off+9*time.Minute))
	}
	a1 := p.FindMerges(segs)
	a2 := p.FindMerges(segs)
	if len(a1) != len(a2) {
		t.Fatalf("deterministic length: %d vs %d", len(a1), len(a2))
	}
	for i := range a1[0].Sources {
		if a1[0].Sources[i].Path != a2[0].Sources[i].Path {
			t.Fatalf("deterministic source order mismatch at %d", i)
		}
	}
}

func TestFindMergesPacksTowardMaxWhenMaxMergeAtOnceHigh(t *testing.T) {
	const maxBytes int64 = 200
	const perFile int64 = 10
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  100,
		MaxSegmentBytes: maxBytes,
		FloorBytes:      10,
	})
	var segs []Segment
	for i := 0; i < 40; i++ {
		off := time.Duration(i) * time.Minute
		segs = append(segs, seg(0, pathID(i%26)+pathID((i/26)%26), perFile, off, off+30*time.Second))
	}
	actions := p.FindMerges(segs)
	if len(actions) != 1 {
		t.Fatalf("want 1 merge, got %d", len(actions))
	}
	wantN := int(maxBytes / perFile)
	if len(actions[0].Sources) != wantN {
		t.Fatalf("want %d sources packing to max, got %d", wantN, len(actions[0].Sources))
	}
	if wantN <= 6 {
		t.Fatalf("test setup: pack count must exceed SEGMENTS_PER_TIER")
	}
}

func TestNewPlannerDerivesMaxMergeAtOnce(t *testing.T) {
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  0,
		MaxSegmentBytes: 200,
		FloorBytes:      10,
	})
	// 200/10 = 20 floor-sized pieces fit under max.
	if p.cfg.MaxMergeAtOnce != 20 {
		t.Fatalf("derived MaxMergeAtOnce = %d, want 20", p.cfg.MaxMergeAtOnce)
	}
}

func TestNewPlannerDerivedMaxMergeAtOnceAtLeastSegmentsPerTier(t *testing.T) {
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  0,
		MaxSegmentBytes: 50,
		FloorBytes:      20,
	})
	// 50/20 = 2, but trigger floor is SegmentsPerTier.
	if p.cfg.MaxMergeAtOnce != 6 {
		t.Fatalf("derived MaxMergeAtOnce = %d, want ≥ SegmentsPerTier (6)", p.cfg.MaxMergeAtOnce)
	}
}

func pathID(i int) string {
	return string(rune('a' + i))
}
