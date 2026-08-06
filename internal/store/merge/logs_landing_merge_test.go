package merge

import (
	"testing"
	"time"
)

func TestFindLogLandingMergeIgnoresSealed(t *testing.T) {
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  6,
		MaxSegmentBytes: 200,
		FloorBytes:      10,
	})
	landing := []Segment{
		seg(-1, "sealed", 200, 0, time.Minute),
	}
	for i := 0; i < 6; i++ {
		off := time.Duration(i+1) * time.Hour
		landing = append(landing, seg(-1, "l"+pathID(i), 20, off, off+time.Minute))
	}
	actions := p.FindLogMerges(landing, nil)
	if len(actions) != 1 {
		t.Fatalf("want 1 landing merge ignoring sealed, got %d", len(actions))
	}
	for _, s := range actions[0].Sources {
		if s.Bytes >= 200 {
			t.Fatalf("sealed segment must not be a merge source: %+v", s)
		}
	}
	if len(actions[0].Sources) != 6 {
		t.Fatalf("want 6 unsealed sources, got %d", len(actions[0].Sources))
	}
}

func TestFindLogLandingMergeShrinksWhenSumExceedsMax(t *testing.T) {
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  6,
		MaxSegmentBytes: 200,
		FloorBytes:      10,
	})
	var landing []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i) * time.Minute
		landing = append(landing, seg(-1, pathID(i), 80, off, off+30*time.Second))
	}
	actions := p.FindLogMerges(landing, nil)
	if len(actions) != 1 {
		t.Fatalf("want 1 merge after shrink, got %d", len(actions))
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

func TestFindLogLandingMergeAllSealedNoAction(t *testing.T) {
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  6,
		MaxSegmentBytes: 200,
		FloorBytes:      10,
	})
	var landing []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i) * time.Minute
		landing = append(landing, seg(-1, pathID(i), 200, off, off+30*time.Second))
	}
	if actions := p.FindLogMerges(landing, nil); len(actions) != 0 {
		t.Fatalf("all-sealed landing should not merge, got %v", actions)
	}
}
