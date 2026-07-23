package merge

import (
	"testing"
	"time"
)

func TestRetentionDeletesOlderThan15Days(t *testing.T) {
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	old := Segment{Tier: 0, Path: "old.parquet", MaxTs: now.Add(-16 * 24 * time.Hour)}
	recent := Segment{Tier: 0, Path: "recent.parquet", MaxTs: now.Add(-14 * 24 * time.Hour)}
	actions := Retention([]Segment{old, recent}, now, RetentionConfig{RetentionDays: 15})
	if len(actions) != 1 {
		t.Fatalf("want 1 delete, got %d", len(actions))
	}
	if actions[0].Segment.Path != "old.parquet" {
		t.Fatalf("want old segment deleted, got %s", actions[0].Segment.Path)
	}
}

func TestRetentionExactly15DaysNotDeleted(t *testing.T) {
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	boundary := Segment{Tier: 0, Path: "boundary.parquet", MaxTs: now.Add(-15 * 24 * time.Hour)}
	if actions := Retention([]Segment{boundary}, now, RetentionConfig{RetentionDays: 15}); len(actions) != 0 {
		t.Fatalf("exactly 15d boundary must not be deleted, got %v", actions)
	}
}
