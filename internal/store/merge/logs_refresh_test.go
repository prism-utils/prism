package merge

import (
	"testing"
	"time"
)

// refreshPlanner sizes the seal budget so no fixture segment counts as sealed
// and a pack is bounded by MaxMergeAtOnce rather than by bytes.
func refreshPlanner(interval time.Duration, maxActions, maxMergeAtOnce int) *Planner {
	return NewPlanner(PlannerConfig{
		SegmentsPerTier:       6,
		MaxMergeAtOnce:        maxMergeAtOnce,
		MaxSegmentBytes:       200,
		FloorBytes:            10,
		LogsRefreshInterval:   interval,
		LogsRefreshMaxActions: maxActions,
	})
}

func landingSegs(n int, bytes int64, spacing time.Duration) []Segment {
	out := make([]Segment, 0, n)
	for i := 0; i < n; i++ {
		off := time.Duration(i) * spacing
		out = append(out, seg(logLandingTier, "l"+pathID(i%26)+pathID((i/26)%26), bytes, off, off))
	}
	return out
}

func totalSources(actions []LogMergeAction) int {
	n := 0
	for _, a := range actions {
		n += len(a.Sources)
	}
	return n
}

func assertSourcesDisjoint(t *testing.T, actions []LogMergeAction) {
	t.Helper()
	seen := map[string]int{}
	for i, a := range actions {
		for _, s := range a.Sources {
			if prev, dup := seen[s.Path]; dup {
				t.Fatalf("segment %s planned twice (actions %d and %d)", s.Path, prev, i)
			}
			seen[s.Path] = i
		}
	}
}

func TestDefaultPlannerConfigRefreshDefaults(t *testing.T) {
	cfg := DefaultPlannerConfig()
	if cfg.LogsRefreshInterval != time.Minute {
		t.Fatalf("default LogsRefreshInterval = %v, want 1m", cfg.LogsRefreshInterval)
	}
	if cfg.LogsRefreshMaxActions != 8 {
		t.Fatalf("default LogsRefreshMaxActions = %d, want 8", cfg.LogsRefreshMaxActions)
	}
}

func TestLogRefreshAgeTriggersBelowSegmentsPerTier(t *testing.T) {
	p := refreshPlanner(time.Minute, 8, 6)
	landing := landingSegs(2, 10, 30*time.Second)
	now := fixtureBase.Add(90 * time.Second)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) != 1 {
		t.Fatalf("want 1 age-triggered refresh below segments-per-tier, got %d", len(actions))
	}
	if actions[0].DestTier != 0 {
		t.Fatalf("refresh DestTier = %d, want 0", actions[0].DestTier)
	}
	if len(actions[0].Sources) != 2 {
		t.Fatalf("refresh sources = %d, want both live landing files", len(actions[0].Sources))
	}
}

func TestLogRefreshAgeTriggersAtExactInterval(t *testing.T) {
	p := refreshPlanner(time.Minute, 8, 6)
	landing := landingSegs(1, 10, 0)
	now := fixtureBase.Add(time.Minute)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) != 1 || len(actions[0].Sources) != 1 {
		t.Fatalf("oldest landing exactly at the interval must refresh, got %+v", actions)
	}
}

func TestLogRefreshBelowIntervalAndBelowCountNoAction(t *testing.T) {
	p := refreshPlanner(time.Minute, 8, 6)
	landing := landingSegs(2, 10, time.Second)
	now := fixtureBase.Add(59 * time.Second)

	if actions := p.FindLogMerges(now, landing, nil); len(actions) != 0 {
		t.Fatalf("young landing below segments-per-tier must not refresh, got %+v", actions)
	}
}

func TestLogRefreshIntervalZeroDisablesAgeTrigger(t *testing.T) {
	p := refreshPlanner(0, 8, 6)
	landing := landingSegs(5, 10, time.Minute)
	now := fixtureBase.Add(24 * time.Hour)

	if actions := p.FindLogMerges(now, landing, nil); len(actions) != 0 {
		t.Fatalf("interval 0 must leave the count trigger alone, got %+v", actions)
	}
}

func TestLogRefreshCountTriggerFiresBeforeInterval(t *testing.T) {
	p := refreshPlanner(time.Hour, 8, 6)
	landing := landingSegs(6, 10, time.Second)
	now := fixtureBase.Add(2 * time.Second)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) != 1 {
		t.Fatalf("want 1 count-triggered refresh, got %d", len(actions))
	}
	if len(actions[0].Sources) != 6 {
		t.Fatalf("count-triggered sources = %d, want 6", len(actions[0].Sources))
	}
}

func TestLogRefreshSealedLandingNotAgeTriggered(t *testing.T) {
	p := refreshPlanner(time.Minute, 8, 6)
	landing := []Segment{seg(logLandingTier, "sealed", 200, 0, 0)}
	now := fixtureBase.Add(time.Hour)

	if actions := p.FindLogMerges(now, landing, nil); len(actions) != 0 {
		t.Fatalf("sealed landing must never be a refresh source, got %+v", actions)
	}
}

func TestLogRefreshDrainsUpToMaxActions(t *testing.T) {
	const maxActions = 3
	const perAction = 6
	p := refreshPlanner(0, maxActions, perAction)
	landing := landingSegs(40, 10, time.Minute)
	now := fixtureBase.Add(time.Hour)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) != maxActions {
		t.Fatalf("drain planned %d refreshes, want the %d-action cap", len(actions), maxActions)
	}
	for i, a := range actions {
		if a.DestTier != 0 {
			t.Fatalf("action %d DestTier = %d, want 0 (landing refresh)", i, a.DestTier)
		}
		if len(a.Sources) != perAction {
			t.Fatalf("action %d sources = %d, want %d", i, len(a.Sources), perAction)
		}
	}
	assertSourcesDisjoint(t, actions)
	if got := totalSources(actions); got != maxActions*perAction {
		t.Fatalf("drained sources = %d, want %d", got, maxActions*perAction)
	}
}

func TestLogRefreshDrainStopsWhenRemainderBelowTriggers(t *testing.T) {
	p := refreshPlanner(0, 8, 6)
	landing := landingSegs(8, 10, time.Minute)
	now := fixtureBase.Add(time.Hour)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) != 1 {
		t.Fatalf("count-only drain planned %d refreshes, want 1 (remainder below trigger)", len(actions))
	}
	if got := totalSources(actions); got != 6 {
		t.Fatalf("drained sources = %d, want 6", got)
	}
}

func TestLogRefreshDrainRefreshesRemainderByAge(t *testing.T) {
	p := refreshPlanner(time.Minute, 8, 6)
	landing := landingSegs(8, 10, time.Minute)
	now := fixtureBase.Add(24 * time.Hour)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) != 2 {
		t.Fatalf("aged drain planned %d refreshes, want 2 (6 + remainder)", len(actions))
	}
	assertSourcesDisjoint(t, actions)
	if got := totalSources(actions); got != 8 {
		t.Fatalf("drained sources = %d, want all 8 aged files", got)
	}
}

func TestLogRefreshDrainOrdersOldestFirst(t *testing.T) {
	p := refreshPlanner(time.Minute, 8, 6)
	landing := landingSegs(18, 10, time.Minute)
	now := fixtureBase.Add(24 * time.Hour)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) < 3 {
		t.Fatalf("want newest-first plus two drain packs, got %d", len(actions))
	}
	drainNewest := actions[1].Sources[len(actions[1].Sources)-1].MinTs
	nextOldest := actions[2].Sources[0].MinTs
	if nextOldest.Before(drainNewest) {
		t.Fatalf("second drain starts at %v, before the first drain ends at %v", nextOldest, drainNewest)
	}
}

func TestLogRefreshNewestFirstWhenBacklogExceedsOnePack(t *testing.T) {
	const maxActions = 3
	const perAction = 6
	p := refreshPlanner(time.Minute, maxActions, perAction)
	landing := landingSegs(20, 10, time.Minute)
	now := fixtureBase.Add(24 * time.Hour)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) != maxActions {
		t.Fatalf("planned %d refreshes, want %d", len(actions), maxActions)
	}
	assertSourcesDisjoint(t, actions)

	newest := landing[len(landing)-1].MinTs
	var sawNewest bool
	for _, s := range actions[0].Sources {
		if s.MinTs.Equal(newest) {
			sawNewest = true
		}
	}
	if !sawNewest {
		t.Fatalf("first refresh must pack the newest landing (%v); got %+v", newest, actions[0].Sources)
	}

	firstOldest := actions[1].Sources[0].MinTs
	if !firstOldest.Equal(landing[0].MinTs) {
		t.Fatalf("remaining drain must start at the oldest landing, got %v want %v", firstOldest, landing[0].MinTs)
	}
}

func TestLogRefreshPacksTinyLandingsTowardSealBudget(t *testing.T) {
	p := NewPlanner(PlannerConfig{
		SegmentsPerTier:       6,
		MaxMergeAtOnce:        0,
		MaxSegmentBytes:       2 << 30,
		FloorBytes:            360 << 20,
		LogsRefreshInterval:   time.Minute,
		LogsRefreshMaxActions: 2,
	})
	landing := landingSegs(80, 536000, time.Minute)
	now := fixtureBase.Add(24 * time.Hour)

	actions := p.FindLogMerges(now, landing, nil)
	if len(actions) == 0 {
		t.Fatal("want at least one refresh")
	}
	if got := len(actions[0].Sources); got <= 6 {
		t.Fatalf("tiny landing pack width = %d, want more than the metrics-floor cap of 6", got)
	}
	if got := len(actions[0].Sources); got > 64 {
		t.Fatalf("tiny landing pack width = %d, want capped at 64", got)
	}
}

func TestLogRefreshPrecedesTierPack(t *testing.T) {
	p := refreshPlanner(time.Minute, 8, 6)
	landing := landingSegs(2, 10, time.Second)
	var tiers []Segment
	for i := 0; i < 6; i++ {
		off := time.Duration(i) * 6 * time.Minute
		tiers = append(tiers, seg(0, "t"+pathID(i), 10, off, off))
	}
	now := fixtureBase.Add(time.Hour)

	actions := p.FindLogMerges(now, landing, tiers)
	if len(actions) != 2 {
		t.Fatalf("want landing refresh + tier pack, got %d actions", len(actions))
	}
	if actions[0].DestTier != 0 {
		t.Fatalf("landing refresh must be planned first, got DestTier %d", actions[0].DestTier)
	}
	if actions[1].DestTier != 1 {
		t.Fatalf("tier pack DestTier = %d, want 1", actions[1].DestTier)
	}
}
