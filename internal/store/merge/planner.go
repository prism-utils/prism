package merge

import (
	"math"
	"sort"
	"time"
)

// PlannerConfig mirrors Lucene TieredMergePolicy knobs for the parquet tier store.
type PlannerConfig struct {
	SegmentsPerTier int
	MaxMergeAtOnce  int
	MaxSegmentBytes int64
	FloorBytes      int64
}

// DefaultPlannerConfig returns production defaults with a 1 MiB floor for tests
// when FloorBytes is unset by callers that pass zero. MaxMergeAtOnce 0 means
// NewPlanner derives how many floor-sized pieces fit under MaxSegmentBytes.
func DefaultPlannerConfig() PlannerConfig {
	return PlannerConfig{
		SegmentsPerTier: 6,
		MaxMergeAtOnce:  0,
		MaxSegmentBytes: 2 << 30,
		FloorBytes:      1 << 20,
	}
}

// Planner selects tier merges without DuckDB (pure Go, fast tests).
type Planner struct {
	cfg PlannerConfig
}

// NewPlanner returns a merge planner with defaulted configuration.
func NewPlanner(cfg PlannerConfig) *Planner {
	if cfg.SegmentsPerTier <= 0 {
		cfg.SegmentsPerTier = 6
	}
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = 2 << 30
	}
	if cfg.FloorBytes <= 0 {
		cfg.FloorBytes = 1 << 20
	}
	if cfg.MaxMergeAtOnce <= 0 {
		cfg.MaxMergeAtOnce = derivedMaxMergeAtOnce(cfg.MaxSegmentBytes, cfg.FloorBytes, cfg.SegmentsPerTier)
	}
	return &Planner{cfg: cfg}
}

// derivedMaxMergeAtOnce is how many floor-sized segments fit under the seal
// budget, never below SegmentsPerTier (the merge trigger).
func derivedMaxMergeAtOnce(maxBytes, floorBytes int64, segmentsPerTier int) int {
	if floorBytes <= 0 {
		floorBytes = 1 << 20
	}
	n := int(maxBytes / floorBytes)
	if n < segmentsPerTier {
		n = segmentsPerTier
	}
	if n < 1 {
		n = 1
	}
	return n
}

// FindMerges returns at most one merge action per source tier (no cascade).
func (p *Planner) FindMerges(segments []Segment) []MergeAction {
	if len(segments) == 0 {
		return nil
	}
	byTier := map[int][]Segment{}
	for _, s := range segments {
		if s.Bytes >= p.cfg.MaxSegmentBytes {
			continue
		}
		byTier[s.Tier] = append(byTier[s.Tier], s)
	}

	var actions []MergeAction
	tiers := sortedKeys(byTier)
	for _, tier := range tiers {
		if action, ok := p.findMergeForTier(byTier[tier], tier); ok {
			actions = append(actions, action)
			break
		}
	}
	return actions
}

func (p *Planner) findMergeForTier(segs []Segment, tier int) (MergeAction, bool) {
	if len(segs) < p.cfg.SegmentsPerTier {
		return MergeAction{}, false
	}
	levels := map[int][]Segment{}
	for _, s := range segs {
		lvl := p.sizeLevel(s.Bytes)
		levels[lvl] = append(levels[lvl], s)
	}
	levelIDs := sortedKeys(levels)
	for _, lvl := range levelIDs {
		group := levels[lvl]
		if len(group) < p.cfg.SegmentsPerTier {
			continue
		}
		sort.Slice(group, func(i, j int) bool {
			if group[i].MinTs.Equal(group[j].MinTs) {
				return group[i].Path < group[j].Path
			}
			return group[i].MinTs.Before(group[j].MinTs)
		})
		candidates := pickTimeAdjacent(group, p.cfg.MaxMergeAtOnce)
		if len(candidates) < p.cfg.SegmentsPerTier {
			continue
		}
		for n := len(candidates); n >= 1; n-- {
			subset := candidates[:n]
			sum := int64(0)
			for _, s := range subset {
				sum += s.Bytes
			}
			if sum <= p.cfg.MaxSegmentBytes {
				return MergeAction{Sources: subset, DestTier: tier + 1}, true
			}
		}
	}
	return MergeAction{}, false
}

func (p *Planner) sizeLevel(bytes int64) int {
	floor := float64(p.cfg.FloorBytes)
	if bytes <= p.cfg.FloorBytes {
		return 0
	}
	ratio := float64(bytes) / floor
	return int(math.Floor(math.Log(ratio) / math.Log(float64(p.cfg.SegmentsPerTier))))
}

func pickTimeAdjacent(sorted []Segment, max int) []Segment {
	if len(sorted) == 0 {
		return nil
	}
	best := []Segment{sorted[0]}
	cur := []Segment{sorted[0]}
	for i := 1; i < len(sorted); i++ {
		prev := cur[len(cur)-1]
		next := sorted[i]
		if !rangesAdjacent(&prev, &next) {
			if len(cur) > len(best) {
				best = append([]Segment(nil), cur...)
			}
			cur = []Segment{next}
			continue
		}
		cur = append(cur, next)
	}
	if len(cur) > len(best) {
		best = cur
	}
	if len(best) > max {
		best = best[:max]
	}
	return best
}

func rangesAdjacent(a, b *Segment) bool {
	gap := b.MinTs.Sub(a.MaxTs)
	if gap < 0 {
		return false
	}
	span := a.MaxTs.Sub(a.MinTs)
	if span <= 0 {
		span = time.Minute
	}
	return gap <= span
}

func sortedKeys[V any](m map[int]V) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}
