package merge

import (
	"sort"
	"time"
)

// LogRetentionConfig controls log segment expiry and optional file caps.
type LogRetentionConfig struct {
	RetentionDays int
	// MaxLogFiles caps segment count (0 = disabled). Callers should pass the
	// hot landing set only; cold tiers are retained by RetentionDays so a
	// landing flood cannot erase sealed history.
	MaxLogFiles int
}

// LogRetention selects segments to delete by age and optional max file count.
func LogRetention(segments []Segment, now time.Time, cfg LogRetentionConfig) []DeleteAction {
	deleted := map[string]struct{}{}
	var out []DeleteAction

	for _, del := range Retention(segments, now, RetentionConfig{RetentionDays: cfg.RetentionDays}) {
		out = append(out, del)
		deleted[del.Segment.Path] = struct{}{}
	}

	if cfg.MaxLogFiles <= 0 {
		return out
	}

	remaining := make([]Segment, 0, len(segments))
	for _, s := range segments {
		if _, gone := deleted[s.Path]; gone {
			continue
		}
		remaining = append(remaining, s)
	}
	if len(remaining) <= cfg.MaxLogFiles {
		return out
	}

	sort.Slice(remaining, func(i, j int) bool {
		if remaining[i].MinTs.Equal(remaining[j].MinTs) {
			return remaining[i].Path < remaining[j].Path
		}
		return remaining[i].MinTs.Before(remaining[j].MinTs)
	})
	excess := len(remaining) - cfg.MaxLogFiles
	for i := 0; i < excess; i++ {
		out = append(out, DeleteAction{Segment: remaining[i]})
	}
	return out
}
