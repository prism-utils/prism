package merge

import "time"

// RetentionConfig controls segment expiry.
type RetentionConfig struct {
	RetentionDays int
}

// Retention selects segments whose MaxTs is strictly older than the retention window.
func Retention(segments []Segment, now time.Time, cfg RetentionConfig) []DeleteAction {
	if cfg.RetentionDays <= 0 {
		cfg.RetentionDays = 15
	}
	cutoff := now.Add(-time.Duration(cfg.RetentionDays) * 24 * time.Hour)
	var out []DeleteAction
	for _, s := range segments {
		if s.MaxTs.Before(cutoff) {
			out = append(out, DeleteAction{Segment: s})
		}
	}
	return out
}
