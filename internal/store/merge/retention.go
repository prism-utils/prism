package merge

import "time"

// RetentionConfig controls segment expiry.
type RetentionConfig struct {
	RetentionDays int
}

// Retention selects segments whose MaxTs is strictly older than the retention window.
func Retention(segments []Segment, now time.Time, cfg RetentionConfig) []DeleteAction {
	return nil
}
