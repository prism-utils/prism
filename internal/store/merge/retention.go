package merge

import (
	"os"
	"path/filepath"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
)

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

// UnreadableExpired lists segment files in dir that are not among live and
// whose modification time is strictly before cutoff. Retired names are skipped.
func UnreadableExpired(dir string, live []Segment, cutoff time.Time) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	keep := make(map[string]struct{}, len(live))
	for _, s := range live {
		keep[s.Path] = struct{}{}
	}
	retired := layout.CompactedSet(entries)
	var out []string
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		if _, held := retired[e.Name()]; held {
			continue
		}
		path := filepath.Join(dir, e.Name())
		if _, ok := keep[path]; ok {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		if !fi.ModTime().Before(cutoff) {
			continue
		}
		out = append(out, path)
	}
	return out, nil
}
