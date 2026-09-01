package merge

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/prism-utils/prism/internal/store/layout"
)

// MergeMaxRewriteAttempts is how many failed concat/k-way+COPY cycles a source
// may take before it is marked unmergeable. The segment stays queryable.
const MergeMaxRewriteAttempts = 5

const skipReasonTooLarge = "too-large"

// RecordRewriteFailure increments the attempt sidecar for each source and writes
// a skip marker once the budget is exhausted.
func RecordRewriteFailure(sources []Segment) error {
	for _, s := range sources {
		if s.Path == "" {
			continue
		}
		if _, err := os.Stat(layout.MergeSkipMarker(s.Path)); err == nil {
			continue
		}
		n, err := readMergeAttempts(s.Path)
		if err != nil {
			return err
		}
		n++
		if err := writeMergeAttempts(s.Path, n); err != nil {
			return err
		}
		if n >= MergeMaxRewriteAttempts {
			if err := writeMergeSkip(s.Path, n, skipReasonTooLarge); err != nil {
				return err
			}
		}
	}
	return nil
}

func readMergeAttempts(segmentPath string) (int, error) {
	body, err := os.ReadFile(layout.MergeAttemptsMarker(segmentPath)) //nolint:gosec // G304: sidecar beside a server-owned segment
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(body)))
	if err != nil || n < 0 {
		n = 0
	}
	return n, nil
}

func writeMergeAttempts(segmentPath string, n int) error {
	path := layout.MergeAttemptsMarker(segmentPath)
	body := strconv.Itoa(n) + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return fmt.Errorf("merge: write attempts %s: %w", path, err)
	}
	return nil
}

func writeMergeSkip(segmentPath string, attempts int, reason string) error {
	path := layout.MergeSkipMarker(segmentPath)
	body := fmt.Sprintf("attempts=%d\nreason=%s\n", attempts, reason)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return fmt.Errorf("merge: write skip %s: %w", path, err)
	}
	return nil
}
