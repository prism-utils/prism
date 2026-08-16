package merge

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
)

// retireSources releases the inputs of a completed merge. A positive grace
// keeps their bytes exactly where they are and records a delete deadline
// beside each one, because a reader resolves a path before it opens it and
// cannot recover from one that vanished in between — DuckDB fails the whole
// relation. Zero deletes on the spot.
func retireSources(sources []Segment, now time.Time, grace time.Duration) error {
	for _, s := range sources {
		if grace <= 0 {
			if err := os.Remove(s.Path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("merge: delete %s: %w", s.Path, err)
			}
			continue
		}
		if _, err := os.Stat(s.Path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("merge: hold %s: %w", s.Path, err)
		}
		if err := writeCompactedMarker(s.Path, now.Add(grace)); err != nil {
			return err
		}
	}
	return nil
}

// writeCompactedMarker records when a held segment may be unlinked. The
// deadline is staged and renamed so a crashed write is never read as a
// deadline of its own.
func writeCompactedMarker(segmentPath string, deleteAfter time.Time) error {
	tmp := layout.CompactedMarkerTemp(segmentPath)
	body := strconv.FormatInt(deleteAfter.UTC().Unix(), 10) + "\n"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		return fmt.Errorf("merge: mark %s compacted: %w", segmentPath, err)
	}
	if err := os.Rename(tmp, layout.CompactedMarker(segmentPath)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("merge: mark %s compacted: %w", segmentPath, err)
	}
	return nil
}

// readCompactedDeadline reports when a held segment may be unlinked. A marker
// that cannot be parsed reports no deadline, which reclaims the bytes on the
// next pass: the rows already live in the merge output, so failing toward
// space is safer than holding them forever.
func readCompactedDeadline(markerPath string) (time.Time, bool, error) {
	body, err := os.ReadFile(markerPath) //nolint:gosec // G304: server-owned sidecar beside a segment it wrote
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, err
	}
	secs, ok := parseUnixSeconds(string(body))
	if !ok {
		return time.Time{}, false, nil
	}
	return time.Unix(secs, 0).UTC(), true, nil
}

func parseUnixSeconds(body string) (int64, bool) {
	secs, err := strconv.ParseInt(strings.TrimSpace(body), 10, 64)
	if err != nil {
		return 0, false
	}
	return secs, true
}

// purgeCompactedDir unlinks the held segments in one directory whose grace has
// expired, and returns how many went away. Markers whose segment is already
// gone, and staged marker writes nobody finished, are reclaimed too.
func purgeCompactedDir(dir string, now time.Time) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	purged := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if layout.IsCompactedMarkerTemp(e.Name()) {
			if err := removeIfPresent(filepath.Join(dir, e.Name())); err != nil {
				return purged, err
			}
			continue
		}
		segmentName, ok := layout.CompactedSegmentName(e.Name())
		if !ok {
			continue
		}
		markerPath := filepath.Join(dir, e.Name())
		segmentPath := filepath.Join(dir, segmentName)
		gone, err := purgeHeldSegment(segmentPath, markerPath, now)
		if err != nil {
			return purged, err
		}
		if gone {
			purged++
		}
	}
	return purged, nil
}

// purgeHeldSegment drops one held segment and its marker once the deadline has
// passed, and reports whether segment bytes were reclaimed.
func purgeHeldSegment(segmentPath, markerPath string, now time.Time) (bool, error) {
	segmentPresent := true
	if _, err := os.Stat(segmentPath); err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		segmentPresent = false
	}
	if segmentPresent {
		deadline, ok, err := readCompactedDeadline(markerPath)
		if err != nil {
			return false, err
		}
		if ok && !now.After(deadline) {
			return false, nil
		}
		if err := removeIfPresent(segmentPath); err != nil {
			return false, err
		}
	}
	if err := removeIfPresent(markerPath); err != nil {
		return segmentPresent, err
	}
	return segmentPresent, nil
}

// PurgeCompacted reclaims every expired hold of one tenant — metrics tiers,
// log landing zones, and log tiers — and returns how many segments it deleted.
func PurgeCompacted(dataDir, tenant string, maxTier int, now time.Time) (int, error) {
	dirs := make([]string, 0, maxTier+1)
	for tier := 0; tier <= maxTier; tier++ {
		dirs = append(dirs, layout.TierDir(dataDir, tenant, tier))
	}
	artifacts, err := ListLogArtifacts(dataDir, tenant)
	if err != nil {
		return 0, err
	}
	for _, artifact := range artifacts {
		dirs = append(dirs, layout.LogsLandingDir(dataDir, tenant, artifact))
		for tier := 0; tier <= maxTier; tier++ {
			dirs = append(dirs, layout.LogsTierDir(dataDir, tenant, artifact, tier))
		}
	}
	matRoot := filepath.Join(dataDir, tenant, "materializations")
	if ents, err := os.ReadDir(matRoot); err == nil {
		for _, e := range ents {
			if e.IsDir() {
				dirs = append(dirs, filepath.Join(matRoot, e.Name()))
			}
		}
	} else if err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	purged := 0
	for _, dir := range dirs {
		n, err := purgeCompactedDir(dir, now)
		purged += n
		if err != nil {
			return purged, err
		}
	}
	return purged, nil
}

func removeIfPresent(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
