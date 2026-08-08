package layout

import (
	"io/fs"
	"strings"
)

// compactedSuffix names the sidecar that retires a segment: the segment's rows
// have been rewritten into a parent, but its bytes are held at their original
// path for a delete grace window. The suffix is not a segment extension, so a
// marker is invisible to every listing that opens segments — including a
// dashboard glob for "*.parquet".
const compactedSuffix = ".compacted"

// compactedTempSuffix names a marker write that has not been renamed into
// place. It is never a live marker; a later pass reclaims it.
const compactedTempSuffix = compactedSuffix + ".tmp"

// CompactedMarker returns the sidecar path that retires a segment.
func CompactedMarker(segmentPath string) string {
	return segmentPath + compactedSuffix
}

// CompactedMarkerTemp returns the staging path a marker is written to before it
// is renamed into place, so a torn write is never read as a deadline.
func CompactedMarkerTemp(segmentPath string) string {
	return segmentPath + compactedTempSuffix
}

// IsCompactedMarker reports whether a directory entry name is a marker.
func IsCompactedMarker(name string) bool {
	_, ok := CompactedSegmentName(name)
	return ok
}

// IsCompactedMarkerTemp reports whether a directory entry name is an
// unfinished marker write.
func IsCompactedMarkerTemp(name string) bool {
	return len(name) > len(compactedTempSuffix) && strings.HasSuffix(name, compactedTempSuffix)
}

// CompactedSegmentName returns the segment a marker name retires.
func CompactedSegmentName(markerName string) (string, bool) {
	if IsCompactedMarkerTemp(markerName) {
		return "", false
	}
	if len(markerName) <= len(compactedSuffix) || !strings.HasSuffix(markerName, compactedSuffix) {
		return "", false
	}
	return strings.TrimSuffix(markerName, compactedSuffix), true
}

// CompactedSet names the retired segments in one directory listing. Building it
// from a listing the caller already has keeps the skip a map lookup rather than
// a stat per candidate file.
func CompactedSet(entries []fs.DirEntry) map[string]struct{} {
	var out map[string]struct{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		segment, ok := CompactedSegmentName(e.Name())
		if !ok {
			continue
		}
		if out == nil {
			out = make(map[string]struct{})
		}
		out[segment] = struct{}{}
	}
	return out
}
