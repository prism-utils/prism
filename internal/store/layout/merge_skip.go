package layout

import (
	"io/fs"
	"strings"
)

// mergeSkipSuffix names the sidecar that keeps a live segment out of merge
// planning after rewrite attempts are exhausted. The file stays readable.
const mergeSkipSuffix = ".merge-skip"

// mergeAttemptsSuffix names the sidecar that counts failed rewrites.
const mergeAttemptsSuffix = ".merge-attempts"

// MergeSkipMarker returns the sidecar path that marks a segment unmergeable.
func MergeSkipMarker(segmentPath string) string {
	return segmentPath + mergeSkipSuffix
}

// MergeAttemptsMarker returns the sidecar path that stores rewrite attempts.
func MergeAttemptsMarker(segmentPath string) string {
	return segmentPath + mergeAttemptsSuffix
}

// IsMergeSkipMarker reports whether a directory entry name is a skip sidecar.
func IsMergeSkipMarker(name string) bool {
	_, ok := MergeSkipSegmentName(name)
	return ok
}

// MergeSkipSegmentName returns the segment a skip sidecar names.
func MergeSkipSegmentName(markerName string) (string, bool) {
	if len(markerName) <= len(mergeSkipSuffix) || !strings.HasSuffix(markerName, mergeSkipSuffix) {
		return "", false
	}
	return strings.TrimSuffix(markerName, mergeSkipSuffix), true
}

// MergeSkipSet names the segments in one listing that must not be merge inputs.
func MergeSkipSet(entries []fs.DirEntry) map[string]struct{} {
	var out map[string]struct{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		segment, ok := MergeSkipSegmentName(e.Name())
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
