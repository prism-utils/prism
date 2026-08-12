package logmeta

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prism-utils/prism/internal/store/layout"
)

// A segment whose rows were merged into a parent is held on disk for a delete
// grace window. It is not searchable any more, so cataloguing it would double
// every one of its lines in the relation the planners build.
func TestRebuildManifestSkipsCompactedSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-test-apps"
	tierDir := filepath.Join(dir, tenant, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(tierDir, "1786140844863329878-aaaaaaaa.parquet")
	compacted := filepath.Join(tierDir, "1786140848277436838-bbbbbbbb.parquet")
	writeFixture(t, live)
	writeFixture(t, compacted)
	writeFixture(t, layout.CompactedMarker(compacted))

	m, err := RebuildManifest(dir, tenant, "logs-raw", 4)
	if err != nil {
		t.Fatalf("RebuildManifest: %v", err)
	}
	if len(m.Files) != 1 {
		t.Fatalf("manifest files = %v, want only the live segment", m.Files)
	}
	if want := "tiers/L0/1786140844863329878-aaaaaaaa.parquet"; m.Files[0].Path != want {
		t.Fatalf("manifest file = %s, want %s", m.Files[0].Path, want)
	}
}

func TestRebuildManifestSkipsCompactedLandingWindows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-test-apps"
	landing := filepath.Join(dir, tenant, "logs", "logs-raw")
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	compacted := filepath.Join(landing, "1786140848277436838-bbbbbbbb.parquet")
	writeFixture(t, compacted)
	writeFixture(t, layout.CompactedMarker(compacted))

	m, err := RebuildManifest(dir, tenant, "logs-raw", 4)
	if err != nil {
		t.Fatalf("RebuildManifest: %v", err)
	}
	if len(m.Files) != 0 {
		t.Fatalf("manifest files = %v, want nothing catalogued", m.Files)
	}
}
