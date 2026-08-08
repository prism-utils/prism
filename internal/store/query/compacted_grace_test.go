package query

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/elk-utilities/prism/internal/store/layout"
)

const graceTenant = "user-gracequery-apps"

func writeGraceFixture(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("segment"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func markGraceCompacted(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(layout.CompactedMarker(path), []byte("99999999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func resolvedTenantRoot(t *testing.T, dataDir, tenant string) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join(dataDir, tenant))
	if err != nil {
		t.Fatal(err)
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	return filepath.Clean(root)
}

// A merged-away segment keeps its bytes on disk for a grace window so readers
// that already resolved its path can still open it. Its rows now also live in
// the merge output, so counting it here would return every line twice.
func TestScanLogParquetFilesSkipsCompactedSegments(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	tierDir := layout.LogsTierDir(dataDir, graceTenant, artifact, 0)
	live := filepath.Join(tierDir, "1786140844863329878-aaaaaaaa.parquet")
	compacted := filepath.Join(tierDir, "1786140844863329879-bbbbbbbb.parquet")
	writeGraceFixture(t, live)
	writeGraceFixture(t, compacted)
	markGraceCompacted(t, compacted)

	files, err := scanLogParquetFiles(resolvedTenantRoot(t, dataDir, graceTenant))
	if err != nil {
		t.Fatalf("scanLogParquetFiles: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("catalog holds %d files, want only the live segment", len(files))
	}
	if filepath.Base(files[0].Path) != filepath.Base(live) {
		t.Fatalf("catalog holds %s, want %s", files[0].Path, live)
	}
}

func TestCollectMetricsSourcesSkipsCompactedSegments(t *testing.T) {
	dataDir := t.TempDir()
	tierDir := layout.TierDir(dataDir, graceTenant, 0)
	live := filepath.Join(tierDir, "1786140844863329878-aaaaaaaa.parquet")
	compacted := filepath.Join(tierDir, "1786140844863329879-bbbbbbbb.parquet")
	writeGraceFixture(t, live)
	writeGraceFixture(t, compacted)
	markGraceCompacted(t, compacted)

	sources, err := collectMetricsSources(resolvedTenantRoot(t, dataDir, graceTenant), false)
	if err != nil {
		t.Fatalf("collectMetricsSources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("metrics sources = %v, want only the live segment", sources)
	}
	if filepath.Base(sources[0].Path) != filepath.Base(live) {
		t.Fatalf("metrics sources = %v, want %s", sources, live)
	}
}

// The marker itself is not a segment: opening it as parquet would fail the
// whole relation.
func TestScanLogParquetFilesIgnoresTheMarkerFile(t *testing.T) {
	dataDir := t.TempDir()
	artifact := "logs-raw"
	tierDir := layout.LogsTierDir(dataDir, graceTenant, artifact, 0)
	compacted := filepath.Join(tierDir, "1786140844863329879-bbbbbbbb.parquet")
	writeGraceFixture(t, compacted)
	markGraceCompacted(t, compacted)

	files, err := scanLogParquetFiles(resolvedTenantRoot(t, dataDir, graceTenant))
	if err != nil {
		t.Fatalf("scanLogParquetFiles: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("catalog holds %v, want nothing searchable", files)
	}
}
