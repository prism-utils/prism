package logmeta

import (
	"os"
	"path/filepath"
	"testing"
)

// A refresh writes the L0 segment in MERGE_SEGMENT_FORMAT, so a tier can hold
// .duckdb segments. The manifest is the catalog the query planners trust ahead
// of a directory walk: dropping duckdb segments from it hides refreshed rows
// from every search.
func TestRebuildManifestRecordsDuckDBAndParquetSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-test-apps"
	tierDir := filepath.Join(dir, tenant, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(tierDir, "1786140844863329878-aaaaaaaa.parquet"))
	writeFixture(t, filepath.Join(tierDir, "1786140848277436838-bbbbbbbb.duckdb"))
	// Neither of these is an openable segment.
	writeFixture(t, filepath.Join(tierDir, "_manifest.json"))
	writeFixture(t, filepath.Join(tierDir, ".hidden.parquet"))

	m, err := RebuildManifest(dir, tenant, "logs-raw", 3)
	if err != nil {
		t.Fatalf("RebuildManifest: %v", err)
	}
	if m.Version != 3 {
		t.Fatalf("version=%d want 3", m.Version)
	}
	var got []string
	for _, f := range m.Files {
		got = append(got, f.Path)
	}
	want := []string{
		"tiers/L0/1786140844863329878-aaaaaaaa.parquet",
		"tiers/L0/1786140848277436838-bbbbbbbb.duckdb",
	}
	if len(got) != len(want) {
		t.Fatalf("manifest files = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("manifest files = %v, want %v", got, want)
		}
	}
	for _, f := range m.Files {
		if f.MinTsNs == 0 || f.MaxTsNs == 0 {
			t.Fatalf("bounds unset for %s", f.Path)
		}
	}
}

// The label index only understands parquet. Now that duckdb segments are
// catalogued, indexing must skip them instead of failing the whole build and
// taking the Loki label APIs down with it.
func TestEnsureLabelIndexSkipsDuckDBSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-test-apps"
	tierDir := filepath.Join(dir, tenant, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(tierDir, "1786140848277436838-bbbbbbbb.duckdb"))

	if _, err := EnsureLabelIndex(dir, tenant); err != nil {
		t.Fatalf("EnsureLabelIndex: %v", err)
	}
}

func writeFixture(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
}
