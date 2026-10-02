package layout

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

func TestMergeSkipMarkerJoinsSuffix(t *testing.T) {
	t.Parallel()
	seg := "/data/t/tiers/L0/a.parquet"
	if got := MergeSkipMarker(seg); got != seg+".merge-skip" {
		t.Fatalf("MergeSkipMarker = %q", got)
	}
	if got := MergeAttemptsMarker(seg); got != seg+".merge-attempts" {
		t.Fatalf("MergeAttemptsMarker = %q", got)
	}
}

func TestMergeSkipSetNamesSkippedSegmentsOnly(t *testing.T) {
	t.Parallel()
	dir := fstest.MapFS{
		"a.parquet":                {Data: []byte("1")},
		"a.parquet.merge-skip":     {Data: []byte("too-large")},
		"b.parquet.merge-attempts": {Data: []byte("3")},
		"c.duckdb.merge-skip":      {Data: []byte("1")},
		"d.parquet.compacted":      {Data: []byte("1")},
	}
	entries, err := fs.ReadDir(dir, ".")
	if err != nil {
		t.Fatal(err)
	}
	got := MergeSkipSet(entries)
	want := map[string]struct{}{"a.parquet": {}, "c.duckdb": {}}
	if len(got) != len(want) {
		t.Fatalf("MergeSkipSet = %v, want %v", got, want)
	}
	for name := range want {
		if _, ok := got[name]; !ok {
			t.Fatalf("MergeSkipSet missing %q: %v", name, got)
		}
	}
}

func TestMergeSkipSetEmptyListing(t *testing.T) {
	t.Parallel()
	if got := MergeSkipSet(nil); len(got) != 0 {
		t.Fatalf("MergeSkipSet(nil) = %v, want empty", got)
	}
}

func TestRemoveSidecarsUnlinksBothMarkers(t *testing.T) {
	dir := t.TempDir()
	seg := filepath.Join(dir, "a.parquet")
	if err := os.WriteFile(seg, []byte("seg"), 0o600); err != nil {
		t.Fatal(err)
	}
	skip := MergeSkipMarker(seg)
	attempts := MergeAttemptsMarker(seg)
	if err := os.WriteFile(skip, []byte("skip"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(attempts, []byte("3"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RemoveSidecars(seg); err != nil {
		t.Fatalf("RemoveSidecars: %v", err)
	}
	if _, err := os.Stat(skip); !os.IsNotExist(err) {
		t.Fatal("merge-skip sidecar must be unlinked")
	}
	if _, err := os.Stat(attempts); !os.IsNotExist(err) {
		t.Fatal("merge-attempts sidecar must be unlinked")
	}
	if _, err := os.Stat(seg); err != nil {
		t.Fatal("segment file must remain")
	}
}

func TestRemoveSidecarsMissingIsSuccess(t *testing.T) {
	dir := t.TempDir()
	seg := filepath.Join(dir, "a.parquet")
	if err := os.WriteFile(seg, []byte("seg"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RemoveSidecars(seg); err != nil {
		t.Fatalf("missing sidecars: %v", err)
	}
	if err := RemoveSidecars(filepath.Join(dir, "gone.parquet")); err != nil {
		t.Fatalf("missing segment: %v", err)
	}
}
