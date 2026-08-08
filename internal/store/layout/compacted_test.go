package layout

import (
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"
)

func TestCompactedMarkerNaming(t *testing.T) {
	t.Parallel()
	seg := filepath.Join("data", "tenant", "tiers", "L0", "1786140844863329878-aaaaaaaa.parquet")
	marker := CompactedMarker(seg)
	if want := seg + ".compacted"; marker != want {
		t.Fatalf("CompactedMarker = %q, want %q", marker, want)
	}
	if !IsCompactedMarker(filepath.Base(marker)) {
		t.Fatalf("IsCompactedMarker(%q) = false, want true", filepath.Base(marker))
	}
	got, ok := CompactedSegmentName(filepath.Base(marker))
	if !ok || got != filepath.Base(seg) {
		t.Fatalf("CompactedSegmentName(%q) = %q,%v, want %q,true", filepath.Base(marker), got, ok, filepath.Base(seg))
	}
}

func TestCompactedMarkerRejectsNonMarkers(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"a.parquet",
		"a.parquet.compacted.tmp",
		".compacted",
		"",
		"compacted",
	} {
		if IsCompactedMarker(name) {
			t.Fatalf("IsCompactedMarker(%q) = true, want false", name)
		}
		if _, ok := CompactedSegmentName(name); ok {
			t.Fatalf("CompactedSegmentName(%q) reported a segment, want none", name)
		}
	}
}

func TestCompactedSetNamesRetiredSegmentsOnly(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"a.parquet":               {Data: []byte("x")},
		"a.parquet.compacted":     {Data: []byte("1")},
		"b.parquet":               {Data: []byte("x")},
		"c.duckdb":                {Data: []byte("x")},
		"c.duckdb.compacted":      {Data: []byte("1")},
		"d.parquet.compacted.tmp": {Data: []byte("1")},
	}
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		t.Fatal(err)
	}
	got := CompactedSet(entries)
	want := map[string]struct{}{"a.parquet": {}, "c.duckdb": {}}
	if len(got) != len(want) {
		t.Fatalf("CompactedSet = %v, want %v", got, want)
	}
	for name := range want {
		if _, ok := got[name]; !ok {
			t.Fatalf("CompactedSet = %v, want %v", got, want)
		}
	}
}

func TestCompactedSetEmptyListing(t *testing.T) {
	t.Parallel()
	if got := CompactedSet(nil); len(got) != 0 {
		t.Fatalf("CompactedSet(nil) = %v, want empty", got)
	}
}
