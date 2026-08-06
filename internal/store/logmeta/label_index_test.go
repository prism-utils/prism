package logmeta

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadLabelIndexQuarantinesCorruptJSON(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-test-apps"
	logs := filepath.Join(dir, tenant, "logs")
	if err := os.MkdirAll(logs, 0o750); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(logs, labelIndexName)
	// Valid object followed by garbage — Go reports "invalid character after top-level value".
	corrupt := []byte(`{"generation":1,"values":{"job":["prism"]}}` + "\x88MORE")
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}

	idx, err := ReadLabelIndex(dir, tenant)
	if err != nil {
		t.Fatalf("ReadLabelIndex: %v", err)
	}
	if len(idx.Values) != 0 {
		t.Fatalf("expected empty index after quarantine, got %#v", idx.Values)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("corrupt index should be renamed away, stat err=%v", err)
	}
	matches, _ := filepath.Glob(path + ".corrupt-*")
	if len(matches) != 1 {
		t.Fatalf("expected one quarantine file, got %v", matches)
	}
}

func TestWriteLabelIndexRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-test-apps"
	idx := LabelIndex{
		Generation: 7,
		Values:     map[string][]string{"job": {"prism", "other"}, "format": {"none"}},
	}
	if err := WriteLabelIndex(dir, tenant, idx); err != nil {
		t.Fatal(err)
	}
	got, err := ReadLabelIndex(dir, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 7 {
		t.Fatalf("generation=%d", got.Generation)
	}
	if got.Values["job"][0] != "other" || got.Values["job"][1] != "prism" {
		t.Fatalf("job values not sorted: %v", got.Values["job"])
	}
}
