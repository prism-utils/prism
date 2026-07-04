package dir

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/data"
)

// When a block carries pipeline/branch/window provenance, the file name encodes
// the time range so a consumer can select files by range without opening them:
//
//	<pipeline>-<phase>-<startUTC>-<endUTC>-<seq>.parquet
func TestConsume_TimeRangeName(t *testing.T) {
	root := t.TempDir()
	out := &Output{cfg: Config{Dir: root}}
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	start := time.Date(2026, 7, 4, 0, 11, 22, 500000000, time.UTC)
	end := start.Add(3 * time.Second)
	block := data.EncodedBlock{
		Format: "parquet", Bytes: []byte("PAR1"), Rows: 3,
		Meta: &data.BlockMeta{
			Pipeline: "metrics", Branch: "raw",
			Window: data.TimeWindow{Start: start, End: end},
		},
	}
	if err := out.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 {
		t.Fatalf("wrote %d files, want 1", len(entries))
	}
	name := entries[0].Name()
	if !strings.HasPrefix(name, "metrics-raw-") || !strings.HasSuffix(name, "-1.parquet") {
		t.Fatalf("name %q does not follow <pipeline>-<phase>-<start>-<end>-<seq>.parquet", name)
	}
	if strings.ContainsAny(name, ":/ ") {
		t.Fatalf("name %q is not filesystem-safe", name)
	}
	// The window bounds must be embedded and time-sortable (start <= end).
	s, e := windowFromName(t, name)
	if s > e {
		t.Fatalf("start %q not <= end %q in %q", s, e, name)
	}
}

// windowFromName pulls the two timestamp components out of a range file name.
func windowFromName(t *testing.T, name string) (string, string) {
	t.Helper()
	base := strings.TrimSuffix(name, ".parquet")
	parts := strings.Split(base, "-")
	if len(parts) < 5 {
		t.Fatalf("name %q lacks the 5 range components", name)
	}
	// metrics - raw - <start> - <end> - <seq>
	return parts[len(parts)-3], parts[len(parts)-2]
}

func TestConsume_WritesOneFilePerBlock(t *testing.T) {
	root := t.TempDir()
	out := &Output{cfg: Config{Dir: filepath.Join(root, "metrics"), Prefix: "m-"}}
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i, payload := range [][]byte{[]byte("PAR1a"), []byte("PAR1b")} {
		if err := out.Consume(context.Background(), data.EncodedBlock{Format: "parquet", Bytes: payload, Rows: i + 1}); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}

	entries, err := os.ReadDir(filepath.Join(root, "metrics"))
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("wrote %d files, want 2", len(entries))
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".parquet" {
			t.Fatalf("file %q lacks .parquet extension", e.Name())
		}
		// No leftover temp files must remain visible.
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("temp file leaked: %q", e.Name())
		}
	}
}

func TestConsume_EmptyBlockWritesNothing(t *testing.T) {
	root := t.TempDir()
	out := &Output{cfg: Config{Dir: root}}
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := out.Consume(context.Background(), data.EncodedBlock{Format: "json", Rows: 0}); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 0 {
		t.Fatalf("empty block produced %d files, want 0", len(entries))
	}
}

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{}).Validate(); err == nil {
		t.Fatal("missing dir should be invalid")
	}
}
