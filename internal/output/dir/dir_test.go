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

// A prefix is preserved ahead of the range-encoded name.
func TestConsume_TimeRangeName_WithPrefix(t *testing.T) {
	root := t.TempDir()
	out := &Output{cfg: Config{Dir: root, Prefix: "m-"}}
	_ = out.Start(context.Background(), nil)
	start := time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
	block := data.EncodedBlock{
		Format: "parquet", Bytes: []byte("PAR1"), Rows: 1,
		Meta: &data.BlockMeta{Pipeline: "metrics", Branch: "raw", Window: data.TimeWindow{Start: start, End: start.Add(time.Second)}},
	}
	if err := out.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), "m-metrics-raw-") {
		t.Fatalf("name %v missing prefix", names(entries))
	}
}

// Without window provenance the output falls back to the legacy <nanos>-<seq>
// name so it never produces a malformed range name. Covers nil Meta and a Meta
// with zero window bounds.
func TestConsume_LegacyFallbackWhenNoWindow(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta *data.BlockMeta
	}{
		{"nil meta", nil},
		{"zero window", &data.BlockMeta{Pipeline: "p", Branch: "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			out := &Output{cfg: Config{Dir: root}}
			_ = out.Start(context.Background(), nil)
			block := data.EncodedBlock{Format: "parquet", Bytes: []byte("X"), Rows: 1, Meta: tc.meta}
			if err := out.Consume(context.Background(), block); err != nil {
				t.Fatalf("Consume: %v", err)
			}
			entries, _ := os.ReadDir(root)
			if len(entries) != 1 {
				t.Fatalf("wrote %d files, want 1", len(entries))
			}
			n := entries[0].Name()
			if strings.Contains(n, "T") || !strings.HasSuffix(n, ".parquet") {
				t.Fatalf("legacy fallback expected, got %q", n)
			}
		})
	}
}

// A restart resets seq to 0; a deterministic time-range name for the same window
// must not overwrite the file written by the previous run.
func TestConsume_NoOverwriteOnRestart(t *testing.T) {
	root := t.TempDir()
	start := time.Date(2026, 7, 4, 1, 2, 3, 0, time.UTC)
	block := data.EncodedBlock{
		Format: "parquet", Bytes: []byte("PAR1"), Rows: 1,
		Meta: &data.BlockMeta{Pipeline: "logs", Branch: "raw", Window: data.TimeWindow{Start: start, End: start.Add(time.Second)}},
	}
	// First "process".
	out1 := &Output{cfg: Config{Dir: root}}
	_ = out1.Start(context.Background(), nil)
	if err := out1.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume#1: %v", err)
	}
	// Second "process" (fresh Output, seq back to 0) with an identical window.
	out2 := &Output{cfg: Config{Dir: root}}
	_ = out2.Start(context.Background(), nil)
	if err := out2.Consume(context.Background(), data.EncodedBlock{Format: "parquet", Bytes: []byte("PAR2"), Rows: 1, Meta: block.Meta}); err != nil {
		t.Fatalf("Consume#2: %v", err)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 2 {
		t.Fatalf("restart overwrote a file: got %d files %v, want 2", len(entries), names(entries))
	}
}

func names(entries []os.DirEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Name()
	}
	return out
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
