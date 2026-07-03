package dir

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/elk-utilities/prism/internal/data"
)

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
