package dir

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/data"
)

func TestConsume_DuckDBExtension(t *testing.T) {
	root := t.TempDir()
	out := &Output{cfg: Config{Dir: root}}
	if err := out.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	block := data.EncodedBlock{Format: "duckdb", Bytes: []byte("not-empty"), Rows: 1}
	if err := out.Consume(context.Background(), block); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".duckdb") {
		t.Fatalf("name %v want *.duckdb", names(entries))
	}
}
