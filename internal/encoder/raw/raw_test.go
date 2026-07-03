package raw

import (
	"context"
	"testing"

	"github.com/elk-utilities/prism/internal/data"
)

func TestEncode_NewlineDelimits(t *testing.T) {
	in := data.RecordBatch{Records: [][]byte{[]byte("x"), []byte("y"), []byte("z")}}
	block, err := encoder{}.Encode(context.Background(), in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got, want := string(block.Bytes), "x\ny\nz\n"; got != want {
		t.Fatalf("bytes: got %q, want %q", got, want)
	}
	if block.Rows != 3 {
		t.Fatalf("rows: got %d, want 3", block.Rows)
	}
	if block.Format != Type {
		t.Fatalf("format: got %q, want %q", block.Format, Type)
	}
}

func TestEncode_Empty(t *testing.T) {
	block, err := encoder{}.Encode(context.Background(), data.RecordBatch{})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if block.Rows != 0 || len(block.Bytes) != 0 {
		t.Fatalf("empty batch: got rows=%d bytes=%d, want 0/0", block.Rows, len(block.Bytes))
	}
}
