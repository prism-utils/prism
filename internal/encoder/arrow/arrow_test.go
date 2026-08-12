package arrow

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

func sampleBatch(mem memory.Allocator) data.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	ib := array.NewInt64Builder(mem)
	defer ib.Release()
	sb.Append("a")
	sb.Append("b")
	ib.Append(1)
	ib.Append(2)
	sc := sb.NewArray()
	defer sc.Release()
	ic := ib.NewArray()
	defer ic.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{sc, ic}, 2)
	return data.NewRecordBatch("t", rec)
}

// The encoder emits an Arrow IPC stream that round-trips to the same rows.
func TestEncode_IPCRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	enc, err := factory{}.Create(&Config{}, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	block, err := enc.Encode(context.Background(), sampleBatch(mem))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if block.Format != Type || block.Rows != 2 || len(block.Bytes) == 0 {
		t.Fatalf("block = %+v", block)
	}
	rdr, err := ipc.NewReader(bytes.NewReader(block.Bytes), ipc.WithAllocator(mem))
	if err != nil {
		t.Fatalf("ipc reader: %v", err)
	}
	defer rdr.Release()
	if !rdr.Next() {
		t.Fatal("no record in IPC stream")
	}
	rec := rdr.RecordBatch()
	if rec.NumRows() != 2 || rec.NumCols() != 2 {
		t.Fatalf("roundtrip shape = %dx%d, want 2x2", rec.NumRows(), rec.NumCols())
	}
	if rec.Schema().Field(0).Name != "name" {
		t.Fatalf("schema drifted: %v", rec.Schema())
	}
}

// An empty (zero-row) batch yields an empty block, not an error.
func TestEncode_EmptyBatch(t *testing.T) {
	enc, _ := factory{}.Create(&Config{}, component.Settings{})
	block, err := enc.Encode(context.Background(), data.RecordBatch{})
	if err != nil {
		t.Fatalf("Encode empty: %v", err)
	}
	if block.Rows != 0 || len(block.Bytes) != 0 {
		t.Fatalf("empty batch produced %+v", block)
	}
}
