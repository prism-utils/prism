package json

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/data"
)

func TestEncode_RowObjects(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0) // encoder owns and releases its input

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "avg", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	nb := array.NewStringBuilder(mem)
	cb := array.NewInt64Builder(mem)
	ab := array.NewFloat64Builder(mem)
	nb.AppendValues([]string{"a", "b"}, nil)
	cb.AppendValues([]int64{3, 1}, nil)
	ab.AppendValues([]float64{2.5, 10}, nil)
	cols := []arrow.Array{nb.NewArray(), cb.NewArray(), ab.NewArray()}
	nb.Release()
	cb.Release()
	ab.Release()
	rec := array.NewRecordBatch(schema, cols, 2)
	for _, c := range cols {
		c.Release()
	}
	in := data.NewRecordBatch("t", rec)

	block, err := encoder{}.Encode(context.Background(), in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if block.Rows != 2 || block.Format != Type {
		t.Fatalf("block rows=%d format=%q", block.Rows, block.Format)
	}
	var got []map[string]any
	if err := json.Unmarshal(block.Bytes, &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, block.Bytes)
	}
	if len(got) != 2 || got[0]["name"] != "a" || got[0]["count"].(float64) != 3 || got[0]["avg"].(float64) != 2.5 {
		t.Fatalf("row0 mismatch: %+v", got[0])
	}
}

func TestEncode_Empty(t *testing.T) {
	block, err := encoder{}.Encode(context.Background(), data.RecordBatch{})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if string(block.Bytes) != "[]" || block.Rows != 0 {
		t.Fatalf("empty encode = %q rows=%d, want [] / 0", block.Bytes, block.Rows)
	}
}
