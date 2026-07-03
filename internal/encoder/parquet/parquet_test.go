package parquet

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/elk-utilities/prism/internal/data"
)

func sampleBatch(mem memory.Allocator) data.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__name__", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	nb := array.NewStringBuilder(mem)
	vb := array.NewFloat64Builder(mem)
	nb.AppendValues([]string{"m1", "m2", "m3"}, nil)
	vb.AppendValues([]float64{1.5, 2.5, 3.5}, nil)
	cols := []arrow.Array{nb.NewArray(), vb.NewArray()}
	nb.Release()
	vb.Release()
	rec := array.NewRecordBatch(schema, cols, 3)
	for _, c := range cols {
		c.Release()
	}
	return data.NewRecordBatch("t", rec)
}

func TestEncode_ParquetRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0) // encoder owns and releases its input

	block, err := (&encoder{codec: mustCodec(t, "snappy")}).Encode(context.Background(), sampleBatch(mem))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if block.Rows != 3 || block.Format != Type || len(block.Bytes) == 0 {
		t.Fatalf("block rows=%d format=%q size=%d", block.Rows, block.Format, len(block.Bytes))
	}

	rdr, err := file.NewParquetReader(bytes.NewReader(block.Bytes))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer func() { _ = rdr.Close() }()
	pr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("arrow reader: %v", err)
	}
	tbl, err := pr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	defer tbl.Release()

	if tbl.NumRows() != 3 || tbl.NumCols() != 2 {
		t.Fatalf("round-trip shape rows=%d cols=%d, want 3/2", tbl.NumRows(), tbl.NumCols())
	}
	if got := tbl.Schema().Field(0).Name; got != "__name__" {
		t.Fatalf("col0 name = %q, want __name__", got)
	}
}

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{Compression: "snappy"}).Validate(); err != nil {
		t.Fatalf("snappy should be valid: %v", err)
	}
	if err := (&Config{Compression: "brotli-nope"}).Validate(); err == nil {
		t.Fatal("unknown codec should be invalid")
	}
}

func mustCodec(t *testing.T, name string) compress.Compression {
	t.Helper()
	cc, err := codec(name)
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	return cc
}
