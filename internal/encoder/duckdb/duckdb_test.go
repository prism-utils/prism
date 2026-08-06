//go:build cgo

package duckdb_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/marcboeker/go-duckdb/v2"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/duckdbfile"
	encoderduckdb "github.com/elk-utilities/prism/internal/encoder/duckdb"
)

func sampleBatch(mem memory.Allocator) data.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__name__", Type: arrow.BinaryTypes.String},
		{Name: "labels", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "timestamp_ms", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	nb := array.NewStringBuilder(mem)
	lb := array.NewStringBuilder(mem)
	vb := array.NewFloat64Builder(mem)
	tb := array.NewInt64Builder(mem)
	nb.AppendValues([]string{"up", "up"}, nil)
	lb.AppendValues([]string{`{"job":"a"}`, `{"job":"b"}`}, nil)
	vb.AppendValues([]float64{1, 0}, nil)
	tb.AppendValues([]int64{100, 200}, nil)
	cols := []arrow.Array{nb.NewArray(), lb.NewArray(), vb.NewArray(), tb.NewArray()}
	nb.Release()
	lb.Release()
	vb.Release()
	tb.Release()
	rec := array.NewRecordBatch(schema, cols, 2)
	for _, c := range cols {
		c.Release()
	}
	return data.NewRecordBatch("t", rec)
}

func TestEncode_DuckDBRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	enc, err := encoderduckdb.NewFactory().Create(&encoderduckdb.Config{}, component.Settings{})
	if err != nil {
		t.Fatal(err)
	}
	block, err := enc.Encode(context.Background(), sampleBatch(mem))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if block.Format != encoderduckdb.Type || block.Rows != 2 || len(block.Bytes) == 0 {
		t.Fatalf("block format=%q rows=%d size=%d", block.Format, block.Rows, len(block.Bytes))
	}
	if !duckdbfile.HasMagic(block.Bytes) {
		t.Fatal("encoded bytes missing DuckDB magic")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "w.duckdb")
	if err := os.WriteFile(path, block.Bytes, 0o600); err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	attach := "ATTACH '" + filepath.ToSlash(path) + "' AS w (READ_ONLY)"
	if _, err := db.ExecContext(ctx, attach); err != nil {
		t.Fatalf("attach: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM w."+duckdbfile.Table).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
	var name string
	var val float64
	if err := db.QueryRowContext(ctx,
		`SELECT "__name__", value FROM w.`+duckdbfile.Table+` ORDER BY timestamp_ms LIMIT 1`).
		Scan(&name, &val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if name != "up" || val != 1 {
		t.Fatalf("got name=%q val=%v", name, val)
	}
}

func TestEncode_EmptyBatch(t *testing.T) {
	enc, err := encoderduckdb.NewFactory().Create(&encoderduckdb.Config{}, component.Settings{})
	if err != nil {
		t.Fatal(err)
	}
	block, err := enc.Encode(context.Background(), data.RecordBatch{})
	if err != nil {
		t.Fatal(err)
	}
	if block.Format != encoderduckdb.Type || block.Rows != 0 || len(block.Bytes) != 0 {
		t.Fatalf("empty block = %+v", block)
	}
}
