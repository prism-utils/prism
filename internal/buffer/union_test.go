package buffer_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/buffer"
	"github.com/prism-utils/prism/internal/data"
)

// oneRow builds a single-row batch from an ordered set of (name, value) columns
// where value is int64 or string, so tests can craft heterogeneous windows.
func oneRow(mem memory.Allocator, cols []string, vals []any) data.RecordBatch {
	fields := make([]arrow.Field, len(cols))
	arrs := make([]arrow.Array, len(cols))
	for i, name := range cols {
		switch v := vals[i].(type) {
		case int64:
			fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
			b := array.NewInt64Builder(mem)
			b.Append(v)
			arrs[i] = b.NewArray()
			b.Release()
		default:
			fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}
			b := array.NewStringBuilder(mem)
			b.Append(v.(string))
			arrs[i] = b.NewArray()
			b.Release()
		}
	}
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), arrs, 1)
	for _, a := range arrs {
		a.Release()
	}
	return data.NewRecordBatch("s", rec)
}

func colByName(t *testing.T, rec arrow.RecordBatch, name string) arrow.Array {
	t.Helper()
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		t.Fatalf("column %q missing; schema=%v", name, rec.Schema())
	}
	return rec.Column(idx[0])
}

// A window whose batches have different key sets must still flush: the union
// schema is produced with nulls where a batch lacked a column.
func TestAccumulator_UnionSchemaMissingColumns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	acc := buffer.New(buffer.Config{MaxRows: 100}, mem)

	t0 := time.Unix(0, 0)
	acc.Add(oneRow(mem, []string{"level", "latency"}, []any{"info", int64(5)}), t0)
	acc.Add(oneRow(mem, []string{"level"}, []any{"error"}), t0)

	win, ok := mustFlush(t, acc)
	if !ok || win.Len() != 2 {
		win.Release()
		t.Fatalf("window ok=%v rows=%d, want 2", ok, win.Len())
	}
	rec := win.Record()
	lat := colByName(t, rec, "latency")
	if lat.IsNull(0) || !lat.IsNull(1) {
		t.Fatalf("latency nulls wrong: row0null=%v row1null=%v", lat.IsNull(0), lat.IsNull(1))
	}
	win.Release()
	mem.AssertSize(t, 0)
}

// When the same column has different types across batches, the union widens it
// to string so the window still concatenates.
func TestAccumulator_UnionSchemaTypeConflictWidensToString(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	acc := buffer.New(buffer.Config{MaxRows: 100}, mem)

	t0 := time.Unix(0, 0)
	acc.Add(oneRow(mem, []string{"port"}, []any{int64(80)}), t0)
	acc.Add(oneRow(mem, []string{"port"}, []any{"http"}), t0)

	win, ok := mustFlush(t, acc)
	if !ok {
		t.Fatal("expected a window")
	}
	port := colByName(t, win.Record(), "port")
	if port.DataType().ID() != arrow.STRING {
		win.Release()
		t.Fatalf("port type = %s, want string (widened)", port.DataType())
	}
	s := port.(*array.String)
	if s.Value(0) != "80" || s.Value(1) != "http" {
		win.Release()
		t.Fatalf("widened values = %q,%q, want 80,http", s.Value(0), s.Value(1))
	}
	win.Release()
	mem.AssertSize(t, 0)
}
