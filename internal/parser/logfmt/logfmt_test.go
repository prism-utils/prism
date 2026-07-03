package logfmt

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/obs"
)

func newParser(t *testing.T, mem memory.Allocator) *parser {
	t.Helper()
	f := NewFactory()
	c, err := f.Create(f.DefaultConfig(), component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	p := c.(*parser)
	if err := p.Start(context.Background(), obs.NewHostWithAllocator(nil, mem)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return p
}

func colByName(t *testing.T, rec arrow.RecordBatch, name string) arrow.Array {
	t.Helper()
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		t.Fatalf("column %q not found in schema %v", name, rec.Schema())
	}
	return rec.Column(idx[0])
}

func TestParse_KeyValuesAndTypes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem)

	raw := data.RawBatch{Source: "app", Records: [][]byte{
		[]byte(`level=info msg="user logged in" status=200 ok=true`),
		[]byte(`level=error msg="db down" status=500 ok=false`),
	}}
	rb, err := p.Parse(context.Background(), raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer rb.Release()
	rec := rb.Record()
	if rb.Len() != 2 {
		t.Fatalf("rows = %d, want 2", rb.Len())
	}
	// status is all-integer -> Int64; ok is all-bool -> Boolean; level/msg string.
	if colByName(t, rec, "status").DataType().ID() != arrow.INT64 {
		t.Fatalf("status type = %s, want int64", colByName(t, rec, "status").DataType())
	}
	if colByName(t, rec, "ok").DataType().ID() != arrow.BOOL {
		t.Fatalf("ok type = %s, want bool", colByName(t, rec, "ok").DataType())
	}
	level := colByName(t, rec, "level").(*array.String)
	if level.Value(0) != "info" || level.Value(1) != "error" {
		t.Fatalf("level = %q,%q", level.Value(0), level.Value(1))
	}
	msg := colByName(t, rec, "msg").(*array.String)
	if msg.Value(0) != "user logged in" {
		t.Fatalf("msg[0] = %q, want 'user logged in'", msg.Value(0))
	}
}

func TestParse_MissingKeyIsNull(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem)

	raw := data.RawBatch{Source: "app", Records: [][]byte{
		[]byte(`a=1 b=2`),
		[]byte(`a=3`),
	}}
	rb, err := p.Parse(context.Background(), raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer rb.Release()
	b := colByName(t, rb.Record(), "b")
	if !b.IsNull(1) {
		t.Fatal("missing key b in row 1 should be null")
	}
}
