package json

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

func TestParse_Objects(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem)

	raw := data.RawBatch{Source: "app", Records: [][]byte{
		[]byte(`{"level":"info","code":200,"ratio":0.5,"ok":true}`),
		[]byte(`{"level":"warn","code":404,"ratio":1.5,"ok":false,"extra":{"a":1}}`),
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
	code := rec.Column(rec.Schema().FieldIndices("code")[0])
	if code.DataType().ID() != arrow.INT64 {
		t.Fatalf("code type = %s, want int64", code.DataType())
	}
	ratio := rec.Column(rec.Schema().FieldIndices("ratio")[0])
	if ratio.DataType().ID() != arrow.FLOAT64 {
		t.Fatalf("ratio type = %s, want float64", ratio.DataType())
	}
	// nested object flattened to JSON text in a string column
	extra := rec.Column(rec.Schema().FieldIndices("extra")[0]).(*array.String)
	if extra.IsNull(0) != true || extra.Value(1) != `{"a":1}` {
		t.Fatalf("extra col = null?%v %q", extra.IsNull(0), extra.Value(1))
	}
}

func TestParse_NonObjectErrors(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem)
	raw := data.RawBatch{Source: "app", Records: [][]byte{[]byte(`["not","an","object"]`)}}
	if _, err := p.Parse(context.Background(), raw); err == nil {
		t.Fatal("expected error parsing a non-object JSON record")
	}
}
