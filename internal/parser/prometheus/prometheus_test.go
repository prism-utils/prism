package prometheus

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/obs"
)

func newParser(t *testing.T, mem memory.Allocator) *parser {
	t.Helper()
	pf := NewFactory()
	p, err := pf.Create(pf.DefaultConfig(), component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	pp := p.(*parser)
	if err := pp.Start(context.Background(), obs.NewHostWithAllocator(nil, mem)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return pp
}

func TestParse_ExpositionLines(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem)

	raw := data.RawBatch{Source: "t", Records: [][]byte{
		[]byte("# HELP http_requests_total total requests"),
		[]byte("# TYPE http_requests_total counter"),
		[]byte(`http_requests_total{method="post",code="200"} 1027 1395066363000`),
		[]byte("go_goroutines 42"),
		[]byte(""),
		[]byte(`weird{msg="a b}c",x="y"} 3.5`),
	}}
	rb, err := p.Parse(context.Background(), raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer rb.Release()

	if rb.Len() != 3 {
		t.Fatalf("rows = %d, want 3 (comments/blanks skipped)", rb.Len())
	}
	rec := rb.Record()
	names := rec.Column(0).(*array.String)
	labels := rec.Column(1).(*array.String)
	vals := rec.Column(2).(*array.Float64)
	ts := rec.Column(3).(*array.Int64)

	if names.Value(0) != "http_requests_total" || labels.Value(0) != `method="post",code="200"` {
		t.Fatalf("row0 name/labels = %q / %q", names.Value(0), labels.Value(0))
	}
	if vals.Value(0) != 1027 || ts.Value(0) != 1395066363000 {
		t.Fatalf("row0 value/ts = %v / %d", vals.Value(0), ts.Value(0))
	}
	if names.Value(1) != "go_goroutines" || vals.Value(1) != 42 || ts.Value(1) != 0 {
		t.Fatalf("row1 = %q %v %d", names.Value(1), vals.Value(1), ts.Value(1))
	}
	// A '}' inside a quoted label value must not end the block early.
	if labels.Value(2) != `msg="a b}c",x="y"` || vals.Value(2) != 3.5 {
		t.Fatalf("row2 labels/value = %q / %v", labels.Value(2), vals.Value(2))
	}
}

func TestParse_SpecialFloats(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem)

	raw := data.RawBatch{Source: "t", Records: [][]byte{
		[]byte("m_nan NaN"),
		[]byte("m_inf +Inf"),
	}}
	rb, err := p.Parse(context.Background(), raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer rb.Release()
	vals := rb.Record().Column(2).(*array.Float64)
	if !math.IsNaN(vals.Value(0)) || !math.IsInf(vals.Value(1), 1) {
		t.Fatalf("special floats not parsed: %v %v", vals.Value(0), vals.Value(1))
	}
}

func TestParse_MalformedErrors(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem)

	for _, line := range []string{
		`no_value`,
		`bad_value abc`,
		`unterminated{a="b" 1`,
	} {
		raw := data.RawBatch{Source: "t", Records: [][]byte{[]byte(line)}}
		if _, err := p.Parse(context.Background(), raw); err == nil {
			t.Fatalf("expected error for %q", line)
		}
	}
}
