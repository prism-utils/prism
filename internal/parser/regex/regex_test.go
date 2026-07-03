package regex

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

func newParser(t *testing.T, mem memory.Allocator, pattern string) *parser {
	t.Helper()
	f := NewFactory()
	cfg := &Config{Pattern: pattern}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	c, err := f.Create(cfg, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	p := c.(*parser)
	if err := p.Start(context.Background(), obs.NewHostWithAllocator(nil, mem)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return p
}

func TestParse_NamedGroups(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem, `^(?P<ip>\S+) (?P<method>\S+) (?P<status>\d+)$`)

	raw := data.RawBatch{Source: "access", Records: [][]byte{
		[]byte("10.0.0.1 GET 200"),
		[]byte("10.0.0.2 POST 500"),
	}}
	rb, err := p.Parse(context.Background(), raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defer rb.Release()
	rec := rb.Record()
	status := rec.Column(rec.Schema().FieldIndices("status")[0])
	if status.DataType().ID() != arrow.INT64 {
		t.Fatalf("status type = %s, want int64 (all-numeric)", status.DataType())
	}
	ip := rec.Column(rec.Schema().FieldIndices("ip")[0]).(*array.String)
	if ip.Value(0) != "10.0.0.1" {
		t.Fatalf("ip[0] = %q", ip.Value(0))
	}
}

func TestParse_NoMatchErrors(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newParser(t, mem, `^(?P<n>\d+)$`)
	if _, err := p.Parse(context.Background(), data.RawBatch{Records: [][]byte{[]byte("not-a-number")}}); err == nil {
		t.Fatal("expected error for non-matching line")
	}
}

func TestValidate_RequiresNamedGroup(t *testing.T) {
	if err := (&Config{Pattern: `\d+`}).Validate(); err == nil {
		t.Fatal("pattern without named groups should be invalid")
	}
}
