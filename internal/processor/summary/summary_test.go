package summary

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
	"github.com/prism-utils/prism/internal/obs"
)

// sampleBatch builds a (__name__ string, value float64) batch.
func sampleBatch(mem memory.Allocator, names []string, vals []float64) data.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__name__", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	nb := array.NewStringBuilder(mem)
	vb := array.NewFloat64Builder(mem)
	defer nb.Release()
	defer vb.Release()
	nb.AppendValues(names, nil)
	vb.AppendValues(vals, nil)
	na, va := nb.NewArray(), vb.NewArray()
	defer na.Release()
	defer va.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{na, va}, int64(len(names)))
	return data.NewRecordBatch("t", rec)
}

func newProc(t *testing.T, mem memory.Allocator, cfg *Config) *processor {
	t.Helper()
	f := NewFactory()
	c, err := f.Create(cfg, component.Settings{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	p := c.(*processor)
	if err := p.Start(context.Background(), obs.NewHostWithAllocator(nil, mem)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return p
}

func TestProcess_GroupByCountSumAvg(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newProc(t, mem, &Config{GroupBy: []string{"__name__"}, Aggregates: []string{"count", "sum:value", "avg:value"}})

	in := sampleBatch(mem,
		[]string{"a", "b", "a", "a"},
		[]float64{1, 10, 2, 3},
	)
	out, err := p.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	defer out.Release()

	rec := out.Record()
	if out.Len() != 2 {
		t.Fatalf("groups = %d, want 2", out.Len())
	}
	// Sorted by key: "a" then "b".
	names := rec.Column(0).(*array.String)
	count := rec.Column(1).(*array.Int64)
	sumv := rec.Column(2).(*array.Float64)
	avgv := rec.Column(3).(*array.Float64)
	if names.Value(0) != "a" || count.Value(0) != 3 || sumv.Value(0) != 6 || avgv.Value(0) != 2 {
		t.Fatalf("group a: name=%q count=%d sum=%v avg=%v", names.Value(0), count.Value(0), sumv.Value(0), avgv.Value(0))
	}
	if names.Value(1) != "b" || count.Value(1) != 1 || sumv.Value(1) != 10 {
		t.Fatalf("group b: name=%q count=%d sum=%v", names.Value(1), count.Value(1), sumv.Value(1))
	}
}

func TestProcess_Percentile(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newProc(t, mem, &Config{GroupBy: []string{"__name__"}, Aggregates: []string{"p95:value", "max:value", "min:value"}})

	vals := make([]float64, 100)
	names := make([]string, 100)
	for i := range vals {
		vals[i] = float64(i + 1) // 1..100
		names[i] = "g"
	}
	out, err := p.Process(context.Background(), sampleBatch(mem, names, vals))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	defer out.Release()
	rec := out.Record()
	p95 := rec.Column(1).(*array.Float64).Value(0)
	max := rec.Column(2).(*array.Float64).Value(0)
	min := rec.Column(3).(*array.Float64).Value(0)
	if p95 != 95 || max != 100 || min != 1 {
		t.Fatalf("p95=%v max=%v min=%v, want 95/100/1", p95, max, min)
	}
}

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{Aggregates: nil}).Validate(); err == nil {
		t.Fatal("empty aggregates should be invalid")
	}
	if err := (&Config{Aggregates: []string{"bogus:x"}}).Validate(); err == nil {
		t.Fatal("unknown function should be invalid")
	}
	if err := (&Config{Aggregates: []string{"count", "p99:latency"}}).Validate(); err != nil {
		t.Fatalf("valid aggregates rejected: %v", err)
	}
}
