package template

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

func msgBatch(mem memory.Allocator, msgs ...string) data.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "msg", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.AppendValues(msgs, nil)
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, int64(len(msgs)))
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

func TestProcess_MinesStableTemplate(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	p := newProc(t, mem, &Config{Source: "msg"})

	out, err := p.Process(context.Background(), msgBatch(mem,
		"user 4821 logged in from 10.0.0.2",
		"user 12 logged in from 10.9.1.4",
	))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	defer out.Release()
	rec := out.Record()
	idx := rec.Schema().FieldIndices("template")
	if len(idx) == 0 {
		t.Fatalf("no template column added; schema=%v", rec.Schema())
	}
	tmpl := rec.Column(idx[0]).(*array.String)
	const want = "user <*> logged in from <*>"
	if tmpl.Value(0) != want || tmpl.Value(1) != want {
		t.Fatalf("templates = %q, %q; want both %q", tmpl.Value(0), tmpl.Value(1), want)
	}
}

func TestProcess_DisabledIsIdentity(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	no := false
	p := newProc(t, mem, &Config{Source: "msg", Enabled: &no})

	in := msgBatch(mem, "anything 1 2 3")
	out, err := p.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	defer out.Release()
	if len(out.Record().Schema().FieldIndices("template")) != 0 {
		t.Fatal("disabled processor must not add a template column")
	}
}
