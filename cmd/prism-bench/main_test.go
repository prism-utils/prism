package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// writeParquet builds a one-row-group Parquet file from the given schema and
// columns, for exercising the harness's footer/summary readers.
func writeParquet(t *testing.T, path string, schema *arrow.Schema, cols []arrow.Array, rows int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer func() { _ = f.Close() }()
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	w, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	rec := array.NewRecordBatch(schema, cols, rows)
	defer rec.Release()
	if err := w.Write(rec); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func templateSummaryFile(t *testing.T, dir string) string {
	t.Helper()
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "template", Type: arrow.BinaryTypes.String},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	tb := array.NewStringBuilder(mem)
	defer tb.Release()
	cb := array.NewInt64Builder(mem)
	defer cb.Release()
	tb.AppendValues([]string{"user <*> logged in", "GET <*> 200", "cache miss <*>"}, nil)
	cb.AppendValues([]int64{40, 25, 10}, nil)
	ta := tb.NewArray()
	defer ta.Release()
	ca := cb.NewArray()
	defer ca.Release()
	path := filepath.Join(dir, "s-logs-summary-1.parquet")
	writeParquet(t, path, schema, []arrow.Array{ta, ca}, 3)
	return path
}

// A summary Parquet with template+count columns yields per-template counts.
func TestTemplateSummary_ReadsCounts(t *testing.T) {
	path := templateSummaryFile(t, t.TempDir())
	got, ok, err := templateSummary(path)
	if err != nil {
		t.Fatalf("templateSummary: %v", err)
	}
	if !ok {
		t.Fatal("expected file to be recognized as a template summary")
	}
	want := map[string]int64{
		"user <*> logged in": 40,
		"GET <*> 200":        25,
		"cache miss <*>":     10,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d templates, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("template %q = %d, want %d", k, got[k], v)
		}
	}
}

// A Parquet without both template+count columns is not a template summary.
func TestTemplateSummary_NonSummaryFile(t *testing.T) {
	dir := t.TempDir()
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__name__", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	nb := array.NewStringBuilder(mem)
	defer nb.Release()
	vb := array.NewFloat64Builder(mem)
	defer vb.Release()
	nb.AppendValues([]string{"http_requests_total"}, nil)
	vb.AppendValues([]float64{7}, nil)
	na := nb.NewArray()
	defer na.Release()
	va := vb.NewArray()
	defer va.Release()
	path := filepath.Join(dir, "m-metrics-raw-1.parquet")
	writeParquet(t, path, schema, []arrow.Array{na, va}, 1)

	_, ok, err := templateSummary(path)
	if err != nil {
		t.Fatalf("templateSummary: %v", err)
	}
	if ok {
		t.Fatal("metrics parquet should not be treated as a template summary")
	}
}

// A template-phase Parquet (message + template, plus a stray count field) must
// NOT be read as a summary: it still carries the message column, and reading it
// as a summary would both skew metrics and materialize a full-width table.
func TestTemplateSummary_TemplatePhaseWithCountIsNotSummary(t *testing.T) {
	dir := t.TempDir()
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "template", Type: arrow.BinaryTypes.String},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64}, // e.g. a parsed field named count
	}, nil)
	mb := array.NewStringBuilder(mem)
	defer mb.Release()
	tb := array.NewStringBuilder(mem)
	defer tb.Release()
	cb := array.NewInt64Builder(mem)
	defer cb.Release()
	mb.AppendValues([]string{"got 5 items", "got 9 items"}, nil)
	tb.AppendValues([]string{"got <*> items", "got <*> items"}, nil)
	cb.AppendValues([]int64{5, 9}, nil)
	ma := mb.NewArray()
	defer ma.Release()
	ta := tb.NewArray()
	defer ta.Release()
	ca := cb.NewArray()
	defer ca.Release()
	path := filepath.Join(dir, "l-logs-template-1.parquet")
	writeParquet(t, path, schema, []arrow.Array{ma, ta, ca}, 2)

	_, ok, err := templateSummary(path)
	if err != nil {
		t.Fatalf("templateSummary: %v", err)
	}
	if ok {
		t.Fatal("template-phase Parquet (has message) must not be a summary")
	}
}

// inspectOutputs aggregates template counts across summary Parquet files and
// still counts raw/template phases as plain Parquet rows.
func TestInspectOutputs_AggregatesTemplateMetrics(t *testing.T) {
	root := t.TempDir()
	summaryDir := filepath.Join(root, "summary")
	rawDir := filepath.Join(root, "raw")
	if err := os.MkdirAll(summaryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		t.Fatal(err)
	}
	templateSummaryFile(t, summaryDir)

	// a raw parquet (message column only) contributes rows but no templates
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "message", Type: arrow.BinaryTypes.String},
	}, nil)
	mb := array.NewStringBuilder(mem)
	defer mb.Release()
	mb.AppendValues([]string{"a", "b", "c", "d"}, nil)
	ma := mb.NewArray()
	defer ma.Release()
	writeParquet(t, filepath.Join(rawDir, "l-logs-raw-1.parquet"), schema, []arrow.Array{ma}, 4)

	rep := &Report{}
	if err := inspectOutputs(root, rep); err != nil {
		t.Fatalf("inspectOutputs: %v", err)
	}
	if rep.TemplateGroups != 3 {
		t.Fatalf("template groups = %d, want 3", rep.TemplateGroups)
	}
	if rep.TemplateCountTotal != 75 {
		t.Fatalf("template count total = %d, want 75", rep.TemplateCountTotal)
	}
	if len(rep.TopTemplates) == 0 || rep.TopTemplates[0].Count != 40 {
		t.Fatalf("top template = %+v, want highest count 40 first", rep.TopTemplates)
	}
	if rep.ParquetFiles != 2 {
		t.Fatalf("parquet files = %d, want 2", rep.ParquetFiles)
	}
}
