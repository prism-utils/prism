package gen

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const defaultBatch = 50_000

// WriteMetricsWindows writes contract-v1 metrics parquet files (no ts column)
// under dir, chunked for HTTP ingest windows.
func WriteMetricsWindows(dir string, rows []MetricRow, batch int) ([]string, error) {
	if batch <= 0 {
		batch = defaultBatch
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("gen: mkdir metrics: %w", err)
	}
	var paths []string
	for off := 0; off < len(rows); off += batch {
		end := off + batch
		if end > len(rows) {
			end = len(rows)
		}
		path := filepath.Join(dir, fmt.Sprintf("metrics-%06d.parquet", len(paths)))
		if err := writeMetricsParquet(path, rows[off:end]); err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func writeMetricsParquet(path string, rows []MetricRow) error {
	mem := memory.NewGoAllocator()
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
	defer nb.Release()
	defer lb.Release()
	defer vb.Release()
	defer tb.Release()

	names := make([]string, len(rows))
	labels := make([]string, len(rows))
	values := make([]float64, len(rows))
	tsMs := make([]int64, len(rows))
	for i, r := range rows {
		names[i] = r.Name
		labels[i] = r.Labels
		values[i] = r.Value
		tsMs[i] = r.TimestampMs
	}
	nb.AppendValues(names, nil)
	lb.AppendValues(labels, nil)
	vb.AppendValues(values, nil)
	tb.AppendValues(tsMs, nil)

	na := nb.NewArray()
	la := lb.NewArray()
	va := vb.NewArray()
	ta := tb.NewArray()
	defer na.Release()
	defer la.Release()
	defer va.Release()
	defer ta.Release()

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("gen: create metrics parquet: %w", err)
	}
	defer func() { _ = f.Close() }()

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
	)
	w, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("gen: parquet writer: %w", err)
	}
	rec := array.NewRecordBatch(schema, []arrow.Array{na, la, va, ta}, int64(len(rows)))
	defer rec.Release()
	if err := w.Write(rec); err != nil {
		return fmt.Errorf("gen: write metrics batch: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gen: close metrics parquet: %w", err)
	}
	return nil
}

// WriteLogsTier writes a zstd-compressed logs-shaped parquet segment in row batches.
func WriteLogsTier(path string, rows []LogRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("gen: mkdir logs tier: %w", err)
	}
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "level", Type: arrow.BinaryTypes.String},
		{Name: "service", Type: arrow.BinaryTypes.String},
		{Name: "message", Type: arrow.BinaryTypes.String},
	}, nil)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("gen: create logs parquet: %w", err)
	}
	defer func() { _ = f.Close() }()

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
	)
	w, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("gen: logs parquet writer: %w", err)
	}

	for off := 0; off < len(rows); off += defaultBatch {
		end := off + defaultBatch
		if end > len(rows) {
			end = len(rows)
		}
		if err := writeLogsBatch(w, schema, mem, rows[off:end]); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gen: close logs parquet: %w", err)
	}
	return nil
}

func writeLogsBatch(w *pqarrow.FileWriter, schema *arrow.Schema, mem memory.Allocator, rows []LogRow) error {
	tsb := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	lvb := array.NewStringBuilder(mem)
	svb := array.NewStringBuilder(mem)
	mvb := array.NewStringBuilder(mem)
	defer tsb.Release()
	defer lvb.Release()
	defer svb.Release()
	defer mvb.Release()

	tsVals := make([]arrow.Timestamp, len(rows))
	levels := make([]string, len(rows))
	services := make([]string, len(rows))
	messages := make([]string, len(rows))
	for i, r := range rows {
		tsVals[i] = arrow.Timestamp(r.Ts.UnixMicro())
		levels[i] = r.Level
		services[i] = r.Service
		messages[i] = r.Message
	}
	tsb.AppendValues(tsVals, nil)
	lvb.AppendValues(levels, nil)
	svb.AppendValues(services, nil)
	mvb.AppendValues(messages, nil)

	tsa := tsb.NewArray()
	lva := lvb.NewArray()
	sva := svb.NewArray()
	mva := mvb.NewArray()
	defer tsa.Release()
	defer lva.Release()
	defer sva.Release()
	defer mva.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{tsa, lva, sva, mva}, int64(len(rows)))
	defer rec.Release()
	if err := w.Write(rec); err != nil {
		return fmt.Errorf("gen: write logs batch: %w", err)
	}
	return nil
}
