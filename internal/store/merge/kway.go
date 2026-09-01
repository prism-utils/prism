package merge

import (
	"container/heap"
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const kwayFlushRows = 1024

type kwayCursor struct {
	src      int
	stampNs  int64
	tsCol    int
	eventCol int
	rec      arrow.RecordBatch
	row      int
	fr       *pqarrow.FileReader
	pf       *file.Reader
	rg       int
	nrg      int
}

func (c *kwayCursor) ts() int64 {
	return ingestTSAt(c.rec, c.tsCol, c.row, c.stampNs)
}

type kwayHeap []*kwayCursor

func (h kwayHeap) Len() int { return len(h) }
func (h kwayHeap) Less(i, j int) bool {
	ti, tj := h[i].ts(), h[j].ts()
	if ti != tj {
		return ti < tj
	}
	ei, ej := h[i].eventTs(), h[j].eventTs()
	if ei != ej {
		return ei < ej
	}
	return h[i].src < h[j].src
}

func (c *kwayCursor) eventTs() int64 {
	return eventTSAt(c.rec, c.eventCol, c.row)
}
func (h kwayHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *kwayHeap) Push(x any)   { *h = append(*h, x.(*kwayCursor)) }
func (h *kwayHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

func kwayMergeLogs(dest string, sources []Segment, mem memory.Allocator) error {
	if len(sources) == 0 {
		return fmt.Errorf("log merge: no sources")
	}
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	ctx := context.Background()
	cursors := make([]*kwayCursor, 0, len(sources))
	schemas := make([]*arrow.Schema, 0, len(sources))
	defer func() {
		for _, c := range cursors {
			if c.rec != nil {
				c.rec.Release()
				c.rec = nil
			}
			if c.pf != nil {
				_ = c.pf.Close()
			}
		}
	}()

	for i, s := range sources {
		pf, err := file.OpenParquetFile(s.Path, false)
		if err != nil {
			return err
		}
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
		if err != nil {
			_ = pf.Close()
			return err
		}
		schema, err := fr.Schema()
		if err != nil {
			_ = pf.Close()
			return err
		}
		cursors = append(cursors, &kwayCursor{
			src:      i,
			stampNs:  s.MinTs.UTC().UnixNano(),
			fr:       fr,
			pf:       pf,
			nrg:      pf.NumRowGroups(),
			tsCol:    -1,
			eventCol: -1,
		})
		schemas = append(schemas, schema)
	}

	union := unionLogSchema(schemas)
	tmp := dest + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // G304: dest is a server-owned logs tier path
	if err != nil {
		return err
	}
	success := false
	defer func() {
		_ = out.Close()
		if !success {
			_ = os.Remove(tmp)
		}
	}()
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	writer, err := pqarrow.NewFileWriter(union, out, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}

	h := kwayHeap{}
	for _, c := range cursors {
		if err := c.loadNext(ctx); err != nil {
			_ = writer.Close()
			return err
		}
		if c.rec != nil {
			heap.Push(&h, c)
		}
	}

	b := array.NewRecordBuilder(mem, union)
	defer b.Release()
	pending := 0
	flush := func() error {
		if pending == 0 {
			return nil
		}
		rec := b.NewRecordBatch()
		tbl := array.NewTableFromRecords(union, []arrow.RecordBatch{rec})
		rec.Release()
		chunk := tbl.NumRows()
		if chunk <= 0 {
			chunk = 1
		}
		err := writer.WriteTable(tbl, chunk)
		tbl.Release()
		pending = 0
		return err
	}

	for h.Len() > 0 {
		c := heap.Pop(&h).(*kwayCursor)
		if err := appendProjectedRow(b, union, c); err != nil {
			_ = writer.Close()
			return err
		}
		pending++
		c.row++
		if c.row >= int(c.rec.NumRows()) {
			c.rec.Release()
			c.rec = nil
			if err := c.loadNext(ctx); err != nil {
				_ = writer.Close()
				return err
			}
		}
		if c.rec != nil {
			heap.Push(&h, c)
		}
		if pending >= kwayFlushRows {
			if err := flush(); err != nil {
				_ = writer.Close()
				return err
			}
		}
	}
	if err := flush(); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dest); err != nil {
		return err
	}
	success = true
	return nil
}

func (c *kwayCursor) loadNext(ctx context.Context) error {
	for c.rg < c.nrg {
		tbl, err := c.fr.ReadRowGroups(ctx, parquetColIndices(c.pf), []int{c.rg})
		c.rg++
		if err != nil {
			return err
		}
		if tbl.NumRows() == 0 {
			tbl.Release()
			continue
		}
		tr := array.NewTableReader(tbl, tbl.NumRows())
		if !tr.Next() {
			tr.Release()
			tbl.Release()
			continue
		}
		rec := tr.RecordBatch()
		rec.Retain()
		tr.Release()
		tbl.Release()
		c.rec = rec
		c.row = 0
		c.tsCol = fieldIndex(rec.Schema(), logIngestTSColumn)
		c.eventCol = fieldIndex(rec.Schema(), "ts")
		return nil
	}
	c.rec = nil
	return nil
}

func unionLogSchema(schemas []*arrow.Schema) *arrow.Schema {
	var fields []arrow.Field
	seen := map[string]struct{}{}
	for _, sc := range schemas {
		if sc == nil {
			continue
		}
		for _, f := range sc.Fields() {
			if _, ok := seen[f.Name]; ok {
				continue
			}
			seen[f.Name] = struct{}{}
			fields = append(fields, flattenLogField(&f))
		}
	}
	if _, ok := seen[logIngestTSColumn]; !ok {
		fields = append(fields, arrow.Field{
			Name:     logIngestTSColumn,
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
		})
	}
	return arrow.NewSchema(fields, nil)
}

func flattenLogField(f *arrow.Field) arrow.Field {
	out := *f
	switch out.Type.ID() {
	case arrow.DICTIONARY, arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY:
		out.Type = arrow.BinaryTypes.String
	}
	out.Nullable = true
	return out
}

func fieldIndex(sc *arrow.Schema, name string) int {
	idx := sc.FieldIndices(name)
	if len(idx) == 0 {
		return -1
	}
	return idx[0]
}

func eventTSAt(rec arrow.RecordBatch, colIdx, row int) int64 {
	if rec == nil || colIdx < 0 || row >= int(rec.NumRows()) {
		return 0
	}
	col := rec.Column(colIdx)
	if col.IsNull(row) {
		return 0
	}
	switch c := col.(type) {
	case *array.Timestamp:
		return int64(c.Value(row))
	case *array.Int64:
		return c.Value(row)
	case *array.Int32:
		return int64(c.Value(row))
	default:
		return 0
	}
}

func ingestTSAt(rec arrow.RecordBatch, colIdx, row int, stamp int64) int64 {
	if rec == nil || colIdx < 0 || row >= int(rec.NumRows()) {
		return stamp
	}
	col := rec.Column(colIdx)
	if col.IsNull(row) {
		return stamp
	}
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(row)
	case *array.Int32:
		return int64(c.Value(row))
	default:
		return stamp
	}
}

func appendProjectedRow(b *array.RecordBuilder, union *arrow.Schema, c *kwayCursor) error {
	srcSchema := c.rec.Schema()
	for i, field := range union.Fields() {
		fb := b.Field(i)
		if field.Name == logIngestTSColumn {
			appendInt64(fb, c.ts())
			continue
		}
		srcIdx := fieldIndex(srcSchema, field.Name)
		if srcIdx < 0 {
			fb.AppendNull()
			continue
		}
		appendFromArray(fb, c.rec.Column(srcIdx), c.row)
	}
	return nil
}

func appendInt64(b array.Builder, v int64) {
	if tb, ok := b.(*array.Int64Builder); ok {
		tb.Append(v)
		return
	}
	b.AppendNull()
}

func appendFromArray(b array.Builder, col arrow.Array, row int) {
	if col.IsNull(row) {
		b.AppendNull()
		return
	}
	switch src := col.(type) {
	case *array.String:
		if tb, ok := b.(*array.StringBuilder); ok {
			tb.Append(src.Value(row))
			return
		}
	case *array.LargeString:
		if tb, ok := b.(*array.StringBuilder); ok {
			tb.Append(src.Value(row))
			return
		}
	case *array.Binary:
		if tb, ok := b.(*array.StringBuilder); ok {
			tb.Append(string(src.Value(row)))
			return
		}
	case *array.Int64:
		if tb, ok := b.(*array.Int64Builder); ok {
			tb.Append(src.Value(row))
			return
		}
	case *array.Float64:
		if tb, ok := b.(*array.Float64Builder); ok {
			tb.Append(src.Value(row))
			return
		}
	case *array.Boolean:
		if tb, ok := b.(*array.BooleanBuilder); ok {
			tb.Append(src.Value(row))
			return
		}
	case *array.Timestamp:
		if tb, ok := b.(*array.TimestampBuilder); ok {
			tb.Append(src.Value(row))
			return
		}
	}
	if sb, ok := b.(*array.StringBuilder); ok {
		sb.Append(col.ValueStr(row))
		return
	}
	b.AppendNull()
}
