// Package data defines the units of information that flow between pipeline
// stages: RawBatch (pre-parse), RecordBatch (structured, Arrow-backed), and
// EncodedBlock (serialized output).
//
// RecordBatch wraps an Apache Arrow record: a schema plus columnar arrays held
// in poolable allocator buffers (docs/DESIGN.md §5). Ownership is linear —
// whoever receives a RecordBatch calls Release exactly once. Fan-out gives each
// branch an independent reference via Retain, so branches release independently.
package data

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RawBatch is a bounded group of unparsed records plus their provenance.
type RawBatch struct {
	// Source identifies where the records came from (e.g. a file path or
	// "stdin"), used for provenance and error routing.
	Source string
	// Records are the raw, unparsed record bytes in arrival order.
	Records [][]byte
}

// Len reports the number of raw records in the batch.
func (b RawBatch) Len() int { return len(b.Records) }

// LineColumn is the column name used by row-oriented sources whose records are
// opaque line bytes, before a typed parser imposes structure.
const LineColumn = "line"

// TimeWindow bounds the interval a flushed buffer window covers.
type TimeWindow struct {
	Start time.Time
	End   time.Time
}

// RecordBatch is the structured, columnar unit that moves through parse →
// processors → encode. It wraps an Arrow record and carries source provenance.
type RecordBatch struct {
	// Source is carried through from the originating RawBatch.
	Source string
	// Window is the time range this batch covers, set by the buffer on flush;
	// nil for pre-buffer batches. It is held by pointer so the batch stays small
	// (it is passed by value on every hop) and immutable across fan-out copies.
	Window *TimeWindow
	rec    arrow.RecordBatch
}

// NewRecordBatch wraps an existing Arrow record batch. The batch takes ownership
// of the record's reference; the caller must not Release it separately.
func NewRecordBatch(source string, rec arrow.RecordBatch) RecordBatch {
	return RecordBatch{Source: source, rec: rec}
}

// Record returns the underlying Arrow record batch. It is nil for the zero
// value and for a batch explicitly constructed with none.
func (b RecordBatch) Record() arrow.RecordBatch { return b.rec }

// Len reports the number of rows in the batch.
func (b RecordBatch) Len() int {
	if b.rec == nil {
		return 0
	}
	return int(b.rec.NumRows())
}

// Retain increments the reference count so an additional owner can Release it
// independently. Fan-out uses this to hand the same immutable columns to
// multiple branches without copying.
func (b RecordBatch) Retain() {
	if b.rec != nil {
		b.rec.Retain()
	}
}

// Release returns the batch's buffers to the allocator. It is safe to call on
// the zero value and idempotent for a given RecordBatch variable.
func (b *RecordBatch) Release() {
	if b.rec != nil {
		b.rec.Release()
		b.rec = nil
	}
}

// NewLinesBatch builds a single-column RecordBatch (LineColumn: binary) from
// raw row bytes, allocating from mem. It is the base row→columnar conversion
// for opaque line inputs until typed parsers land. A nil mem uses the default
// allocator.
func NewLinesBatch(mem memory.Allocator, source string, rows [][]byte) RecordBatch {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: LineColumn, Type: arrow.BinaryTypes.Binary}},
		nil,
	)
	bld := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer bld.Release()
	bld.Reserve(len(rows))
	for _, r := range rows {
		bld.Append(r)
	}
	col := bld.NewArray()
	defer col.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, int64(len(rows)))
	return RecordBatch{Source: source, rec: rec}
}

// BlockMeta is the producing pipeline/branch and window provenance an output
// uses to name an artifact. The branch name is the artifact's "phase"
// (raw | template | summary).
type BlockMeta struct {
	Pipeline string
	Branch   string
	Window   TimeWindow
}

// EncodedBlock is a self-contained serialized artifact ready for an output —
// e.g. a complete Parquet file or a framed byte blob (docs/DESIGN.md §9).
type EncodedBlock struct {
	// Format is the encoder that produced the block (e.g. "parquet", "raw").
	Format string
	// Bytes is the encoded payload.
	Bytes []byte
	// Rows is how many logical rows the block represents.
	Rows int
	// Meta is the runtime-stamped provenance for naming; nil when unknown. It is
	// held by pointer so the block stays small when passed by value to outputs.
	Meta *BlockMeta
}

// Size reports the encoded payload size in bytes.
func (b EncodedBlock) Size() int { return len(b.Bytes) }
