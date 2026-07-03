// Package data defines the units of information that flow between pipeline
// stages: RawBatch (pre-parse), RecordBatch (structured), and EncodedBlock
// (serialized output).
//
// The interim payload here is row-oriented ([][]byte). Phase 2 of docs/PLAN.md
// replaces RecordBatch's internals with an Apache Arrow-backed columnar
// representation (schema + column arrays + poolable buffers) per
// docs/DESIGN.md §5. The component interfaces do NOT change when that happens —
// that is the whole point of keeping the payload behind these types.
package data

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

// RecordBatch is the structured unit that moves through parse → processors →
// encode. In the foundation it carries rows as byte slices; Phase 2 gives it an
// Arrow schema and columnar arrays.
//
// Ownership is linear: whoever receives a RecordBatch is responsible for
// calling Release when done (docs/DESIGN.md §5). Release is a no-op placeholder
// today and becomes an allocator return in Phase 2.
type RecordBatch struct {
	// Source is carried through from the originating RawBatch.
	Source string
	// Records is the interim row payload. Phase 2 replaces this with columns.
	Records [][]byte
}

// Len reports the number of rows in the batch.
func (b RecordBatch) Len() int { return len(b.Records) }

// Release returns any pooled buffers backing the batch. It is safe to call
// multiple times (idempotent) and must be called exactly once by the owner.
func (b *RecordBatch) Release() { b.Records = nil }

// EncodedBlock is a self-contained serialized artifact ready for an output —
// e.g. a complete Parquet file or a framed byte blob (docs/DESIGN.md §9).
type EncodedBlock struct {
	// Format is the encoder that produced the block (e.g. "parquet", "raw").
	Format string
	// Bytes is the encoded payload.
	Bytes []byte
	// Rows is how many logical rows the block represents.
	Rows int
}

// Size reports the encoded payload size in bytes.
func (b EncodedBlock) Size() int { return len(b.Bytes) }
