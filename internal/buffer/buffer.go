// Package buffer implements the accumulation window that sits between a
// pipeline's parser/pre-processors and its fan-out branches (docs/DESIGN.md
// §6.1). It accumulates parsed RecordBatches and flushes one bounded window
// batch on the first of: max age, max rows, or max bytes.
//
// The Accumulator is a pure state machine: callers drive Add/Flush and consult
// AgeExceeded/Deadline to arm a timer. Keeping time out of this type makes the
// flush logic deterministically testable without sleeps.
package buffer

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/data"
)

// Config bounds a window. A bound of 0 disables that trigger; at least one
// should be active (the config loader enforces this and applies defaults).
type Config struct {
	MaxAge   time.Duration
	MaxRows  int
	MaxBytes int64
}

// Accumulator collects RecordBatches into one window and reports when a size
// bound is met. It is not safe for concurrent use; a single pump goroutine owns
// it.
type Accumulator struct {
	cfg     Config
	mem     memory.Allocator
	batches []data.RecordBatch
	rows    int
	bytes   int64
	oldest  time.Time
	hasData bool
}

// New returns an empty accumulator. A nil allocator uses the default.
func New(cfg Config, mem memory.Allocator) *Accumulator {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	return &Accumulator{cfg: cfg, mem: mem}
}

// Add appends a batch (taking ownership of its buffers) and reports whether a
// row or byte bound is now met, i.e. the caller should Flush.
func (a *Accumulator) Add(rb data.RecordBatch, now time.Time) bool {
	if !a.hasData {
		a.oldest = now
		a.hasData = true
	}
	a.batches = append(a.batches, rb)
	a.rows += rb.Len()
	a.bytes += approxSize(rb)
	return a.sizeExceeded()
}

func (a *Accumulator) sizeExceeded() bool {
	if a.cfg.MaxRows > 0 && a.rows >= a.cfg.MaxRows {
		return true
	}
	if a.cfg.MaxBytes > 0 && a.bytes >= a.cfg.MaxBytes {
		return true
	}
	return false
}

// AgeExceeded reports whether the oldest buffered data has reached MaxAge.
func (a *Accumulator) AgeExceeded(now time.Time) bool {
	if !a.hasData || a.cfg.MaxAge <= 0 {
		return false
	}
	return !now.Before(a.oldest.Add(a.cfg.MaxAge))
}

// Deadline returns when the age bound will trigger, if there is buffered data
// and an age bound is set.
func (a *Accumulator) Deadline() (time.Time, bool) {
	if !a.hasData || a.cfg.MaxAge <= 0 {
		return time.Time{}, false
	}
	return a.oldest.Add(a.cfg.MaxAge), true
}

// Empty reports whether the accumulator holds no buffered data.
func (a *Accumulator) Empty() bool { return !a.hasData }

// Flush concatenates the accumulated batches into one window RecordBatch and
// resets. ok is false when the window is empty. The caller owns Releasing the
// returned batch. Input batches are always released here.
func (a *Accumulator) Flush() (data.RecordBatch, bool, error) {
	if !a.hasData || len(a.batches) == 0 {
		a.reset()
		return data.RecordBatch{}, false, nil
	}
	if len(a.batches) == 1 {
		out := a.batches[0]
		a.reset()
		return out, true, nil
	}
	out, err := concat(a.mem, a.batches)
	for i := range a.batches {
		a.batches[i].Release()
	}
	a.reset()
	if err != nil {
		return data.RecordBatch{}, false, err
	}
	return out, true, nil
}

func (a *Accumulator) reset() {
	a.batches = nil
	a.rows = 0
	a.bytes = 0
	a.oldest = time.Time{}
	a.hasData = false
}

// concat merges same-schema batches column-by-column into one RecordBatch.
func concat(mem memory.Allocator, batches []data.RecordBatch) (data.RecordBatch, error) {
	first := batches[0].Record()
	if first == nil {
		return data.RecordBatch{}, fmt.Errorf("buffer: concat: first batch has no record")
	}
	schema := first.Schema()
	ncols := int(first.NumCols())

	cols := make([]arrow.Array, ncols)
	var total int64
	for c := 0; c < ncols; c++ {
		parts := make([]arrow.Array, 0, len(batches))
		for _, b := range batches {
			rec := b.Record()
			if rec == nil {
				continue
			}
			if !rec.Schema().Equal(schema) {
				releaseArrays(cols[:c])
				return data.RecordBatch{}, fmt.Errorf("buffer: concat: mismatched schema across window batches")
			}
			parts = append(parts, rec.Column(c))
		}
		merged, err := array.Concatenate(parts, mem)
		if err != nil {
			releaseArrays(cols[:c])
			return data.RecordBatch{}, fmt.Errorf("buffer: concat column %d: %w", c, err)
		}
		cols[c] = merged
		total = int64(merged.Len())
	}
	rec := array.NewRecordBatch(schema, cols, total)
	releaseArrays(cols) // NewRecordBatch retained them
	return data.NewRecordBatch(batches[0].Source, rec), nil
}

func releaseArrays(arrs []arrow.Array) {
	for _, a := range arrs {
		if a != nil {
			a.Release()
		}
	}
}

// approxSize sums the in-memory buffer bytes backing a batch's columns.
func approxSize(rb data.RecordBatch) int64 {
	rec := rb.Record()
	if rec == nil {
		return 0
	}
	var total int64
	for c := 0; c < int(rec.NumCols()); c++ {
		total += arrayBytes(rec.Column(c).Data())
	}
	return total
}

func arrayBytes(d arrow.ArrayData) int64 {
	if d == nil {
		return 0
	}
	var total int64
	for _, b := range d.Buffers() {
		if b != nil {
			total += int64(b.Len())
		}
	}
	for _, child := range d.Children() {
		total += arrayBytes(child)
	}
	return total
}
