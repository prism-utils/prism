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
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/data"
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

// WindowStart returns the time of the oldest buffered batch — the start of the
// window that the next Flush will produce. It is the zero time when empty, and
// resets to zero after Flush.
func (a *Accumulator) WindowStart() time.Time {
	if !a.hasData {
		return time.Time{}
	}
	return a.oldest
}

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

// concat merges batches into one RecordBatch over the union of their columns.
// Real logs are heterogeneous: a key present in one line may be absent in the
// next, or typed differently. Rather than fail, concat aligns to a union schema
// (first-seen column order): a column absent from a batch contributes nulls,
// and a column whose type differs across batches is widened to string. Only
// same-typed windows stay narrow.
func concat(mem memory.Allocator, batches []data.RecordBatch) (data.RecordBatch, error) {
	recs := make([]arrow.RecordBatch, 0, len(batches))
	for _, b := range batches {
		if rec := b.Record(); rec != nil {
			recs = append(recs, rec)
		}
	}
	if len(recs) == 0 {
		return data.RecordBatch{}, fmt.Errorf("buffer: concat: no records to merge")
	}

	order, types := unionSchema(recs)
	fields := make([]arrow.Field, len(order))
	for i, name := range order {
		fields[i] = arrow.Field{Name: name, Type: types[name], Nullable: true}
	}

	cols := make([]arrow.Array, len(order))
	built := 0
	var total int64
	for _, rec := range recs {
		total += rec.NumRows()
	}
	for ci, name := range order {
		target := types[name]
		parts := make([]arrow.Array, 0, len(recs))
		temps := make([]arrow.Array, 0, len(recs))
		for _, rec := range recs {
			part, owned, err := alignColumn(mem, rec, name, target)
			if err != nil {
				releaseArrays(temps)
				releaseArrays(cols[:built])
				return data.RecordBatch{}, err
			}
			parts = append(parts, part)
			if owned {
				temps = append(temps, part)
			}
		}
		merged, err := array.Concatenate(parts, mem)
		releaseArrays(temps)
		if err != nil {
			releaseArrays(cols[:built])
			return data.RecordBatch{}, fmt.Errorf("buffer: concat column %q: %w", name, err)
		}
		cols[ci] = merged
		built++
	}

	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, total)
	releaseArrays(cols) // NewRecordBatch retained them
	return data.NewRecordBatch(batches[0].Source, rec), nil
}

// unionSchema returns the union column names in first-seen order and the
// resolved type per column (string when types conflict across batches).
func unionSchema(recs []arrow.RecordBatch) ([]string, map[string]arrow.DataType) {
	var order []string
	types := map[string]arrow.DataType{}
	conflict := map[string]bool{}
	for _, rec := range recs {
		for _, f := range rec.Schema().Fields() {
			if _, seen := types[f.Name]; !seen {
				types[f.Name] = f.Type
				order = append(order, f.Name)
				continue
			}
			if !conflict[f.Name] && !arrow.TypeEqual(types[f.Name], f.Type) {
				conflict[f.Name] = true
			}
		}
	}
	for name := range conflict {
		types[name] = arrow.BinaryTypes.String
	}
	return order, types
}

// alignColumn returns rec's column `name` coerced to target. owned reports
// whether the returned array was freshly built (and must be released by the
// caller after concatenation); a borrowed source column must not be released.
func alignColumn(mem memory.Allocator, rec arrow.RecordBatch, name string, target arrow.DataType) (arr arrow.Array, owned bool, err error) {
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		a, err := nullArray(mem, target, int(rec.NumRows()))
		return a, true, err
	}
	col := rec.Column(idx[0])
	if arrow.TypeEqual(col.DataType(), target) {
		return col, false, nil
	}
	a, err := castToString(mem, col)
	return a, true, err
}

// nullArray builds an all-null array of the given type and length.
func nullArray(mem memory.Allocator, dt arrow.DataType, n int) (arrow.Array, error) {
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	b.AppendNulls(n)
	return b.NewArray(), nil
}

// castToString renders any supported array as a string array (nulls preserved).
func castToString(mem memory.Allocator, col arrow.Array) (arrow.Array, error) {
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		switch a := col.(type) {
		case *array.String:
			b.Append(a.Value(i))
		case *array.Binary:
			b.Append(string(a.Value(i)))
		case *array.Int64:
			b.Append(strconv.FormatInt(a.Value(i), 10))
		case *array.Float64:
			b.Append(strconv.FormatFloat(a.Value(i), 'g', -1, 64))
		case *array.Boolean:
			b.Append(strconv.FormatBool(a.Value(i)))
		default:
			return nil, fmt.Errorf("buffer: cannot widen %s to string", col.DataType())
		}
	}
	return b.NewArray(), nil
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
