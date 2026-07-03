// Package columnar builds an Arrow-backed RecordBatch from row-oriented,
// schema-discovered records (used by the logfmt and json parsers).
//
// Columns are the sorted union of keys across the batch (deterministic order).
// Each column's type is inferred with a fixed precedence — int64 if every
// present value is an integer, else float64 if every value is numeric, else
// bool if every value is boolean, else string — and absent/nil cells are null.
// Emitting a stable per-batch schema lets same-shaped windows concatenate in
// the buffer; heterogeneous batches surface as a schema error the runtime
// routes per policy.
package columnar

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/data"
)

// Build turns rows into a RecordBatch. Values may be string, int64, float64,
// bool, or nil; strings are re-parsed so numeric/bool logfmt values infer
// narrow types. An empty rows slice yields a zero-column, zero-row batch.
func Build(mem memory.Allocator, source string, rows []map[string]any) (data.RecordBatch, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	keys := unionKeys(rows)
	fields := make([]arrow.Field, len(keys))
	cols := make([]arrow.Array, len(keys))
	built := 0
	release := func() {
		for i := 0; i < built; i++ {
			cols[i].Release()
		}
	}
	for i, k := range keys {
		typ := inferType(rows, k)
		fields[i] = arrow.Field{Name: k, Type: typ, Nullable: true}
		col, err := buildColumn(mem, rows, k, typ)
		if err != nil {
			release()
			return data.RecordBatch{}, err
		}
		cols[i] = col
		built++
	}
	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecordBatch(schema, cols, int64(len(rows)))
	release() // NewRecordBatch retained the arrays
	return data.NewRecordBatch(source, rec), nil
}

func unionKeys(rows []map[string]any) []string {
	set := map[string]struct{}{}
	for _, r := range rows {
		for k := range r {
			set[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// kind ranks the inferred type of a single value for precedence widening.
type kind int

const (
	kInt kind = iota
	kFloat
	kBool
	kString
)

func valueKind(v any) (kind, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case int64:
		return kInt, true
	case float64:
		if x == float64(int64(x)) {
			return kInt, true
		}
		return kFloat, true
	case bool:
		return kBool, true
	case string:
		if _, err := strconv.ParseInt(x, 10, 64); err == nil {
			return kInt, true
		}
		if _, err := strconv.ParseFloat(x, 64); err == nil {
			return kFloat, true
		}
		if x == "true" || x == "false" {
			return kBool, true
		}
		return kString, true
	default:
		return kString, true
	}
}

func inferType(rows []map[string]any, key string) arrow.DataType {
	var hasInt, hasFloat, hasBool, hasStr, seen bool
	for _, r := range rows {
		v, ok := r[key]
		if !ok || v == nil {
			continue
		}
		k, present := valueKind(v)
		if !present {
			continue
		}
		seen = true
		switch k {
		case kInt:
			hasInt = true
		case kFloat:
			hasFloat = true
		case kBool:
			hasBool = true
		case kString:
			hasStr = true
		}
	}
	switch {
	case !seen, hasStr:
		return arrow.BinaryTypes.String
	case hasBool:
		if hasInt || hasFloat { // bool mixed with numbers has no common narrow type
			return arrow.BinaryTypes.String
		}
		return arrow.FixedWidthTypes.Boolean
	case hasFloat:
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.PrimitiveTypes.Int64
	}
}

func buildColumn(mem memory.Allocator, rows []map[string]any, key string, typ arrow.DataType) (arrow.Array, error) {
	switch typ.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[key]
			if n, valid := toInt(v, ok); valid {
				b.Append(n)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray(), nil
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[key]
			if f, valid := toFloat(v, ok); valid {
				b.Append(f)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray(), nil
	case arrow.BOOL:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[key]
			if bv, valid := toBool(v, ok); valid {
				b.Append(bv)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray(), nil
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for _, r := range rows {
			v, ok := r[key]
			if !ok || v == nil {
				b.AppendNull()
				continue
			}
			b.Append(toString(v))
		}
		return b.NewArray(), nil
	default:
		return nil, fmt.Errorf("columnar: unsupported inferred type %s", typ)
	}
}

func toInt(v any, ok bool) (int64, bool) {
	if !ok || v == nil {
		return 0, false
	}
	switch x := v.(type) {
	case int64:
		return x, true
	case float64:
		return int64(x), true
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	}
	return 0, false
}

func toFloat(v any, ok bool) (float64, bool) {
	if !ok || v == nil {
		return 0, false
	}
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	}
	return 0, false
}

func toBool(v any, ok bool) (bool, bool) {
	if !ok || v == nil {
		return false, false
	}
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		b, err := strconv.ParseBool(x)
		return b, err == nil
	}
	return false, false
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
