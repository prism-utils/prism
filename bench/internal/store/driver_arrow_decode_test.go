//go:build cgo

package store

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// TestDecodeArrowRowsTimestampColumn guards against the O(rows^2) decode bug: a
// Timestamp column must be decoded per element, not via fmt.Sprint of the whole
// column array (which is O(rows) per cell). A large row count would hang if the
// quadratic path returned; here we assert correct per-cell values instead.
func TestDecodeArrowRowsTimestampColumn(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "__name__", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	const rows = 5000
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < rows; i++ {
		b.Field(0).(*array.StringBuilder).Append("metric")
		b.Field(1).(*array.Float64Builder).Append(float64(i))
		ts, err := arrow.TimestampFromTime(base.Add(time.Duration(i)*time.Second), arrow.Microsecond)
		require.NoError(t, err)
		b.Field(2).(*array.TimestampBuilder).Append(ts)
	}
	rec := b.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	out, err := decodeArrowRows(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Len(t, out, rows)

	// Every row must have all three cells and a non-nil timestamp decoded per element.
	for i, row := range out {
		require.Len(t, row, 3)
		require.NotNil(t, row[2], "row %d timestamp cell must decode per element", i)
	}
}
