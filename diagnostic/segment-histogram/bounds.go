package main

import (
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

var timeColumnPref = []string{"__prism_ts_ns", "ts", "timestamp_ms", "bucket"}

func parquetTimeBounds(path string) (minTS, maxTS time.Time, rows int64, err error) {
	rdr, err := file.OpenParquetFile(path, false) //nolint:gosec // G304: path is under an operator-supplied tenant root.
	if err != nil {
		return time.Time{}, time.Time{}, 0, err
	}
	defer func() { _ = rdr.Close() }()
	rows = rdr.NumRows()
	md := rdr.MetaData()
	idx, name, ok := pickTimeColumn(md.Schema)
	if !ok {
		return time.Time{}, time.Time{}, rows, nil
	}
	col := md.Schema.Column(idx)
	var found bool
	for i := 0; i < md.NumRowGroups(); i++ {
		rg := md.RowGroup(i)
		chunk, cerr := rg.ColumnChunk(idx)
		if cerr != nil {
			return time.Time{}, time.Time{}, rows, cerr
		}
		stats, serr := chunk.Statistics()
		if serr != nil {
			return time.Time{}, time.Time{}, rows, serr
		}
		if stats == nil || !stats.HasMinMax() {
			continue
		}
		lo, hi, decoded := decodeInt64Times(name, col, stats)
		if !decoded {
			continue
		}
		if !found || lo.Before(minTS) {
			minTS = lo
		}
		if !found || hi.After(maxTS) {
			maxTS = hi
		}
		found = true
	}
	if !found {
		return time.Time{}, time.Time{}, rows, nil
	}
	return minTS.UTC(), maxTS.UTC(), rows, nil
}

func pickTimeColumn(sc *schema.Schema) (idx int, name string, ok bool) {
	n := sc.NumColumns()
	byName := map[string]int{}
	for i := 0; i < n; i++ {
		byName[sc.Column(i).Name()] = i
	}
	for _, want := range timeColumnPref {
		if i, exists := byName[want]; exists {
			return i, want, true
		}
	}
	return 0, "", false
}

func decodeInt64Times(name string, col *schema.Column, stats metadata.TypedStatistics) (minTS, maxTS time.Time, ok bool) {
	is, ok := stats.(*metadata.Int64Statistics)
	if !ok || !is.HasMinMax() {
		return time.Time{}, time.Time{}, false
	}
	minTS = int64ToTime(name, col, is.Min())
	maxTS = int64ToTime(name, col, is.Max())
	if minTS.IsZero() || maxTS.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	return minTS, maxTS, true
}

func int64ToTime(name string, col *schema.Column, v int64) time.Time {
	switch name {
	case "__prism_ts_ns":
		return time.Unix(0, v).UTC()
	case "timestamp_ms":
		return time.UnixMilli(v).UTC()
	}
	if ts, ok := col.LogicalType().(schema.TimestampLogicalType); ok {
		switch ts.TimeUnit() {
		case schema.TimeUnitMillis:
			return time.UnixMilli(v).UTC()
		case schema.TimeUnitMicros:
			return time.UnixMicro(v).UTC()
		case schema.TimeUnitNanos:
			return time.Unix(0, v).UTC()
		}
	}
	switch col.ConvertedType() {
	case schema.ConvertedTypes.TimestampMillis:
		return time.UnixMilli(v).UTC()
	case schema.ConvertedTypes.TimestampMicros:
		return time.UnixMicro(v).UTC()
	}
	return time.Time{}
}
