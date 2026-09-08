package events

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/prism-utils/prism/internal/alert/notify"
)

const (
	statusFiring   = "firing"
	statusResolved = "resolved"
)

// Row is one decoded alert_events parquet record (tests and round-trip checks).
type Row struct {
	Ts             time.Time
	Fingerprint    string
	Alertname      string
	Severity       string
	Status         string
	Summary        string
	Description    string
	Recommendation string
	StartsAt       time.Time
	EndsAt         time.Time
	Labels         string
}

func alertEventsSchema() *arrow.Schema {
	ts := arrow.FixedWidthTypes.Timestamp_us
	str := arrow.BinaryTypes.String
	return arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: ts, Nullable: true},
		{Name: "fingerprint", Type: str, Nullable: true},
		{Name: "alertname", Type: str, Nullable: true},
		{Name: "severity", Type: str, Nullable: true},
		{Name: "status", Type: str, Nullable: true},
		{Name: "summary", Type: str, Nullable: true},
		{Name: "description", Type: str, Nullable: true},
		{Name: "recommendation", Type: str, Nullable: true},
		{Name: "starts_at", Type: ts, Nullable: true},
		{Name: "ends_at", Type: ts, Nullable: true},
		{Name: "labels", Type: str, Nullable: true},
	}, nil)
}

func recommendationOf(ann map[string]string) string {
	if ann == nil {
		return ""
	}
	if v := ann["recommendation"]; v != "" {
		return v
	}
	return ann["runbook"]
}

func labelsJSON(lbls map[string]string) string {
	if lbls == nil {
		lbls = map[string]string{}
	}
	b, err := json.Marshal(lbls)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func appendTS(b *array.TimestampBuilder, t time.Time) {
	if t.IsZero() {
		b.AppendNull()
		return
	}
	b.Append(arrow.Timestamp(t.UTC().UnixMicro()))
}

func appendStr(b *array.StringBuilder, s string) {
	b.Append(s)
}

// Encode writes alert transition rows as a single parquet window. An empty
// input yields a nil body (ingest no-op).
func Encode(alerts []notify.Alert) ([]byte, error) {
	if len(alerts) == 0 {
		return nil, nil
	}
	mem := memory.NewGoAllocator()
	schema := alertEventsSchema()
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	tsB := bldr.Field(0).(*array.TimestampBuilder)
	fpB := bldr.Field(1).(*array.StringBuilder)
	nameB := bldr.Field(2).(*array.StringBuilder)
	sevB := bldr.Field(3).(*array.StringBuilder)
	stB := bldr.Field(4).(*array.StringBuilder)
	sumB := bldr.Field(5).(*array.StringBuilder)
	descB := bldr.Field(6).(*array.StringBuilder)
	recB := bldr.Field(7).(*array.StringBuilder)
	startB := bldr.Field(8).(*array.TimestampBuilder)
	endB := bldr.Field(9).(*array.TimestampBuilder)
	lblB := bldr.Field(10).(*array.StringBuilder)

	for _, a := range alerts {
		lbls := a.Labels
		if lbls == nil {
			lbls = map[string]string{}
		}
		ann := a.Annotations
		if ann == nil {
			ann = map[string]string{}
		}
		status := statusFiring
		eventTS := a.StartsAt
		if a.Resolved {
			status = statusResolved
			eventTS = a.ResolvedAt
		}
		appendTS(tsB, eventTS)
		appendStr(fpB, notify.Fingerprint(lbls))
		appendStr(nameB, lbls["alertname"])
		appendStr(sevB, lbls["severity"])
		appendStr(stB, status)
		appendStr(sumB, ann["summary"])
		appendStr(descB, ann["description"])
		appendStr(recB, recommendationOf(ann))
		appendTS(startB, a.StartsAt)
		if a.Resolved && !a.ResolvedAt.IsZero() {
			appendTS(endB, a.ResolvedAt)
		} else {
			endB.AppendNull()
		}
		appendStr(lblB, labelsJSON(lbls))
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	fw, err := pqarrow.NewFileWriter(schema, &buf, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("alert events parquet writer: %w", err)
	}
	if err := fw.Write(rec); err != nil {
		_ = fw.Close()
		return nil, fmt.Errorf("alert events parquet write: %w", err)
	}
	if err := fw.Close(); err != nil {
		return nil, fmt.Errorf("alert events parquet close: %w", err)
	}
	return buf.Bytes(), nil
}

func colString(col *arrow.Chunked, i int) string {
	off := i
	for _, ch := range col.Chunks() {
		n := int(ch.Len())
		if off < n {
			if ch.IsNull(off) {
				return ""
			}
			return ch.(*array.String).Value(off)
		}
		off -= n
	}
	return ""
}

func colTS(col *arrow.Chunked, i int) time.Time {
	off := i
	for _, ch := range col.Chunks() {
		n := int(ch.Len())
		if off < n {
			if ch.IsNull(off) {
				return time.Time{}
			}
			ts := ch.(*array.Timestamp).Value(off)
			return time.UnixMicro(int64(ts)).UTC()
		}
		off -= n
	}
	return time.Time{}
}

// DecodeForTest reads an Encode() parquet window back into rows.
func DecodeForTest(body []byte) ([]Row, error) {
	if len(body) == 0 {
		return nil, nil
	}
	rdr, err := file.NewParquetReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdr.Close() }()
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	n := int(tbl.NumRows())
	out := make([]Row, n)
	for i := 0; i < n; i++ {
		out[i] = Row{
			Ts:             colTS(tbl.Column(0).Data(), i),
			Fingerprint:    colString(tbl.Column(1).Data(), i),
			Alertname:      colString(tbl.Column(2).Data(), i),
			Severity:       colString(tbl.Column(3).Data(), i),
			Status:         colString(tbl.Column(4).Data(), i),
			Summary:        colString(tbl.Column(5).Data(), i),
			Description:    colString(tbl.Column(6).Data(), i),
			Recommendation: colString(tbl.Column(7).Data(), i),
			StartsAt:       colTS(tbl.Column(8).Data(), i),
			EndsAt:         colTS(tbl.Column(9).Data(), i),
			Labels:         colString(tbl.Column(10).Data(), i),
		}
	}
	return out, nil
}
