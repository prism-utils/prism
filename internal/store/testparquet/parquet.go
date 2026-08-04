// Package testparquet builds contract-v1 metrics-raw parquet fixtures via DuckDB.
package testparquet

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// Row is one metrics-raw sample (prism contract v1).
type Row struct {
	Name        string
	Labels      string
	Value       float64
	TimestampMs int64
}

// WriteFile writes rows to path as parquet. Empty rows is a no-op.
func WriteFile(t testing.TB, path string, rows []Row) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	tmp := path + ".tmp"
	if _, err := db.ExecContext(context.Background(), fmt.Sprintf(`
		COPY (
			SELECT * FROM (VALUES %s) AS t("__name__", labels, value, timestamp_ms)
		) TO '%s' (FORMAT parquet)
	`, valuesClause(rows), filepath.ToSlash(tmp))); err != nil {
		t.Fatalf("copy parquet: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

func valuesClause(rows []Row) string {
	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = fmt.Sprintf("('%s', '%s', %f, %d)", escape(r.Name), escape(r.Labels), r.Value, r.TimestampMs)
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += ", " + p
	}
	return out
}

// WriteRollupBucket writes a single-bucket rollup parquet for retention tests.
func WriteRollupBucket(t testing.TB, path string, bucket time.Time, name string, value float64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	bucketStr := bucket.UTC().Format("2006-01-02 15:04:05.999999")
	tmp := path + ".tmp"
	//nolint:gosec // G201: test fixture SQL with controlled literals only.
	q := fmt.Sprintf(`
		COPY (
			SELECT CAST('%s' AS TIMESTAMP) AS bucket, '%s' AS "__name__",
			       %f AS avg, %f AS min, %f AS max, 1::BIGINT AS count, %f AS sum
		) TO '%s' (FORMAT parquet)
	`, bucketStr, escape(name), value, value, value, value, filepath.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("copy rollup: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

// WriteSegmentWithTs writes a single-row metrics parquet including proxy ingest ts.
func WriteSegmentWithTs(t testing.TB, path string, ts time.Time, metric string, value float64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	tsStr := ts.UTC().Format("2006-01-02 15:04:05.999999")
	tmp := path + ".tmp"
	//nolint:gosec // G201: test fixture SQL with controlled literals only.
	q := fmt.Sprintf(`
		COPY (
			SELECT '%s' AS "__name__", '{}' AS labels, %f AS value, 0 AS timestamp_ms,
			       CAST('%s' AS TIMESTAMP) AS ts
		) TO '%s' (FORMAT parquet)
	`, escape(metric), value, tsStr, filepath.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("copy: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

func escape(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			b = append(b, '\'', '\'')
		} else {
			b = append(b, s[i])
		}
	}
	return string(b)
}

// SegRow is one metrics row with an explicit ingest timestamp, used to build
// fixtures whose samples are spread over time (range/rate query tests).
type SegRow struct {
	Name   string
	Labels string
	Value  float64
	Ts     time.Time
}

// WriteSegmentRows writes rows to path as a full-schema metrics parquet
// (contract v1 columns plus the ingest `ts`), so a sandbox metrics view reads
// samples at controlled timestamps. Empty rows is a no-op.
func WriteSegmentRows(t testing.TB, path string, rows []SegRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	parts := make([]string, len(rows))
	for i, r := range rows {
		tsStr := r.Ts.UTC().Format("2006-01-02 15:04:05.999999999")
		parts[i] = fmt.Sprintf("('%s', '%s', %v, 0::BIGINT, CAST('%s' AS TIMESTAMP))",
			escape(r.Name), escape(r.Labels), r.Value, tsStr)
	}
	values := parts[0]
	for _, p := range parts[1:] {
		values += ", " + p
	}
	tmp := path + ".tmp"
	//nolint:gosec // G201: test fixture SQL with controlled literals only.
	q := fmt.Sprintf(`
		COPY (
			SELECT * FROM (VALUES %s) AS t("__name__", labels, value, timestamp_ms, ts)
		) TO '%s' (FORMAT parquet)
	`, values, filepath.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("copy segment: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

// LogSummaryRow is one logs-summary row (template → count), contract v1 §3.4.
type LogSummaryRow struct {
	Template string
	Count    int64
}

// WriteLogsSummaryFile writes rows to path as a logs-summary parquet
// (columns template VARCHAR, count BIGINT). Empty rows is a no-op.
func WriteLogsSummaryFile(t testing.TB, path string, rows []LogSummaryRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = fmt.Sprintf("('%s', CAST(%d AS BIGINT))", escape(r.Template), r.Count)
	}
	values := parts[0]
	for _, p := range parts[1:] {
		values += ", " + p
	}
	tmp := path + ".tmp"
	//nolint:gosec // G201: test fixture SQL with controlled literals only.
	q := fmt.Sprintf(`
		COPY (
			SELECT * FROM (VALUES %s) AS t(template, "count")
		) TO '%s' (FORMAT parquet)
	`, values, filepath.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("copy logs summary: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

// LogRow is one logs-raw row (message → line text, format → discovered shape),
// contract v1 §3.4.
type LogRow struct {
	Message string
	Format  string
}

// WriteLogsRawFile writes rows to path as a logs-raw parquet (columns message
// VARCHAR, format VARCHAR). Empty rows is a no-op.
func WriteLogsRawFile(t testing.TB, path string, rows []LogRow) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = fmt.Sprintf("('%s', '%s')", escape(r.Message), escape(r.Format))
	}
	values := parts[0]
	for _, p := range parts[1:] {
		values += ", " + p
	}
	tmp := path + ".tmp"
	//nolint:gosec // G201: test fixture SQL with controlled literals only.
	q := fmt.Sprintf(`
		COPY (
			SELECT * FROM (VALUES %s) AS t(message, format)
		) TO '%s' (FORMAT parquet)
	`, values, filepath.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("copy logs raw: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

// WriteWindow returns a reader-sized parquet blob in dir for ingest tests.
func WriteWindow(t testing.TB, dir, name string, rows []Row) string {
	t.Helper()
	path := filepath.Join(dir, name)
	WriteFile(t, path, rows)
	return path
}
