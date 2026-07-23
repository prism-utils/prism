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

	duckdb "github.com/marcboeker/go-duckdb"
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

// WriteWindow returns a reader-sized parquet blob in dir for ingest tests.
func WriteWindow(t testing.TB, dir, name string, rows []Row) string {
	t.Helper()
	path := filepath.Join(dir, name)
	WriteFile(t, path, rows)
	return path
}
