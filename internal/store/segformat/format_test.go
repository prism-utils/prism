package segformat_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/segformat"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

func TestParse(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    segformat.Format
		wantErr bool
	}{
		{"", segformat.Parquet, false},
		{"parquet", segformat.Parquet, false},
		{"PARQUET", segformat.Parquet, false},
		{"duckdb", segformat.DuckDB, false},
		{"DuckDB", segformat.DuckDB, false},
		{"json", "", true},
		{"orc", "", true},
	}
	for _, tc := range cases {
		got, err := segformat.Parse(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("Parse(%q) err=nil, want error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("Parse(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestExt(t *testing.T) {
	t.Parallel()
	if got := segformat.Parquet.DotExt(); got != ".parquet" {
		t.Fatalf("Parquet.DotExt=%q", got)
	}
	if got := segformat.DuckDB.DotExt(); got != ".duckdb" {
		t.Fatalf("DuckDB.DotExt=%q", got)
	}
}

func TestConvertDuckDBFileToParquet(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "seg.duckdb")
	dst := filepath.Join(dir, "seg.parquet")
	writeMetricsDuckDB(t, src, 1)
	if err := segformat.ConvertDuckDBToParquet(src, dst, segformat.MetricsTable); err != nil {
		t.Fatalf("convert: %v", err)
	}
	assertParquetReadable(t, dst)
}

func TestConvertTenantDuckDBSegments(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-6f3a9c2b-apps"
	hot := filepath.Join(dataDir, tenant, "hot")
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	logsTier := filepath.Join(dataDir, tenant, "logs", "logs-raw", "tiers", "L0")
	for _, d := range []string{hot, l0, logsTier} {
		if err := segformat.MkdirAllForTest(d); err != nil {
			t.Fatal(err)
		}
	}

	writeMetricsDuckDB(t, filepath.Join(hot, "current.duckdb"), 1)
	segDB := filepath.Join(l0, "1700000000000000000-aabb.duckdb")
	writeMetricsDuckDB(t, segDB, 2)
	logDB := filepath.Join(logsTier, "1700000000000000001-ccdd.duckdb")
	writeLogsDuckDB(t, logDB)

	n, err := segformat.ConvertTenantDuckDBToParquet(dataDir, tenant)
	if err != nil {
		t.Fatalf("ConvertTenantDuckDBToParquet: %v", err)
	}
	if n < 3 {
		t.Fatalf("converted %d files, want >= 3", n)
	}
	if segformat.FileExistsForTest(filepath.Join(hot, "current.duckdb")) {
		t.Fatal("hot current.duckdb should be removed after convert")
	}
	if !segformat.FileExistsForTest(filepath.Join(hot, "current.parquet")) {
		t.Fatal("hot current.parquet missing after convert")
	}
	if segformat.FileExistsForTest(segDB) {
		t.Fatal("L0 duckdb should be removed after convert")
	}
	if !segformat.FileExistsForTest(filepath.Join(l0, "1700000000000000000-aabb.parquet")) {
		t.Fatal("L0 parquet sibling missing after convert")
	}
}

func writeMetricsDuckDB(t *testing.T, path string, value float64) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	slash := filepath.ToSlash(path)
	q := fmt.Sprintf(`
		ATTACH '%s' AS exp (STORAGE_VERSION '%s');
		CREATE TABLE exp.%s AS
			SELECT 'up' AS "__name__", '{}' AS labels, %g::DOUBLE AS value,
			       42::BIGINT AS timestamp_ms, TIMESTAMP '2023-11-14 22:13:20' AS ts;
		CHECKPOINT exp;
		DETACH exp;
	`, slash, segformat.DefaultStorageVersion, segformat.MetricsTable, value)
	if _, err := db.ExecContext(ctx, q); err != nil {
		t.Fatalf("write metrics duckdb: %v", err)
	}
	_ = time.Now
}

func writeLogsDuckDB(t *testing.T, path string) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	slash := filepath.ToSlash(path)
	q := fmt.Sprintf(`
		ATTACH '%s' AS exp (STORAGE_VERSION '%s');
		CREATE TABLE exp.%s AS SELECT 'hello' AS message, 'raw' AS format;
		CHECKPOINT exp;
		DETACH exp;
	`, slash, segformat.DefaultStorageVersion, segformat.LogsTable)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write logs duckdb: %v", err)
	}
}

func assertParquetReadable(t *testing.T, path string) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	var n int
	q := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", filepath.ToSlash(path))
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	if n < 1 {
		t.Fatalf("parquet row count=%d, want >=1", n)
	}
}
