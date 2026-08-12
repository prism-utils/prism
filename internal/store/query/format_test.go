package query

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"database/sql"
	"fmt"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const formatTenant = "user-6f3a9c2b-apps"

func TestSQLSandboxMixedParquetAndDuckDB(t *testing.T) {
	dataDir := t.TempDir()
	tenantRoot := filepath.Join(dataDir, formatTenant)
	hotDir := filepath.Join(tenantRoot, "hot")
	l0 := filepath.Join(tenantRoot, "tiers", "L0")
	for _, d := range []string{hotDir, l0} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}

	testparquet.WriteSegmentWithTs(t, filepath.Join(hotDir, "current.parquet"),
		time.Unix(1, 0).UTC(), "hot_metric", 1)
	writeMetricsDuckDBFile(t, filepath.Join(l0, "1700000000000000000-dead.duckdb"), "cold_metric", 2)

	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &SQLConfig{DataDir: dataDir, RunJobs: false, MaxRows: 1000, Timeout: 10 * time.Second}
	mux := http.NewServeMux()
	mux.Handle(SQLRoutePattern(""), SQLHandler(cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	body := `{"sql":"SELECT \"__name__\" FROM metrics ORDER BY 1"}`
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/"+formatTenant+"/sql", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	var out SQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.RowCount != 2 {
		t.Fatalf("row_count=%d want 2 (hot parquet + L0 duckdb); cols=%v rows=%v",
			out.RowCount, out.Columns, out.Rows)
	}
}

func TestSQLSandboxHotDuckDBOnly(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{
		DataDir:              dataDir,
		HotWindow:            time.Hour,
		HotSegmentFormat:     segformat.DuckDB,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 7, TimestampMs: 0},
	})
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := eng.Ingest(formatTenant, f); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &SQLConfig{DataDir: dataDir, RunJobs: true, MaxRows: 1000, Timeout: 10 * time.Second}
	mux := http.NewServeMux()
	mux.Handle(SQLRoutePattern(""), SQLHandler(cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	body := `{"sql":"SELECT COUNT(*) AS c FROM metrics"}`
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/"+formatTenant+"/sql", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	snap := filepath.Join(dataDir, formatTenant, "hot", "current.duckdb")
	if _, err := os.Stat(snap); err != nil {
		t.Fatalf("expected hot duckdb snapshot: %v", err)
	}
}

func TestSQLLogsReadsDuckDBTier(t *testing.T) {
	dataDir := t.TempDir()
	tenantRoot := filepath.Join(dataDir, formatTenant)
	parquetTier := layout.LogsTierDir(dataDir, formatTenant, "logs-raw", 1)
	tier := layout.LogsTierDir(dataDir, formatTenant, "logs-raw", 0)
	if err := os.MkdirAll(parquetTier, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(tier, 0o750); err != nil {
		t.Fatal(err)
	}
	writeTinyLogParquet(t, filepath.Join(parquetTier, layout.SegmentName(time.Unix(1, 0).UTC())), "packed")
	writeLogsDuckDBFile(t, filepath.Join(tier, "1700000000000000000-beef.duckdb"), "merged")

	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	// Touch tenant engine so resolveSandboxTenantRoot succeeds.
	if _, err := eng.DB(formatTenant); err != nil {
		t.Fatal(err)
	}
	_ = tenantRoot

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &SQLConfig{DataDir: dataDir, RunJobs: false, MaxRows: 1000, Timeout: 10 * time.Second}
	mux := http.NewServeMux()
	mux.Handle(SQLRoutePattern(""), SQLHandler(cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	body := `{"sql":"SELECT COUNT(*) AS c FROM logs"}`
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/"+formatTenant+"/sql", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	var out SQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.RowCount != 1 || len(out.Rows) != 1 {
		t.Fatalf("unexpected response: %+v", out)
	}
	// COUNT(*) returns one row; value should be 2 (parquet tier + duckdb tier).
	got, ok := out.Rows[0][0].(float64)
	if !ok {
		// json numbers may decode as float64; also accept json.Number via Decode defaults
		switch v := out.Rows[0][0].(type) {
		case float64:
			got = v
		default:
			t.Fatalf("count type %T value %v", out.Rows[0][0], out.Rows[0][0])
		}
	}
	if int(got) != 2 {
		t.Fatalf("logs COUNT(*)=%v want 2", out.Rows[0][0])
	}
}

func writeMetricsDuckDBFile(t *testing.T, path, name string, value float64) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf(`
		ATTACH '%s' AS exp (STORAGE_VERSION '%s');
		CREATE TABLE exp.%s AS
			SELECT '%s' AS "__name__", '{}' AS labels, %g::DOUBLE AS value,
			       1::BIGINT AS timestamp_ms, TIMESTAMP '2023-11-14 22:13:20' AS ts;
		CHECKPOINT exp;
		DETACH exp;
	`, filepath.ToSlash(path), segformat.DefaultStorageVersion, segformat.MetricsTable, name, value)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}

func writeLogsDuckDBFile(t *testing.T, path, message string) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf(`
		ATTACH '%s' AS exp (STORAGE_VERSION '%s');
		CREATE TABLE exp.%s AS SELECT '%s' AS message, 'raw' AS format;
		CHECKPOINT exp;
		DETACH exp;
	`, filepath.ToSlash(path), segformat.DefaultStorageVersion, segformat.LogsTable, message)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}

func writeTinyLogParquet(t *testing.T, path, message string) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf(
		`COPY (SELECT '%s' AS message, 'raw' AS format) TO '%s' (FORMAT parquet)`,
		message, filepath.ToSlash(path),
	)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}
