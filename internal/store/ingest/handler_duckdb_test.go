package ingest_test

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"

	"github.com/elk-utilities/prism/internal/duckdbfile"
	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/segformat"
)

func writeMetricsDuckDBWindow(t *testing.T, storageVersion string) []byte {
	t.Helper()
	if storageVersion == "" {
		storageVersion = duckdbfile.DefaultStorageVersion
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "w.duckdb")
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	attach := "ATTACH '" + filepath.ToSlash(path) + "' AS exp (STORAGE_VERSION '" + storageVersion + "')"
	if _, err := db.ExecContext(ctx, attach); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
		CREATE TABLE exp.`+duckdbfile.Table+` AS
		SELECT 'up' AS "__name__", '{}' AS labels, 1.0::DOUBLE AS value, 0::BIGINT AS timestamp_ms`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CHECKPOINT exp"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DETACH exp"); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestIngestDuckDB_ContentType(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	body := writeMetricsDuckDBWindow(t, "")
	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), bytes.NewReader(body))
	req.Header.Set("Content-Type", duckdbfile.ContentType)
	resp := doIngestReq(t, req)
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

func TestIngestDuckDB_OctetStreamMagic(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	body := writeMetricsDuckDBWindow(t, "")
	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp := doIngestReq(t, req)
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

func TestIngestDuckDB_ParquetUnchanged(t *testing.T) {
	h, eng := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	f := validWindowBody(t)
	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), f)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp := doIngestReq(t, req)
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
}

func TestIngestDuckDB_IncompatibleStorageVersion(t *testing.T) {
	h, _ := testHandler(t, testConfig("", ingest.AuthNone))
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Truncate a valid duckdb past the magic so ATTACH fails with a clear 4xx.
	body := writeMetricsDuckDBWindow(t, "")
	if len(body) < 64 {
		t.Fatalf("fixture too small: %d", len(body))
	}
	body = body[:64]

	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), bytes.NewReader(body))
	req.Header.Set("Content-Type", duckdbfile.ContentType)
	resp := doIngestReq(t, req)
	defer closeResp(t, resp)
	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("want 4xx for incompatible/corrupt duckdb, got %d body=%s", resp.StatusCode, b)
	}
	msg, _ := io.ReadAll(resp.Body)
	lower := strings.ToLower(string(msg))
	if !strings.Contains(lower, "incompatible") &&
		!strings.Contains(lower, "duckdb") &&
		!strings.Contains(lower, "storage") {
		t.Fatalf("error body should name the failure, got %q", msg)
	}
}

func TestIngestDuckDB_LogsLandAsDuckDB(t *testing.T) {
	dataDir := t.TempDir()
	cfg := ingest.Config{
		AllowedArtifacts: []string{"logs-raw"},
		MaxBodyBytes:     1 << 20,
		AuthMode:         ingest.AuthNone,
	}
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := discardLogger()
	mux := http.NewServeMux()
	mux.Handle(ingest.IngestRoutePattern(""), ingest.Handler(&cfg, eng, logger))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "l.duckdb")
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	attach := "ATTACH '" + filepath.ToSlash(path) + "' AS exp (STORAGE_VERSION '" + segformat.DefaultStorageVersion + "')"
	if _, err := db.ExecContext(ctx, attach); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE exp.`+duckdbfile.Table+` AS SELECT 'hi' AS message, 'none' AS format`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CHECKPOINT exp"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DETACH exp"); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "logs-raw"), bytes.NewReader(body))
	req.Header.Set("Content-Type", duckdbfile.ContentType)
	resp := doIngestReq(t, req)
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	logsDir := filepath.Join(dataDir, testTenant, "logs", "logs-raw")
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".duckdb") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected a .duckdb log window under %s, got %v", logsDir, entries)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
