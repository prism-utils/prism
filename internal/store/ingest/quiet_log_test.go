package ingest_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

func TestHTTPIngestSuccessLogsAtDebug(t *testing.T) {
	eng := engine.New(engine.Config{DataDir: t.TempDir(), HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	cfg := testConfig("", ingest.AuthNone)
	cfg.AllowedArtifacts = []string{"metrics-raw"}

	var infoBuf bytes.Buffer
	infoLogger := slog.New(slog.NewTextHandler(&infoBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mux := http.NewServeMux()
	mux.Handle(ingest.IngestRoutePattern(cfg.RoutePrefix), ingest.Handler(&cfg, eng, infoLogger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	body := validWindowBody(t)
	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "metrics-raw"), body)
	resp := doIngestReq(t, req)
	closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if strings.Contains(infoBuf.String(), "ingested") {
		t.Fatalf("success ingest must not log at Info; got %q", infoBuf.String())
	}

	var debugBuf bytes.Buffer
	debugLogger := slog.New(slog.NewTextHandler(&debugBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mux2 := http.NewServeMux()
	mux2.Handle(ingest.IngestRoutePattern(cfg.RoutePrefix), ingest.Handler(&cfg, eng, debugLogger))
	srv2 := httptest.NewServer(mux2)
	t.Cleanup(srv2.Close)

	body2 := validWindowBody(t)
	req2 := newIngestReq(t, ingestURL(srv2.URL, "", testTenant, "metrics-raw"), body2)
	resp2 := doIngestReq(t, req2)
	closeResp(t, resp2)
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", resp2.StatusCode)
	}
	out := debugBuf.String()
	if !strings.Contains(out, "ingested") || !strings.Contains(strings.ToLower(out), "debug") {
		t.Fatalf("success ingest should log at Debug; got %q", out)
	}
}

func TestHTTPLogLandSuccessLogsAtDebug(t *testing.T) {
	eng := engine.New(engine.Config{DataDir: t.TempDir(), HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	cfg := testConfig("", ingest.AuthNone)
	cfg.AllowedArtifacts = []string{"logs-raw"}

	var infoBuf bytes.Buffer
	infoLogger := slog.New(slog.NewTextHandler(&infoBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mux := http.NewServeMux()
	mux.Handle(ingest.IngestRoutePattern(cfg.RoutePrefix), ingest.Handler(&cfg, eng, infoLogger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	path := filepath.Join(t.TempDir(), "w.parquet")
	testparquet.WriteLogsRawFile(t, path, []testparquet.LogRow{{Message: "hi", Format: "none"}})
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })

	req := newIngestReq(t, ingestURL(srv.URL, "", testTenant, "logs-raw"), f)
	resp := doIngestReq(t, req)
	closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if strings.Contains(infoBuf.String(), "landed log window") {
		t.Fatalf("log land success must not log at Info; got %q", infoBuf.String())
	}
}
