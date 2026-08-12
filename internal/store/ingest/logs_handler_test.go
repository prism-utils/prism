package ingest_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/ingest"
)

func newLogsIngestServer(t *testing.T) (string, *httptest.Server) {
	t.Helper()
	dir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, nil)
	t.Cleanup(func() { _ = eng.Close() })
	cfg := ingest.Config{
		AllowedArtifacts: []string{"metrics-raw", "logs-summary"},
		MaxBodyBytes:     1 << 20,
		AuthMode:         ingest.AuthNone,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.Handle(ingest.IngestRoutePattern(""), ingest.Handler(&cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return dir, srv
}

func postArtifact(t *testing.T, srv *httptest.Server, tenant, artifact string, body []byte) *http.Response {
	t.Helper()
	url := srv.URL + "/" + tenant + "/ingest/" + artifact
	req := newIngestReq(t, url, bytes.NewReader(body))
	return doIngestReq(t, req)
}

func TestLogsIngestLandsFile(t *testing.T) {
	dir, srv := newLogsIngestServer(t)
	resp := postArtifact(t, srv, testTenant, "logs-summary", []byte("nonempty-window"))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, b)
	}
	glob := filepath.Join(dir, testTenant, "logs", "logs-summary", "*.parquet")
	m, _ := filepath.Glob(glob)
	if len(m) != 1 {
		t.Fatalf("landed files = %v, want 1", m)
	}
}

func TestLogsIngestEmptyIsNoop(t *testing.T) {
	dir, srv := newLogsIngestServer(t)
	resp := postArtifact(t, srv, testTenant, "logs-summary", nil)
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	glob := filepath.Join(dir, testTenant, "logs", "logs-summary", "*.parquet")
	if m, _ := filepath.Glob(glob); len(m) != 0 {
		t.Fatalf("empty body landed files: %v", m)
	}
}

func TestLogsIngestUnknownArtifact(t *testing.T) {
	_, srv := newLogsIngestServer(t)
	// logs-raw is well-formed but not in AllowedArtifacts here → 404.
	resp := postArtifact(t, srv, testTenant, "logs-raw", []byte("x"))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 unknown artifact", resp.StatusCode)
	}
}

func TestLogsIngestClientAbortReturns499(t *testing.T) {
	_, srv := newLogsIngestServer(t)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/"+testTenant+"/ingest/logs-summary", errReader{err: io.ErrUnexpectedEOF})
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()
	srv.Config.Handler.ServeHTTP(rec, req)
	if rec.Code != 499 {
		t.Fatalf("status = %d, want 499 client closed; body=%s", rec.Code, rec.Body.String())
	}
}
