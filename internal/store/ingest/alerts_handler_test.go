package ingest_test

import (
	"bytes"
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

func newAlertsIngestServer(t *testing.T, allowed []string) (string, *engine.Engine, *httptest.Server) {
	t.Helper()
	dir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, nil)
	t.Cleanup(func() { _ = eng.Close() })
	if allowed == nil {
		allowed = []string{"metrics-raw", "alert-events"}
	}
	cfg := ingest.Config{
		AllowedArtifacts: allowed,
		MaxBodyBytes:     1 << 20,
		AuthMode:         ingest.AuthNone,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.Handle(ingest.IngestRoutePattern(""), ingest.Handler(&cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return dir, eng, srv
}

func TestAlertEventsIngestLandsFile(t *testing.T) {
	dir, eng, srv := newAlertsIngestServer(t, nil)
	resp := postArtifact(t, srv, testTenant, "alert-events", []byte("PAR1-alert-window"))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, b)
	}
	glob := filepath.Join(dir, testTenant, "alerts", "alert-events", "*.parquet")
	m, _ := filepath.Glob(glob)
	if len(m) != 1 {
		t.Fatalf("landed files = %v, want 1 under alerts/alert-events/", m)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot rows = %d, want 0", c)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, testTenant, "logs", "**", "*")); len(m) != 0 {
		t.Fatalf("must not land under logs/: %v", m)
	}
}

func TestAlertEventsIngestEmptyIsNoop(t *testing.T) {
	dir, _, srv := newAlertsIngestServer(t, nil)
	resp := postArtifact(t, srv, testTenant, "alert-events", nil)
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	glob := filepath.Join(dir, testTenant, "alerts", "alert-events", "*.parquet")
	if m, _ := filepath.Glob(glob); len(m) != 0 {
		t.Fatalf("empty body landed files: %v", m)
	}
}

func TestAlertEventsIngestUnknownArtifact(t *testing.T) {
	_, _, srv := newAlertsIngestServer(t, []string{"metrics-raw"})
	resp := postArtifact(t, srv, testTenant, "alert-events", []byte("x"))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when alert-events is not in ALLOWED_ARTIFACTS", resp.StatusCode)
	}
}

func TestAlertEventsIngestUnknownTenant(t *testing.T) {
	_, _, srv := newAlertsIngestServer(t, nil)
	resp := postArtifact(t, srv, "INVALID", "alert-events", []byte("x"))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 unknown tenant", resp.StatusCode)
	}
}

func TestAlertEventsIngestDoesNotUseLogsLand(t *testing.T) {
	dir, _, srv := newAlertsIngestServer(t, []string{"metrics-raw", "logs-summary", "alert-events"})
	resp := postArtifact(t, srv, testTenant, "alert-events", []byte("PAR1-not-logs"))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, testTenant, "logs", "logs-summary", "*")); len(m) != 0 {
		t.Fatalf("alert-events must not land via logs path: %v", m)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, testTenant, "alerts", "alert-events", "*.parquet")); len(m) != 1 {
		t.Fatalf("want 1 alerts land file, got under %s", dir)
	}
}

func TestAlertEventsIngestRejectsUnknownWellFormedArtifact(t *testing.T) {
	_, _, srv := newAlertsIngestServer(t, []string{"metrics-raw", "alert-events"})
	resp := postArtifact(t, srv, testTenant, "alert-history", []byte("x"))
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 unknown artifact", resp.StatusCode)
	}
}

func TestAlertEventsIngestEmptyBodyReader(t *testing.T) {
	dir, _, srv := newAlertsIngestServer(t, nil)
	resp := postArtifact(t, srv, testTenant, "alert-events", bytes.NewBuffer(nil).Bytes())
	defer closeResp(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	glob := filepath.Join(dir, testTenant, "alerts", "alert-events", "*.parquet")
	if m, _ := filepath.Glob(glob); len(m) != 0 {
		t.Fatalf("empty buffer landed files: %v", m)
	}
}
