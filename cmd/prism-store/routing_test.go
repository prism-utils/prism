package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
)

func TestServeMuxSplitPlanes(t *testing.T) {
	dir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := &serverConfig{
		dataDir:          dir,
		allowedArtifacts: []string{"metrics-raw"},
		authMode:         "none",
		adminToken:       "admin-tok",
	}

	public := newServeMux(cfg, eng, logger, planePublic, nil, nil, nil)
	adminPlane := newServeMux(cfg, eng, logger, planeAdmin, nil, nil, nil)

	for _, path := range []string{"/healthz", "/readyz"} {
		rec := httptest.NewRecorder()
		public.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("public %s = %d", path, rec.Code)
		}
		rec2 := httptest.NewRecorder()
		adminPlane.ServeHTTP(rec2, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		if rec2.Code != http.StatusOK {
			t.Fatalf("admin %s = %d", path, rec2.Code)
		}
	}

	rec := httptest.NewRecorder()
	public.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/user-6f3a9c2b-apps/ingest/metrics-raw", nil))
	if rec.Code == http.StatusNotFound {
		t.Fatal("public mux must serve ingest")
	}

	rec = httptest.NewRecorder()
	adminPlane.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/user-6f3a9c2b-apps/ingest/metrics-raw", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("admin mux must not serve ingest, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	adminPlane.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/stats", nil))
	if rec.Code == http.StatusNotFound {
		t.Fatal("admin mux must serve /stats")
	}

	rec = httptest.NewRecorder()
	public.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/stats", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("public mux must not serve /stats when split, got %d", rec.Code)
	}

	const testTenant = "user-6f3a9c2b-apps"
	queryPath := "/" + testTenant + "/query"

	rec = httptest.NewRecorder()
	adminPlane.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, queryPath, nil))
	if rec.Code == http.StatusNotFound {
		t.Fatal("admin mux must serve /{ns}/query")
	}

	rec = httptest.NewRecorder()
	public.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, queryPath, nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("public mux must not serve /{ns}/query when split, got %d", rec.Code)
	}
}

func TestCombinedMuxServesAllRoutes(t *testing.T) {
	dir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := &serverConfig{
		dataDir:          dir,
		allowedArtifacts: []string{"metrics-raw"},
		authMode:         "none",
	}
	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, nil)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/stats", nil))
	if rec.Code == http.StatusNotFound {
		t.Fatal("combined mux must serve /stats")
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/tenants/user-6f3a9c2b-apps/ensure", nil))
	if rec.Code == http.StatusNotFound {
		t.Fatal("combined mux must serve /admin/tenants/{ns}/ensure")
	}
}
