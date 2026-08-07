package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/admin"
	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/queue"
)

func queueRouteFixture(t *testing.T) (*serverConfig, *engine.Engine, *slog.Logger) {
	t.Helper()
	dir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	return &serverConfig{
		dataDir:          dir,
		allowedArtifacts: []string{"metrics-raw"},
		authMode:         "none",
	}, eng, slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAdminQueueRouteOnAdminPlaneOnly(t *testing.T) {
	cfg, eng, logger := queueRouteFixture(t)
	cfg.adminListenAddr = ":9090"
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 2, MaxQueue: 128, Wait: 2 * time.Minute})

	adminPlane := newServeMux(cfg, eng, logger, planeAdmin, nil, nil, lim)
	rec := httptest.NewRecorder()
	adminPlane.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/queue", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("admin plane /admin/queue = %d, want 200", rec.Code)
	}

	public := newServeMux(cfg, eng, logger, publicPlane(cfg), nil, nil, lim)
	rec2 := httptest.NewRecorder()
	public.ServeHTTP(rec2, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/queue", nil))
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("public plane /admin/queue = %d, want 404", rec2.Code)
	}
}

func TestAdminQueueRouteReportsConfiguredCaps(t *testing.T) {
	cfg, eng, logger := queueRouteFixture(t)
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 2, MaxQueue: 128, Wait: 120 * time.Second})

	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, lim)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/queue", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("combined mux /admin/queue = %d, want 200", rec.Code)
	}

	var got queue.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	want := queue.Snapshot{Enabled: true, MaxInFlight: 2, MaxQueue: 128, TimeoutMs: 120000}
	if got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func TestAdminQueueRouteRequiresAdminToken(t *testing.T) {
	cfg, eng, logger := queueRouteFixture(t)
	cfg.adminToken = "admin-tok"
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 2, MaxQueue: 128, Wait: time.Minute})
	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, lim)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/queue", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("untokened /admin/queue = %d, want 401", rec.Code)
	}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/queue", nil)
	req.Header.Set("Authorization", "Bearer admin-tok")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusOK {
		t.Fatalf("tokened /admin/queue = %d, want 200", rec2.Code)
	}
}

// TestAdminQueueRoutePatternMatchesMux pins the wiring contract: the pattern the
// admin package advertises is the pattern the store actually mounts.
func TestAdminQueueRoutePatternMatchesMux(t *testing.T) {
	cfg, eng, logger := queueRouteFixture(t)
	method, path, ok := splitPattern(admin.QueueRoutePattern())
	if !ok {
		t.Fatalf("pattern %q is not \"METHOD /path\"", admin.QueueRoutePattern())
	}
	mux := newServeMux(cfg, eng, logger, planeAdmin, nil, nil, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), method, path, nil))
	if rec.Code == http.StatusNotFound {
		t.Fatalf("pattern %q not mounted", admin.QueueRoutePattern())
	}
}
