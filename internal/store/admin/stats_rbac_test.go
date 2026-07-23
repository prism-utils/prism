package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/admin"
	"github.com/elk-utilities/prism/internal/store/engine"
)

func TestStatsHandlerRBACFailClosedWithoutScope(t *testing.T) {
	dir := t.TempDir()
	for _, ns := range []string{"user-6f3a9c2b-apps", "user-7a4b1c9d-web"} {
		if err := os.MkdirAll(filepath.Join(dir, ns), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	cfg := &admin.Config{
		DataDir:          dir,
		AllowedArtifacts: []string{"metrics-raw"},
		RBACEnabled:      true,
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/stats", nil)
	admin.StatsHandler(cfg, eng).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 fail-closed without stats scope", rec.Code)
	}
	var resp admin.StatsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err == nil && resp.TotalWindows > 0 {
		t.Fatalf("unexpected stats payload on deny: %+v", resp)
	}
}

func TestStatsHandlerRBACOffAllowsUnscopedAllTenantScan(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "user-6f3a9c2b-apps"), 0o750); err != nil {
		t.Fatal(err)
	}
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	cfg := &admin.Config{
		DataDir:          dir,
		AllowedArtifacts: []string{"metrics-raw"},
		RBACEnabled:      false,
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/stats", nil)
	admin.StatsHandler(cfg, eng).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		body, _ := io.ReadAll(rec.Body)
		t.Fatalf("rbac-off stats = %d body %s", rec.Code, body)
	}
}
