package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/version"
)

func TestHealthz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handleHealthz(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != "ok\n" {
		t.Fatalf("body = %q, want %q", body, "ok\n")
	}
}

func TestReadyzWritable(t *testing.T) {
	dir := t.TempDir()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handleReadyz(dir)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); body != "ready\n" {
		t.Fatalf("body = %q, want %q", body, "ready\n")
	}
}

func TestReadyzUnwritable(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handleReadyz(blocker)(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestVersionOutput(t *testing.T) {
	got := versionLine()
	want := "prism-store " + version.Version
	if got != want {
		t.Fatalf("versionLine = %q, want %q", got, want)
	}
}

func TestNewServeMuxRoutes(t *testing.T) {
	dir := t.TempDir()
	mux := newServeMux(serverConfig{dataDir: dir})

	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path, rec.Code)
		}
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("DATA_DIR", "")
	cfg := loadConfig()
	if cfg.listenAddr != defaultListenAddr {
		t.Fatalf("listenAddr = %q, want %q", cfg.listenAddr, defaultListenAddr)
	}
	if cfg.dataDir != defaultDataDir {
		t.Fatalf("dataDir = %q, want %q", cfg.dataDir, defaultDataDir)
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv("LISTEN_ADDR", ":9090")
	t.Setenv("DATA_DIR", "/tmp/store-data")
	cfg := loadConfig()
	if cfg.listenAddr != ":9090" {
		t.Fatalf("listenAddr = %q", cfg.listenAddr)
	}
	if cfg.dataDir != "/tmp/store-data" {
		t.Fatalf("dataDir = %q", cfg.dataDir)
	}
}

func TestVersionLineFormat(t *testing.T) {
	if strings.Contains(versionLine(), "\n") {
		t.Fatal("versionLine must not contain a newline; main adds it via Println")
	}
}
