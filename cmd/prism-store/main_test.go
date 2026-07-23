package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/version"
)

func TestHealthz(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil)
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
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
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
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
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
	mux := newServeMux(serverConfig{dataDir: dir}, nil, nil)

	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path, rec.Code)
		}
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	if cfg.listenAddr != defaultListenAddr {
		t.Fatalf("listenAddr = %q, want %q", cfg.listenAddr, defaultListenAddr)
	}
	if cfg.dataDir != defaultDataDir {
		t.Fatalf("dataDir = %q, want %q", cfg.dataDir, defaultDataDir)
	}
	if cfg.flightAddr != "" {
		t.Fatalf("flightAddr = %q, want empty", cfg.flightAddr)
	}
	if cfg.maxBodyBytes != defaultMaxBodyBytes {
		t.Fatalf("maxBodyBytes = %d, want %d", cfg.maxBodyBytes, defaultMaxBodyBytes)
	}
	if len(cfg.allowedArtifacts) != 1 || cfg.allowedArtifacts[0] != "metrics-raw" {
		t.Fatalf("allowedArtifacts = %v", cfg.allowedArtifacts)
	}
	if cfg.authMode != defaultAuthMode {
		t.Fatalf("authMode = %q, want %q", cfg.authMode, defaultAuthMode)
	}
	if cfg.routePrefix != "" {
		t.Fatalf("routePrefix = %q, want empty", cfg.routePrefix)
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("LISTEN_ADDR", ":9090")
	t.Setenv("DATA_DIR", "/tmp/store-data")
	t.Setenv("FLIGHT_ADDR", ":9091")
	t.Setenv("ALLOWED_ARTIFACTS", "metrics-raw,logs-raw")
	t.Setenv("MAX_BODY_BYTES", "1048576")
	t.Setenv("INGEST_TOKEN", "tok")
	t.Setenv("AUTH_MODE", "bearer")
	t.Setenv("ROUTE_PREFIX", "/api")
	cfg := loadConfig()
	if cfg.listenAddr != ":9090" {
		t.Fatalf("listenAddr = %q", cfg.listenAddr)
	}
	if cfg.dataDir != "/tmp/store-data" {
		t.Fatalf("dataDir = %q", cfg.dataDir)
	}
	if cfg.flightAddr != ":9091" {
		t.Fatalf("flightAddr = %q", cfg.flightAddr)
	}
	if len(cfg.allowedArtifacts) != 2 {
		t.Fatalf("allowedArtifacts = %v", cfg.allowedArtifacts)
	}
	if cfg.maxBodyBytes != 1048576 {
		t.Fatalf("maxBodyBytes = %d", cfg.maxBodyBytes)
	}
	if cfg.ingestToken != "tok" {
		t.Fatalf("ingestToken = %q", cfg.ingestToken)
	}
	if cfg.authMode != "bearer" {
		t.Fatalf("authMode = %q", cfg.authMode)
	}
	if cfg.routePrefix != "/api" {
		t.Fatalf("routePrefix = %q", cfg.routePrefix)
	}
}

func clearStoreEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"LISTEN_ADDR", "DATA_DIR", "FLIGHT_ADDR", "ALLOWED_ARTIFACTS",
		"MAX_BODY_BYTES", "INGEST_TOKEN", "AUTH_MODE", "ROUTE_PREFIX",
	} {
		t.Setenv(k, "")
	}
}

func TestVersionLineFormat(t *testing.T) {
	if strings.Contains(versionLine(), "\n") {
		t.Fatal("versionLine must not contain a newline; main adds it via Println")
	}
}
