package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/lifecycle"
	"github.com/elk-utilities/prism/internal/version"
	"go.uber.org/goleak"
)

func TestRunBackgroundLoopStopsOnContextCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	dataDir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	runner := lifecycle.NewRunner(&lifecycle.Config{
		DataDir: dataDir,
		MaxTier: 8,
	}, eng, time.Now)

	cfg := &serverConfig{
		snapshotTick:  5 * time.Millisecond,
		flushTick:     5 * time.Millisecond,
		mergeTick:     5 * time.Millisecond,
		retentionTick: 5 * time.Millisecond,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunBackgroundLoop(ctx, runner, cfg, logger)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("background loop did not exit after context cancel")
	}
}

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
	mux := newServeMux(&serverConfig{dataDir: dir}, nil, nil, planeCombined, nil, nil, nil)

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
	if cfg.queryHotOnly {
		t.Fatalf("queryHotOnly = true, want false by default")
	}
	if !cfg.runJobs {
		t.Fatalf("runJobs = false, want true by default")
	}
	if !cfg.sqlAPIQueueEnabled {
		t.Fatal("sqlAPIQueueEnabled = false, want true by default")
	}
	if cfg.sqlAPIMaxInFlight != 2 {
		t.Fatalf("sqlAPIMaxInFlight = %d, want 2", cfg.sqlAPIMaxInFlight)
	}
	if cfg.sqlAPIMaxQueue != 128 {
		t.Fatalf("sqlAPIMaxQueue = %d, want 128", cfg.sqlAPIMaxQueue)
	}
	if cfg.sqlAPIQueueTimeout != 120*time.Second {
		t.Fatalf("sqlAPIQueueTimeout = %v, want 120s", cfg.sqlAPIQueueTimeout)
	}
	if cfg.maxOpenTenants != 32 {
		t.Fatalf("maxOpenTenants = %d, want 32", cfg.maxOpenTenants)
	}
}

func TestLoadConfigSQLQueueFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("SQL_API_MAX_INFLIGHT", "8")
	t.Setenv("SQL_API_MAX_QUEUE", "256")
	t.Setenv("SQL_API_QUEUE_TIMEOUT_MS", "3000")
	t.Setenv("MAX_OPEN_TENANTS", "16")
	cfg := loadConfig()
	if !cfg.sqlAPIQueueEnabled {
		t.Fatal("sqlAPIQueueEnabled = false, want true")
	}
	if cfg.sqlAPIMaxInFlight != 8 {
		t.Fatalf("sqlAPIMaxInFlight = %d, want 8", cfg.sqlAPIMaxInFlight)
	}
	if cfg.sqlAPIMaxQueue != 256 {
		t.Fatalf("sqlAPIMaxQueue = %d, want 256", cfg.sqlAPIMaxQueue)
	}
	if cfg.sqlAPIQueueTimeout != 3*time.Second {
		t.Fatalf("sqlAPIQueueTimeout = %v, want 3s", cfg.sqlAPIQueueTimeout)
	}
	if cfg.maxOpenTenants != 16 {
		t.Fatalf("maxOpenTenants = %d, want 16", cfg.maxOpenTenants)
	}
}

func TestLoadConfigSQLQueueDisabledByEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("SQL_API_QUEUE_ENABLED", "false")
	if loadConfig().sqlAPIQueueEnabled {
		t.Fatal("SQL_API_QUEUE_ENABLED=false must disable the queue")
	}
}

func TestLoadConfigQueryHotOnlyFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("QUERY_HOT_ONLY", "true")
	cfg := loadConfig()
	if !cfg.queryHotOnly {
		t.Fatal("queryHotOnly = false, want true when QUERY_HOT_ONLY=true")
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
		"QUERY_HOT_ONLY", "RUN_JOBS", "MODE", "CLIENT_TENANTS", "CLUSTER_CLIENTS",
		"SQL_API_QUEUE_ENABLED", "SQL_API_MAX_INFLIGHT", "SQL_API_MAX_QUEUE",
		"SQL_API_QUEUE_TIMEOUT_MS", "MAX_OPEN_TENANTS", "DUCKDB_THREADS", "DUCKDB_MEMORY_LIMIT",
		"HOT_SEGMENT_FORMAT", "MERGE_SEGMENT_FORMAT", "DUCKDB_STORAGE_VERSION",
		"LOGS_REFRESH_INTERVAL", "LOGS_REFRESH_MAX_ACTIONS", "LOGS_DELETE_GRACE_SECONDS",
	} {
		t.Setenv(k, "")
	}
}

func TestVersionLineFormat(t *testing.T) {
	if strings.Contains(versionLine(), "\n") {
		t.Fatal("versionLine must not contain a newline; main adds it via Println")
	}
}
