package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestLoadConfigModeDefaultStandalone(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("MODE", "")
	cfg := loadConfig()
	if cfg.mode != "standalone" {
		t.Fatalf("mode = %q, want standalone", cfg.mode)
	}
}

func TestLoadConfigModeFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("MODE", "cluster")
	cfg := loadConfig()
	if cfg.mode != "cluster" {
		t.Fatalf("mode = %q, want cluster", cfg.mode)
	}
}

func TestRunStoreInvalidMode(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	cfg.mode = "invalid-mode"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	err := runStore(context.Background(), &cfg, logger)
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
	if !strings.Contains(err.Error(), "mode") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunClusterNoEngineDataDir(t *testing.T) {
	defer goleak.VerifyNone(t)

	dataDir := t.TempDir()
	publicAddr := freeTCPAddr(t)

	var upstreamHits atomic.Int32
	up := httptestUpstream(t, &upstreamHits)
	t.Cleanup(up.Close)

	clearStoreEnv(t)
	cfg := testServeConfig(dataDir, publicAddr, "")
	cfg.mode = "cluster"
	cfg.clusterClients = validTenantA + "=" + up.URL

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runStore(ctx, cfg, logger)
	}()

	waitHTTPReady(t, "http://"+publicAddr)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runStore cluster: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cluster server did not shut down")
	}

	up.Close()

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cluster mode created data under DATA_DIR: %v", entries)
	}
}

func TestRunClusterGracefulShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)

	dataDir := t.TempDir()
	publicAddr := freeTCPAddr(t)

	var hits atomic.Int32
	up := httptestUpstream(t, &hits)
	t.Cleanup(up.Close)

	cfg := testServeConfig(dataDir, publicAddr, "")
	cfg.mode = "cluster"
	cfg.clusterClients = validTenantA + "=" + up.URL
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runStore(ctx, cfg, logger)
	}()

	waitHTTPReady(t, "http://"+publicAddr)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runStore: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runCluster did not exit after context cancel")
	}

	assertTCPRefused(t, publicAddr)
	up.Close()
}

func TestRunClusterListenAddrInUse(t *testing.T) {
	lc := net.ListenConfig{}
	occupied, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()
	addr := occupied.Addr().String()

	var hits atomic.Int32
	up := httptestUpstream(t, &hits)
	t.Cleanup(up.Close)

	dataDir := t.TempDir()
	cfg := testServeConfig(dataDir, addr, "")
	cfg.mode = "cluster"
	cfg.clusterClients = validTenantA + "=" + up.URL
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- runStore(ctx, cfg, logger)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected bind error, got nil")
		}
		if !strings.Contains(err.Error(), "listen") {
			t.Fatalf("error = %v, want listen failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runCluster did not return bind error")
	}
}

func TestRunClientModeRequiresOwnedTenants(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	cfg.mode = "client"
	cfg.clientTenants = ""
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	err := runStore(context.Background(), &cfg, logger)
	if err == nil {
		t.Fatal("expected error when CLIENT_TENANTS empty in client mode")
	}
	if !strings.Contains(err.Error(), "CLIENT_TENANTS") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunClusterRequiresClients(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	cfg.mode = "cluster"
	cfg.clusterClients = ""
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	err := runStore(context.Background(), &cfg, logger)
	if err == nil {
		t.Fatal("expected error when CLUSTER_CLIENTS empty in cluster mode")
	}
	if !strings.Contains(err.Error(), "CLUSTER_CLIENTS") {
		t.Fatalf("err = %v", err)
	}
}

const validTenantA = "user-6f3a9c2b-apps"

func httptestUpstream(t *testing.T, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
}

func TestRunClientModeGuardIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)

	dataDir := t.TempDir()
	publicAddr := freeTCPAddr(t)

	cfg := testServeConfig(dataDir, publicAddr, "")
	cfg.mode = "client"
	cfg.clientTenants = validTenantA
	cfg.runJobs = false
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runStore(ctx, cfg, logger)
	}()

	waitHTTPReady(t, "http://"+publicAddr)

	otherTenant := "user-7a4b1c9d-web"
	resp, err := http.Get("http://" + publicAddr + "/" + otherTenant + "/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("non-owned tenant status = %d, want 404", resp.StatusCode)
	}

	if err := os.MkdirAll(filepath.Join(dataDir, otherTenant), 0o750); err != nil {
		t.Fatal(err)
	}
	resp2, err := http.Get("http://" + publicAddr + "/" + otherTenant + "/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("guard must reject even when tenant dir exists: status = %d", resp2.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runStore: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("client server did not shut down")
	}
}
