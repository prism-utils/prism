package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func testServeConfig(dataDir, publicAddr, adminAddr string) *serverConfig {
	return &serverConfig{
		listenAddr:       publicAddr,
		adminListenAddr:  adminAddr,
		dataDir:          dataDir,
		allowedArtifacts: []string{"metrics-raw"},
		authMode:         "none",
		hotWindow:        time.Hour,
		segmentsPerTier:  6,
		maxSegmentBytes:  2147483648,
		retentionDays:    15,
		rollupSteps:      "1m,5m,1h",
		maxTier:          8,
		snapshotTick:     time.Hour,
		flushTick:        time.Hour,
		mergeTick:        time.Hour,
		retentionTick:    time.Hour,
		runJobs:          true,
	}
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func waitHTTPReady(t *testing.T, baseURL string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server not ready at %s", baseURL)
}

func assertTCPRefused(t *testing.T, addr string) {
	t.Helper()
	d := net.Dialer{Timeout: 500 * time.Millisecond}
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("expected connection refused on %s", addr)
	}
}

func TestRunServeDualShutdownStopsBothServers(t *testing.T) {
	defer goleak.VerifyNone(t)

	dataDir := t.TempDir()
	publicAddr := freeTCPAddr(t)
	adminAddr := freeTCPAddr(t)
	cfg := testServeConfig(dataDir, publicAddr, adminAddr)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runServe(ctx, cfg, logger)
	}()

	waitHTTPReady(t, "http://"+publicAddr)
	waitHTTPReady(t, "http://"+adminAddr)

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runServe: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runServe did not exit after context cancel")
	}

	assertTCPRefused(t, publicAddr)
	assertTCPRefused(t, adminAddr)
}

func TestRunServePublicListenAddrInUse(t *testing.T) {
	lc := net.ListenConfig{}
	occupied, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()
	addr := occupied.Addr().String()

	dataDir := t.TempDir()
	cfg := testServeConfig(dataDir, addr, "")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- runServe(ctx, cfg, logger)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected bind error, got nil")
		}
		if !strings.Contains(err.Error(), "public listen") {
			t.Fatalf("error = %v, want public listen failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServe did not return bind error")
	}
}

func TestRunServeAdminListenAddrInUse(t *testing.T) {
	defer goleak.VerifyNone(t)

	publicAddr := freeTCPAddr(t)
	lc := net.ListenConfig{}
	occupied, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()
	adminAddr := occupied.Addr().String()

	dataDir := t.TempDir()
	cfg := testServeConfig(dataDir, publicAddr, adminAddr)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- runServe(ctx, cfg, logger)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected bind error, got nil")
		}
		if !strings.Contains(err.Error(), "admin listen") {
			t.Fatalf("error = %v, want admin listen failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServe did not return bind error")
	}
}
