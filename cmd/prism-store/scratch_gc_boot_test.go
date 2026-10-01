package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/lifecycle"
	"go.uber.org/goleak"
)

const bootGCTenant = "user-gcboot-apps"

func TestRunServeJobsEnabledRunsScratchGC(t *testing.T) {
	defer goleak.VerifyNone(t)

	var loopStarts atomic.Int32
	orig := startBackgroundLoop
	startBackgroundLoop = func(context.Context, *lifecycle.Runner, *serverConfig, *slog.Logger) {
		loopStarts.Add(1)
	}
	t.Cleanup(func() { startBackgroundLoop = orig })

	dataDir := t.TempDir()
	tmp, live := plantBootScratch(t, dataDir, time.Now().Add(-3*time.Minute))

	publicAddr := freeTCPAddr(t)
	cfg := testServeConfig(dataDir, publicAddr, "")
	cfg.runJobs = true
	cfg.deleteGrace = 0
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	}()
	go func() {
		done <- runServe(ctx, cfg, logger, nil, nil)
	}()
	waitHTTPReady(t, "http://"+publicAddr)

	if got := loopStarts.Load(); got != 1 {
		t.Fatalf("background loop started %d times, want 1 when runJobs=true", got)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("boot scratch still present with RUN_JOBS=true, stat err = %v", err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("live snapshot missing: %v", err)
	}
}

func TestRunServeJobsDisabledSkipsScratchGC(t *testing.T) {
	defer goleak.VerifyNone(t)

	var loopStarts atomic.Int32
	orig := startBackgroundLoop
	startBackgroundLoop = func(context.Context, *lifecycle.Runner, *serverConfig, *slog.Logger) {
		loopStarts.Add(1)
	}
	t.Cleanup(func() { startBackgroundLoop = orig })

	dataDir := t.TempDir()
	tmp, live := plantBootScratch(t, dataDir, time.Now().Add(-3*time.Minute))

	publicAddr := freeTCPAddr(t)
	cfg := testServeConfig(dataDir, publicAddr, "")
	cfg.runJobs = false
	cfg.deleteGrace = 0
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	}()
	go func() {
		done <- runServe(ctx, cfg, logger, nil, nil)
	}()
	waitHTTPReady(t, "http://"+publicAddr)

	if got := loopStarts.Load(); got != 0 {
		t.Fatalf("background loop started %d times, want 0 when runJobs=false", got)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("scratch must remain when RUN_JOBS=false: %v", err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("live snapshot missing: %v", err)
	}
}

func plantBootScratch(t *testing.T, dataDir string, mtime time.Time) (tmp, live string) {
	t.Helper()
	hot := filepath.Join(dataDir, bootGCTenant, "hot")
	if err := os.MkdirAll(hot, 0o750); err != nil {
		t.Fatal(err)
	}
	tmp = filepath.Join(hot, "current.duckdb.deadbeef.tmp")
	live = filepath.Join(hot, "current.duckdb")
	if err := os.WriteFile(tmp, []byte("scratch"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(live, []byte("live"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(tmp, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	return tmp, live
}
