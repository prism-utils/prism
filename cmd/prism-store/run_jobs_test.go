package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/lifecycle"
	"go.uber.org/goleak"
)

func TestLoadConfigRunJobsDefaultsTrue(t *testing.T) {
	clearStoreEnv(t)
	cfg := loadConfig()
	if !cfg.runJobs {
		t.Fatal("runJobs = false, want true by default")
	}
}

func TestLoadConfigRunJobsFalseFromEnv(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("RUN_JOBS", "false")
	cfg := loadConfig()
	if cfg.runJobs {
		t.Fatal("runJobs = true, want false when RUN_JOBS=false")
	}
}

func TestAdminConfigWiresRunJobs(t *testing.T) {
	clearStoreEnv(t)
	t.Setenv("RUN_JOBS", "false")
	cfg := loadConfig()
	adminCfg := cfg.adminConfig()
	if adminCfg.RunJobs {
		t.Fatal("adminConfig.RunJobs = true, want false when RUN_JOBS=false")
	}
	t.Setenv("RUN_JOBS", "true")
	cfg = loadConfig()
	adminCfg = cfg.adminConfig()
	if !adminCfg.RunJobs {
		t.Fatal("adminConfig.RunJobs = false, want true when RUN_JOBS=true")
	}
}

func TestRunServeJobsDisabledNoBackgroundLoop(t *testing.T) {
	defer goleak.VerifyNone(t)

	var loopStarts atomic.Int32
	orig := startBackgroundLoop
	startBackgroundLoop = func(ctx context.Context, runner *lifecycle.Runner, cfg *serverConfig, logger *slog.Logger) {
		loopStarts.Add(1)
		go RunBackgroundLoop(ctx, runner, cfg, logger)
	}
	t.Cleanup(func() { startBackgroundLoop = orig })

	dataDir := t.TempDir()
	publicAddr := freeTCPAddr(t)
	cfg := testServeConfig(dataDir, publicAddr, "")
	cfg.runJobs = false
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runServe(ctx, cfg, logger, nil, nil)
	}()

	waitHTTPReady(t, "http://"+publicAddr)

	for _, path := range []string{"/healthz", "/readyz"} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+publicAddr+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path, resp.StatusCode)
		}
	}

	if got := loopStarts.Load(); got != 0 {
		t.Fatalf("background loop started %d times, want 0 when runJobs=false", got)
	}

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
}

func TestRunServeJobsEnabledStartsBackgroundLoop(t *testing.T) {
	defer goleak.VerifyNone(t)

	var loopStarts atomic.Int32
	orig := startBackgroundLoop
	startBackgroundLoop = func(ctx context.Context, runner *lifecycle.Runner, cfg *serverConfig, logger *slog.Logger) {
		loopStarts.Add(1)
		go RunBackgroundLoop(ctx, runner, cfg, logger)
	}
	t.Cleanup(func() { startBackgroundLoop = orig })

	dataDir := t.TempDir()
	publicAddr := freeTCPAddr(t)
	cfg := testServeConfig(dataDir, publicAddr, "")
	cfg.runJobs = true
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runServe(ctx, cfg, logger, nil, nil)
	}()

	waitHTTPReady(t, "http://"+publicAddr)

	if got := loopStarts.Load(); got != 1 {
		t.Fatalf("background loop started %d times, want 1 when runJobs=true", got)
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runServe: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runServe did not exit after context cancel")
	}
}
