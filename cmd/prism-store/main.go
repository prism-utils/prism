// Command prism-store is the durable tiered columnar store and query server for
// prism pipeline outputs. This slice exposes health endpoints only; ingest,
// engine, and query land in later sub-issues.
//
// Usage:
//
//	prism-store          start the HTTP server (default)
//	prism-store serve    start the HTTP server
//	prism-store version  print version
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/elk-utilities/prism/internal/version"
)

const (
	defaultListenAddr = ":8080"
	defaultDataDir    = "/data"
	readHeaderTimeout = 15 * time.Second
	shutdownTimeout   = 10 * time.Second
)

type serverConfig struct {
	listenAddr string
	dataDir    string
}

func loadConfig() serverConfig {
	return serverConfig{
		listenAddr: envOr("LISTEN_ADDR", defaultListenAddr),
		dataDir:    envOr("DATA_DIR", defaultDataDir),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func handleReadyz(dataDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if err := os.MkdirAll(dataDir, 0o750); err != nil {
			http.Error(w, "data dir not writable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	}
}

func newServeMux(cfg serverConfig) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(cfg.dataDir))
	return mux
}

func versionLine() string {
	return fmt.Sprintf("prism-store %s", version.Version)
}

func runServe(ctx context.Context, cfg serverConfig, logger *slog.Logger) error {
	srv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           newServeMux(cfg),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("prism-store listening", "addr", cfg.listenAddr, "data_dir", cfg.dataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("listen: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	logger.Info("prism-store stopped")
	return nil
}

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "version" {
		fmt.Println(versionLine())
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := runServe(ctx, cfg, logger)
	stop()
	if err != nil {
		logger.Error("prism-store failed", "err", err)
		os.Exit(1)
	}
}
