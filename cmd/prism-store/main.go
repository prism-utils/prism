// Command prism-store is the durable tiered columnar store and query server for
// prism pipeline outputs. This slice exposes health endpoints, HTTP ingest, and
// an optional Arrow Flight DoPut receiver.
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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/version"
)

const (
	defaultListenAddr   = ":8080"
	defaultDataDir      = "/data"
	defaultMaxBodyBytes = 268435456
	defaultArtifacts    = "metrics-raw"
	defaultAuthMode     = "none"
	readHeaderTimeout   = 15 * time.Second
	shutdownTimeout     = 10 * time.Second
)

type serverConfig struct {
	listenAddr       string
	flightAddr       string
	dataDir          string
	allowedArtifacts []string
	maxBodyBytes     int64
	ingestToken      string
	authMode         string
	routePrefix      string
}

func loadConfig() serverConfig {
	c := serverConfig{
		listenAddr:   envOr("LISTEN_ADDR", defaultListenAddr),
		flightAddr:   os.Getenv("FLIGHT_ADDR"),
		dataDir:      envOr("DATA_DIR", defaultDataDir),
		maxBodyBytes: defaultMaxBodyBytes,
		ingestToken:  os.Getenv("INGEST_TOKEN"),
		authMode:     envOr("AUTH_MODE", defaultAuthMode),
		routePrefix:  os.Getenv("ROUTE_PREFIX"),
	}
	if v := os.Getenv("MAX_BODY_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			c.maxBodyBytes = n
		}
	}
	for _, a := range strings.Split(envOr("ALLOWED_ARTIFACTS", defaultArtifacts), ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.allowedArtifacts = append(c.allowedArtifacts, a)
		}
	}
	return c
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (c *serverConfig) ingestConfig(mode storeingest.AuthMode) storeingest.Config {
	return storeingest.Config{
		AllowedArtifacts: c.allowedArtifacts,
		MaxBodyBytes:     c.maxBodyBytes,
		IngestToken:      c.ingestToken,
		AuthMode:         mode,
		RoutePrefix:      c.routePrefix,
	}
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

func newServeMux(cfg *serverConfig, eng *engine.Engine, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(cfg.dataDir))
	if eng != nil && logger != nil {
		mode, err := storeingest.ParseAuthMode(cfg.authMode)
		if err != nil {
			logger.Error("invalid auth mode", "auth_mode", cfg.authMode, "err", err)
		} else {
			ingestCfg := cfg.ingestConfig(mode)
			mux.Handle(storeingest.IngestRoutePattern(cfg.routePrefix), storeingest.Handler(&ingestCfg, eng, logger))
		}
	}
	return mux
}

func versionLine() string {
	return fmt.Sprintf("prism-store %s", version.Version)
}

func runServe(ctx context.Context, cfg *serverConfig, logger *slog.Logger) error {
	mode, err := storeingest.ParseAuthMode(cfg.authMode)
	if err != nil {
		return fmt.Errorf("auth mode: %w", err)
	}

	eng := engine.New(engine.Config{DataDir: cfg.dataDir}, nil)
	defer func() { _ = eng.Close() }()

	ingestCfg := cfg.ingestConfig(mode)

	var flightDone chan error
	if cfg.flightAddr != "" {
		flightSrv, err := storeingest.NewFlightServer(&ingestCfg, eng, logger)
		if err != nil {
			return fmt.Errorf("flight server: %w", err)
		}
		flightDone = make(chan error, 1)
		go func() {
			flightDone <- flightSrv.Serve(ctx, cfg.flightAddr, func(bound string) {
				logger.Info("prism-store flight listening", "addr", bound)
			})
		}()
	}

	srv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           newServeMux(cfg, eng, logger),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("prism-store listening", "addr", cfg.listenAddr, "data_dir", cfg.dataDir, "auth_mode", cfg.authMode)
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
	if flightDone != nil {
		select {
		case err := <-flightDone:
			if err != nil {
				return fmt.Errorf("flight shutdown: %w", err)
			}
		case <-time.After(shutdownTimeout):
			return fmt.Errorf("flight shutdown: timeout")
		}
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
	err := runServe(ctx, &cfg, logger)
	stop()
	if err != nil {
		logger.Error("prism-store failed", "err", err)
		os.Exit(1)
	}
}
