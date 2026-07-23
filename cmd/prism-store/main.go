// Command prism-store is the durable tiered columnar store and query server for
// prism pipeline outputs. This slice exposes health endpoints, HTTP ingest, and
// an optional Arrow Flight DoPut receiver.
//
// Usage:
//
//	prism-store          start the HTTP server (default)
//	prism-store serve    start the HTTP server
//	prism-store print-view-sql --tenant <ns> [--data-dir <dir>]
//	prism-store version  print version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/elk-utilities/prism/internal/store/admin"
	"github.com/elk-utilities/prism/internal/store/engine"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/lifecycle"
	"github.com/elk-utilities/prism/internal/store/query"
	"github.com/elk-utilities/prism/internal/version"
)

const (
	defaultListenAddr        = ":8080"
	defaultDataDir           = "/data"
	defaultMaxBodyBytes      = 268435456
	defaultArtifacts         = "metrics-raw"
	defaultAuthMode          = "none"
	defaultHotWindowMinutes  = 10
	defaultSegmentsPerTier   = 6
	defaultMaxSegmentBytes   = 2147483648
	defaultRetentionDays     = 15
	defaultRollupSteps       = "1m,5m,1h"
	defaultMaxTier           = 8
	defaultHotSnapshotSec    = 15
	defaultFlushTickSec      = 30
	defaultMergeTickSec      = 60
	defaultRetentionTickHour = 1
	readHeaderTimeout        = 15 * time.Second
	shutdownTimeout          = 10 * time.Second
)

type servePlane int

const (
	planeCombined servePlane = iota
	planePublic
	planeAdmin
)

type serverConfig struct {
	listenAddr       string
	adminListenAddr  string
	flightAddr       string
	dataDir          string
	allowedArtifacts []string
	maxBodyBytes     int64
	ingestToken      string
	adminToken       string
	authMode         string
	routePrefix      string
	hotWindow        time.Duration
	segmentsPerTier  int
	maxSegmentBytes  int64
	retentionDays    int
	rollupSteps      string
	maxTier          int
	snapshotTick     time.Duration
	flushTick        time.Duration
	mergeTick        time.Duration
	retentionTick    time.Duration
}

func loadConfig() serverConfig {
	c := serverConfig{
		listenAddr:      envOr("LISTEN_ADDR", defaultListenAddr),
		adminListenAddr: os.Getenv("ADMIN_LISTEN_ADDR"),
		adminToken:      os.Getenv("ADMIN_TOKEN"),
		flightAddr:      os.Getenv("FLIGHT_ADDR"),
		dataDir:         envOr("DATA_DIR", defaultDataDir),
		maxBodyBytes:    defaultMaxBodyBytes,
		ingestToken:     os.Getenv("INGEST_TOKEN"),
		authMode:        envOr("AUTH_MODE", defaultAuthMode),
		routePrefix:     os.Getenv("ROUTE_PREFIX"),
		hotWindow:       loadHotWindow(),
		segmentsPerTier: envInt("SEGMENTS_PER_TIER", defaultSegmentsPerTier),
		maxSegmentBytes: envInt64("MAX_SEGMENT_BYTES", defaultMaxSegmentBytes),
		retentionDays:   envInt("RETENTION_DAYS", defaultRetentionDays),
		rollupSteps:     envOr("ROLLUP_STEPS", defaultRollupSteps),
		maxTier:         envInt("MAX_TIER", defaultMaxTier),
		snapshotTick:    time.Duration(envInt("HOT_SNAPSHOT_SECONDS", defaultHotSnapshotSec)) * time.Second,
		flushTick:       time.Duration(envInt("FLUSH_TICK_SECONDS", defaultFlushTickSec)) * time.Second,
		mergeTick:       time.Duration(envInt("MERGE_TICK_SECONDS", defaultMergeTickSec)) * time.Second,
		retentionTick:   loadRetentionTick(),
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

func loadHotWindow() time.Duration {
	if v := os.Getenv("HOT_WINDOW_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return time.Duration(envInt("HOT_WINDOW_MINUTES", defaultHotWindowMinutes)) * time.Minute
}

func loadRetentionTick() time.Duration {
	if v := os.Getenv("RETENTION_TICK_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return time.Duration(envInt("RETENTION_TICK_HOURS", defaultRetentionTickHour)) * time.Hour
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
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

func (c *serverConfig) adminConfig() *admin.Config {
	return &admin.Config{
		DataDir:          c.dataDir,
		AllowedArtifacts: c.allowedArtifacts,
		AdminToken:       c.adminToken,
		RoutePrefix:      c.routePrefix,
	}
}

func publicPlane(cfg *serverConfig) servePlane {
	if cfg.adminListenAddr != "" {
		return planePublic
	}
	return planeCombined
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

func newServeMux(cfg *serverConfig, eng *engine.Engine, logger *slog.Logger, plane servePlane) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(cfg.dataDir))
	if eng == nil || logger == nil {
		return mux
	}

	mode, err := storeingest.ParseAuthMode(cfg.authMode)
	if err != nil {
		logger.Error("invalid auth mode", "auth_mode", cfg.authMode, "err", err)
		return mux
	}

	serveIngest := plane == planeCombined || plane == planePublic
	serveAdmin := plane == planeCombined || plane == planeAdmin
	if !serveIngest && !serveAdmin {
		return mux
	}

	adminCfg := cfg.adminConfig()
	queryCfg := &query.Config{
		DataDir:     cfg.dataDir,
		RoutePrefix: cfg.routePrefix,
		ExposeSQL:   query.ExposeSQLFromEnv(),
	}

	if serveIngest {
		ingestCfg := cfg.ingestConfig(mode)
		mux.Handle(storeingest.IngestRoutePattern(cfg.routePrefix), storeingest.Handler(&ingestCfg, eng, logger))
	}
	if serveAdmin {
		mux.Handle(admin.EnsureRoutePattern(), admin.WithBearerAuth(adminCfg.AdminToken, admin.EnsureHandler(adminCfg, eng, logger)))
		mux.Handle(admin.StatsRoutePattern(), admin.WithBearerAuth(adminCfg.AdminToken, admin.StatsHandler(adminCfg, eng)))
		mux.Handle(query.QueryRoutePattern(cfg.routePrefix), admin.WithBearerAuth(adminCfg.AdminToken, query.Handler(queryCfg, eng, logger)))
	}
	return mux
}

func versionLine() string {
	return fmt.Sprintf("prism-store %s", version.Version)
}

// RunBackgroundLoop runs lifecycle tickers until ctx is cancelled.
func RunBackgroundLoop(ctx context.Context, runner *lifecycle.Runner, cfg *serverConfig, logger *slog.Logger) {
	snapshotTick := time.NewTicker(cfg.snapshotTick)
	flushTick := time.NewTicker(cfg.flushTick)
	mergeTick := time.NewTicker(cfg.mergeTick)
	retentionTick := time.NewTicker(cfg.retentionTick)
	defer snapshotTick.Stop()
	defer flushTick.Stop()
	defer mergeTick.Stop()
	defer retentionTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-snapshotTick.C:
			if err := runner.TickHotSnapshot(); err != nil {
				logger.Error("hot snapshot tick", "err", err)
			}
		case <-flushTick.C:
			if err := runner.TickFlush(); err != nil {
				logger.Error("flush tick", "err", err)
			}
		case <-mergeTick.C:
			if err := runner.TickMerge(); err != nil {
				logger.Error("merge tick", "err", err)
			}
		case <-retentionTick.C:
			if err := runner.TickRetention(); err != nil {
				logger.Error("retention tick", "err", err)
			}
		}
	}
}

func runServe(ctx context.Context, cfg *serverConfig, logger *slog.Logger) error {
	mode, err := storeingest.ParseAuthMode(cfg.authMode)
	if err != nil {
		return fmt.Errorf("auth mode: %w", err)
	}

	now := time.Now
	eng := engine.New(engine.Config{
		DataDir:   cfg.dataDir,
		HotWindow: cfg.hotWindow,
	}, now)
	defer func() { _ = eng.Close() }()

	runner := lifecycle.NewRunner(lifecycle.Config{
		DataDir:         cfg.dataDir,
		SegmentsPerTier: cfg.segmentsPerTier,
		MaxSegmentBytes: cfg.maxSegmentBytes,
		FloorBytes:      lifecycle.FloorBytesFromHotWindow(cfg.hotWindow),
		RetentionDays:   cfg.retentionDays,
		RollupSteps:     cfg.rollupSteps,
		MaxTier:         cfg.maxTier,
	}, eng, now)

	go RunBackgroundLoop(ctx, runner, cfg, logger)

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
		Handler:           newServeMux(cfg, eng, logger, publicPlane(cfg)),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	var adminSrv *http.Server
	if cfg.adminListenAddr != "" {
		adminSrv = &http.Server{
			Addr:              cfg.adminListenAddr,
			Handler:           newServeMux(cfg, eng, logger, planeAdmin),
			ReadHeaderTimeout: readHeaderTimeout,
		}
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("prism-store listening",
			"addr", cfg.listenAddr,
			"data_dir", cfg.dataDir,
			"auth_mode", cfg.authMode,
			"hot_window", cfg.hotWindow.String(),
			"segments_per_tier", cfg.segmentsPerTier,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("public listen: %w", err)
		}
	}()
	if adminSrv != nil {
		go func() {
			logger.Info("prism-store admin listening", "addr", cfg.adminListenAddr)
			if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("admin listen: %w", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if adminSrv != nil {
		if err := adminSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("admin shutdown: %w", err)
		}
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

func runPrintViewSQL(args []string) error {
	fs := flag.NewFlagSet("print-view-sql", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant namespace")
	dataDir := fs.String("data-dir", envOr("DATA_DIR", defaultDataDir), "shared data root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return fmt.Errorf("--tenant is required")
	}
	sqlText, err := query.ViewSQL(*dataDir, *tenant)
	if err != nil {
		return err
	}
	fmt.Println(sqlText)
	return nil
}

func main() {
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "version":
			fmt.Println(versionLine())
			return
		case "print-view-sql":
			if err := runPrintViewSQL(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "print-view-sql: %v\n", err)
				os.Exit(1)
			}
			return
		case "serve":
			// default server path below
		}
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
