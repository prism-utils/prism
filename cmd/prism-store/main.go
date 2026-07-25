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
//
// Query hot-only mode: set QUERY_HOT_ONLY=true to serve HTTP queries from the
// in-memory hot cache only (no tier or rollup Parquet reads). Default false.
// Arbitrary SQL API: POST /{ns}/sql (RBAC action query, or ADMIN_TOKEN when RBAC
// off). Disable with SQL_API_ENABLED=false. Limits: SQL_API_MAX_ROWS (default
// 100000), SQL_API_TIMEOUT_SECONDS (default 30). Reuses DUCKDB_MEMORY_LIMIT.
// Background jobs: set RUN_JOBS=false to skip all lifecycle maintenance
// (snapshot, flush, merge, rollups, retention). Default true. Ingest and query
// still run; hot data will not flush or compact and retention will not delete.
// Grafana print-view-sql is unaffected.
//
// Deployment MODE (default standalone):
//   - standalone — self-contained store (engine, ingest, jobs).
//   - client — data-holding leaf; CLIENT_TENANTS (required) lists owned tenants.
//     Queries for other tenants return 404 before the engine runs.
//   - cluster — stateless query coordinator; CLUSTER_CLIENTS maps tenant=url pairs.
//     Forwards GET /{ns}/query and POST /{ns}/sql to the owning client; no engine, ingest, or jobs.
//
// RBAC (optional): set AUTHZ_POLICY_FILE to a deny-by-default YAML policy path.
// When set, JWT/OIDC auth (OIDC_ISSUER, OIDC_AUDIENCE, OIDC_JWKS_*) is required
// and supersedes ADMIN_TOKEN/INGEST_TOKEN on HTTP query/ingest/admin routes.
// AUTH_MODE remains for RBAC-off and for Arrow Flight. See docs/STORE.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/elk-utilities/prism/internal/store/admin"
	"github.com/elk-utilities/prism/internal/store/cluster"
	"github.com/elk-utilities/prism/internal/store/engine"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/lifecycle"
	"github.com/elk-utilities/prism/internal/store/query"
	"github.com/elk-utilities/prism/internal/store/queue"
	"github.com/elk-utilities/prism/internal/version"
)

const (
	defaultListenAddr         = ":8080"
	defaultDataDir            = "/data"
	defaultMaxBodyBytes       = 268435456
	defaultArtifacts          = "metrics-raw"
	defaultAuthMode           = "none"
	defaultHotWindowMinutes   = 10
	defaultSegmentsPerTier    = 6
	defaultMaxSegmentBytes    = 2147483648
	defaultRetentionDays      = 15
	defaultRollupSteps        = "1m,5m,1h"
	defaultMaxTier            = 8
	defaultHotSnapshotSec     = 15
	defaultFlushTickSec       = 30
	defaultMergeTickSec       = 60
	defaultRetentionTickHour  = 1
	defaultSQLAPIMaxInFlight  = 4
	defaultSQLAPIMaxQueue     = 64
	defaultSQLAPIQueueTimeout = 5000
	defaultMaxOpenTenants     = 32
	readHeaderTimeout         = 15 * time.Second
	shutdownTimeout           = 10 * time.Second
)

type servePlane int

const (
	planeCombined servePlane = iota
	planePublic
	planeAdmin
)

type serverConfig struct {
	listenAddr         string
	adminListenAddr    string
	flightAddr         string
	dataDir            string
	allowedArtifacts   []string
	maxBodyBytes       int64
	ingestToken        string
	adminToken         string
	authMode           string
	routePrefix        string
	hotWindow          time.Duration
	segmentsPerTier    int
	maxSegmentBytes    int64
	retentionDays      int
	rollupSteps        string
	maxTier            int
	snapshotTick       time.Duration
	flushTick          time.Duration
	mergeTick          time.Duration
	retentionTick      time.Duration
	duckdbThreads      int
	duckdbMemoryLimit  string
	queryHotOnly       bool
	sqlAPIEnabled      bool
	sqlAPIMaxRows      int
	sqlAPITimeout      time.Duration
	sqlAPIMaxBodyBytes int64
	promqlAPIEnabled   bool
	runJobs            bool
	mode               string
	clientTenants      string
	clusterClients     string
	sqlAPIQueueEnabled bool
	sqlAPIMaxInFlight  int
	sqlAPIMaxQueue     int
	sqlAPIQueueTimeout time.Duration
	maxOpenTenants     int
	rbac               *rbacConfig
}

func loadConfig() serverConfig {
	c := serverConfig{
		listenAddr:         envOr("LISTEN_ADDR", defaultListenAddr),
		adminListenAddr:    os.Getenv("ADMIN_LISTEN_ADDR"),
		adminToken:         os.Getenv("ADMIN_TOKEN"),
		flightAddr:         os.Getenv("FLIGHT_ADDR"),
		dataDir:            envOr("DATA_DIR", defaultDataDir),
		maxBodyBytes:       defaultMaxBodyBytes,
		ingestToken:        os.Getenv("INGEST_TOKEN"),
		authMode:           envOr("AUTH_MODE", defaultAuthMode),
		routePrefix:        os.Getenv("ROUTE_PREFIX"),
		hotWindow:          loadHotWindow(),
		segmentsPerTier:    envInt("SEGMENTS_PER_TIER", defaultSegmentsPerTier),
		maxSegmentBytes:    envInt64("MAX_SEGMENT_BYTES", defaultMaxSegmentBytes),
		retentionDays:      envInt("RETENTION_DAYS", defaultRetentionDays),
		rollupSteps:        envOr("ROLLUP_STEPS", defaultRollupSteps),
		maxTier:            envInt("MAX_TIER", defaultMaxTier),
		snapshotTick:       time.Duration(envInt("HOT_SNAPSHOT_SECONDS", defaultHotSnapshotSec)) * time.Second,
		flushTick:          time.Duration(envInt("FLUSH_TICK_SECONDS", defaultFlushTickSec)) * time.Second,
		mergeTick:          time.Duration(envInt("MERGE_TICK_SECONDS", defaultMergeTickSec)) * time.Second,
		retentionTick:      loadRetentionTick(),
		duckdbThreads:      envIntZero("DUCKDB_THREADS"),
		duckdbMemoryLimit:  os.Getenv("DUCKDB_MEMORY_LIMIT"),
		queryHotOnly:       envBool("QUERY_HOT_ONLY", false),
		sqlAPIEnabled:      envBool("SQL_API_ENABLED", true),
		sqlAPIMaxRows:      envInt("SQL_API_MAX_ROWS", 100000),
		sqlAPITimeout:      time.Duration(envInt("SQL_API_TIMEOUT_SECONDS", 30)) * time.Second,
		sqlAPIMaxBodyBytes: envInt64("SQL_API_MAX_BODY_BYTES", 1<<20),
		promqlAPIEnabled:   envBool("PROMQL_API_ENABLED", true),
		runJobs:            envBool("RUN_JOBS", true),
		mode:               envOr("MODE", "standalone"),
		clientTenants:      os.Getenv("CLIENT_TENANTS"),
		clusterClients:     os.Getenv("CLUSTER_CLIENTS"),
		sqlAPIQueueEnabled: envBool("SQL_API_QUEUE_ENABLED", false),
		sqlAPIMaxInFlight:  envInt("SQL_API_MAX_INFLIGHT", defaultSQLAPIMaxInFlight),
		sqlAPIMaxQueue:     envInt("SQL_API_MAX_QUEUE", defaultSQLAPIMaxQueue),
		sqlAPIQueueTimeout: time.Duration(envInt("SQL_API_QUEUE_TIMEOUT_MS", defaultSQLAPIQueueTimeout)) * time.Millisecond,
		maxOpenTenants:     envInt("MAX_OPEN_TENANTS", defaultMaxOpenTenants),
	}
	c.rbac = loadRBACConfig()
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

func envIntZero(key string) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
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

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
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
		RBACEnabled:      c.rbac != nil,
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

func newServeMux(cfg *serverConfig, eng *engine.Engine, logger *slog.Logger, plane servePlane, ownedTenants map[string]struct{}, rbac *rbacStack, sqlLimiter *queue.Limiter) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /readyz", handleReadyz(cfg.dataDir))
	if eng == nil || logger == nil {
		return mux
	}

	mode, err := httpIngestAuthMode(cfg, rbac)
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
		HotOnly:     cfg.queryHotOnly,
	}

	if serveIngest {
		ingestCfg := cfg.ingestConfig(mode)
		ingestHandler := storeingest.Handler(&ingestCfg, eng, logger)
		if rbac != nil {
			ingestHandler = rbac.wrapIngest(ingestHandler)
		}
		mux.Handle(storeingest.IngestRoutePattern(cfg.routePrefix), ingestHandler)
	}
	if serveAdmin {
		ensureHandler := admin.EnsureHandler(adminCfg, eng, logger)
		mux.Handle(admin.EnsureRoutePattern(), protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapEnsure, ensureHandler))

		statsHandler := admin.StatsHandler(adminCfg, eng)
		mux.Handle(admin.StatsRoutePattern(), protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapStats, statsHandler))

		queryHandler := query.Handler(queryCfg, eng, logger)
		if ownedTenants != nil {
			queryHandler = cluster.OwnedTenantGuard(ownedTenants, queryHandler)
		}
		mux.Handle(query.QueryRoutePattern(cfg.routePrefix), protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapQuery, queryHandler))

		if cfg.sqlAPIEnabled {
			sqlCfg := &query.SQLConfig{
				DataDir:      cfg.dataDir,
				RoutePrefix:  cfg.routePrefix,
				MaxRows:      cfg.sqlAPIMaxRows,
				Timeout:      cfg.sqlAPITimeout,
				MemoryLimit:  cfg.duckdbMemoryLimit,
				Threads:      cfg.duckdbThreads,
				MaxBodyBytes: cfg.sqlAPIMaxBodyBytes,
				HotOnly:      cfg.queryHotOnly,
				RunJobs:      cfg.runJobs,
			}
			h := query.SQLHandler(sqlCfg, eng, logger)
			h = queue.Middleware(sqlLimiter, h)
			if ownedTenants != nil {
				h = cluster.OwnedTenantGuard(ownedTenants, h)
			}
			mux.Handle(query.SQLRoutePattern(cfg.routePrefix), protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapSQL, h))
		}

		if cfg.promqlAPIEnabled {
			promqlCfg := query.PromQLConfigFromEnv(cfg.dataDir, cfg.routePrefix, cfg.duckdbMemoryLimit, cfg.duckdbThreads)
			promqlCfg.HotOnly = cfg.queryHotOnly
			promqlCfg.RunJobs = cfg.runJobs
			ph := query.PromQLHandler(&promqlCfg, eng, logger)
			// PromQL is a heavy read like /sql, so it shares the same in-flight
			// limiter and the query RBAC action; a cluster client also guards
			// non-owned tenants before touching the engine.
			ph = queue.Middleware(sqlLimiter, ph)
			if ownedTenants != nil {
				ph = cluster.OwnedTenantGuard(ownedTenants, ph)
			}
			wrapped := protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapQuery, ph)
			for _, pattern := range query.PromQLRoutePatterns(cfg.routePrefix) {
				mux.Handle(pattern, wrapped)
			}
		}
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

type backgroundLoopStartFunc func(context.Context, *lifecycle.Runner, *serverConfig, *slog.Logger)

func defaultBackgroundLoopStart(ctx context.Context, runner *lifecycle.Runner, cfg *serverConfig, logger *slog.Logger) {
	go RunBackgroundLoop(ctx, runner, cfg, logger)
}

var startBackgroundLoop backgroundLoopStartFunc = defaultBackgroundLoopStart

func runStore(ctx context.Context, cfg *serverConfig, logger *slog.Logger) error {
	rbac, err := buildRBACStack(ctx, cfg.rbac, logger)
	if err != nil {
		return err
	}
	if rbac != nil {
		defer rbac.close()
	}
	mode, err := cluster.ParseMode(cfg.mode)
	if err != nil {
		return fmt.Errorf("mode: %w", err)
	}
	switch mode {
	case cluster.ModeStandalone:
		return runServe(ctx, cfg, logger, nil, rbac)
	case cluster.ModeClient:
		owned, parseErr := cluster.ParseOwnedTenants(cfg.clientTenants)
		if parseErr != nil {
			return parseErr
		}
		logger.Info("prism-store client mode", "owned_tenant_count", len(owned))
		return runServe(ctx, cfg, logger, owned, rbac)
	case cluster.ModeCluster:
		clients, parseErr := cluster.ParseClients(cfg.clusterClients)
		if parseErr != nil {
			return parseErr
		}
		return runCluster(ctx, cfg, clients, logger, rbac)
	default:
		return fmt.Errorf("mode: %w", cluster.ErrInvalidMode)
	}
}

func runCluster(ctx context.Context, cfg *serverConfig, clients map[string]*url.URL, logger *slog.Logger, rbac *rbacStack) error {
	var wrapQuery func(http.Handler) http.Handler
	if rbac != nil {
		wrapQuery = rbac.wrapQuery
	}
	mux := cluster.NewServeMux(clients, cfg.routePrefix, wrapQuery)
	srv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("prism-store listening",
			"mode", "cluster",
			"addr", cfg.listenAddr,
			"client_count", len(clients),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("listen: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
			defer shutdownCancel()
			_ = srv.Shutdown(shutdownCtx)
			return err
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

func runServe(ctx context.Context, cfg *serverConfig, logger *slog.Logger, ownedTenants map[string]struct{}, rbac *rbacStack) error {
	if err := validateRBACFlight(cfg, rbac); err != nil {
		return err
	}

	flightMode, err := flightIngestAuthMode(cfg)
	if err != nil {
		return fmt.Errorf("auth mode: %w", err)
	}

	now := time.Now
	sqlLimiter := queue.NewLimiter(queue.LimiterConfig{
		Enabled:     cfg.sqlAPIQueueEnabled,
		MaxInFlight: cfg.sqlAPIMaxInFlight,
		MaxQueue:    cfg.sqlAPIMaxQueue,
		Wait:        cfg.sqlAPIQueueTimeout,
	})
	eng := engine.New(engine.Config{
		DataDir:        cfg.dataDir,
		HotWindow:      cfg.hotWindow,
		Threads:        cfg.duckdbThreads,
		MemoryLimit:    cfg.duckdbMemoryLimit,
		MaxOpenTenants: cfg.maxOpenTenants,
	}, now)
	defer func() { _ = eng.Close() }()

	runner := lifecycle.NewRunner(&lifecycle.Config{
		DataDir:         cfg.dataDir,
		SegmentsPerTier: cfg.segmentsPerTier,
		MaxSegmentBytes: cfg.maxSegmentBytes,
		FloorBytes:      lifecycle.FloorBytesFromHotWindow(cfg.hotWindow),
		RetentionDays:   cfg.retentionDays,
		RollupSteps:     cfg.rollupSteps,
		MaxTier:         cfg.maxTier,
		Threads:         cfg.duckdbThreads,
		MemoryLimit:     cfg.duckdbMemoryLimit,
	}, eng, now)

	if cfg.runJobs {
		startBackgroundLoop(ctx, runner, cfg, logger)
	} else {
		logger.Info("prism-store background jobs disabled")
	}

	flightIngestCfg := cfg.ingestConfig(flightMode)

	var flightDone chan error
	if cfg.flightAddr != "" {
		flightSrv, err := storeingest.NewFlightServer(&flightIngestCfg, eng, logger)
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
		Handler:           newServeMux(cfg, eng, logger, publicPlane(cfg), ownedTenants, rbac, sqlLimiter),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	var adminSrv *http.Server
	if cfg.adminListenAddr != "" {
		adminSrv = &http.Server{
			Addr:              cfg.adminListenAddr,
			Handler:           newServeMux(cfg, eng, logger, planeAdmin, ownedTenants, rbac, sqlLimiter),
			ReadHeaderTimeout: readHeaderTimeout,
		}
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("prism-store listening",
			"mode", cfg.mode,
			"addr", cfg.listenAddr,
			"data_dir", cfg.dataDir,
			"auth_mode", cfg.authMode,
			"hot_window", cfg.hotWindow.String(),
			"segments_per_tier", cfg.segmentsPerTier,
			"query_hot_only", cfg.queryHotOnly,
			"run_jobs", cfg.runJobs,
			"sql_api_queue_enabled", cfg.sqlAPIQueueEnabled,
			"sql_api_max_inflight", cfg.sqlAPIMaxInFlight,
			"max_open_tenants", cfg.maxOpenTenants,
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
			shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
			defer shutdownCancel()
			_ = srv.Shutdown(shutdownCtx)
			if adminSrv != nil {
				_ = adminSrv.Shutdown(shutdownCtx)
			}
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
	err := runStore(ctx, &cfg, logger)
	stop()
	if err != nil {
		logger.Error("prism-store failed", "err", err)
		os.Exit(1)
	}
}
