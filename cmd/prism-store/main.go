// Command prism-store is the durable tiered columnar store and query server for
// prism pipeline outputs. This slice exposes health endpoints, HTTP ingest, and
// an optional Arrow Flight DoPut receiver.
//
// Usage:
//
//	prism-store          start the HTTP server (default)
//	prism-store serve    start the HTTP server
//	prism-store print-view-sql --tenant <ns> [--data-dir <dir>]
//	prism-store convert-duckdb-to-parquet [--data-dir <dir>] [--tenant <ns>]
//	prism-store version  print version
//
// Query hot-only mode: set QUERY_HOT_ONLY=true to serve HTTP queries from the
// in-memory hot cache only (no tier or rollup Parquet reads). Default false.
// Arbitrary SQL API: POST /{ns}/sql (RBAC action query, or ADMIN_TOKEN when RBAC
// off). Disable with SQL_API_ENABLED=false. Limits: SQL_API_MAX_ROWS (default
// 100000), SQL_API_TIMEOUT_SECONDS (default 30). Reuses DUCKDB_MEMORY_LIMIT.
// Heavy reads (/sql, PromQL, Loki) share an in-flight queue that is ON by
// default (SQL_API_QUEUE_ENABLED=true, SQL_API_MAX_INFLIGHT=2,
// SQL_API_MAX_QUEUE=128, SQL_API_QUEUE_TIMEOUT_MS=120000); raise the caps only
// with memory headroom for one DUCKDB_MEMORY_LIMIT per in-flight slot. The
// admin plane serves GET /admin/queue with the live snapshot.
// Loki logs API: GET|POST /{ns}/loki/api/v1/{query_range,labels,label/{name}/values}
// (RBAC action query). Serves the file-backed logs relation with a LogQL subset
// (stream selectors + line filters). Disable with LOKI_API_ENABLED=false. Reuses
// SQL_API_MAX_ROWS / SQL_API_TIMEOUT_SECONDS / DUCKDB_* caps.
// Prometheus exporter: GET /metrics is served next to /healthz on every HTTP
// plane, unauthenticated, so a scraper needs no credential and no second port
// (restrict it with a NetworkPolicy). Disable with METRICS_ENABLED=false, move
// it with METRICS_PATH, and drop the tenant dimension from query, error, and
// file series with METRICS_PER_TENANT=false.
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

	"github.com/prism-utils/prism/internal/store/admin"
	"github.com/prism-utils/prism/internal/store/cluster"
	"github.com/prism-utils/prism/internal/store/engine"
	storeingest "github.com/prism-utils/prism/internal/store/ingest"
	"github.com/prism-utils/prism/internal/store/lifecycle"
	"github.com/prism-utils/prism/internal/store/metrics"
	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/queue"
	"github.com/prism-utils/prism/internal/store/segformat"
	"github.com/prism-utils/prism/internal/version"
)

const (
	defaultListenAddr       = ":8080"
	defaultDataDir          = "/data"
	defaultMaxBodyBytes     = 268435456
	defaultArtifacts        = "metrics-raw"
	defaultAuthMode         = "none"
	defaultHotWindowMinutes = 10
	defaultSegmentsPerTier  = 6
	// defaultLogsRefreshSec is the searchable-lag budget for logs: buffered
	// windows open into a tier within a minute even below the count trigger.
	defaultLogsRefreshSec = 60
	// defaultLogsRefreshMaxActions bounds refreshes per artifact per merge tick
	// so a backlog drains without stampeding DuckDB.
	defaultLogsRefreshMaxActions = 8
	// defaultDeleteGraceSec holds a merged-away segment at its original path
	// long enough for a dashboard that resolved the path over a glob to finish
	// opening it, since such a reader cannot skip a file that disappeared.
	defaultDeleteGraceSec    = 120
	defaultMaxSegmentBytes   = 2147483648
	defaultRetentionDays     = 15
	defaultRollupSteps       = "1m,5m,1h"
	defaultMaxTier           = 8
	defaultHotSnapshotSec    = 15
	defaultFlushTickSec      = 30
	defaultMergeTickSec      = 60
	defaultRetentionTickHour = 1
	// Each /sql sandbox honors DUCKDB_MEMORY_LIMIT independently, so peak read
	// memory is MaxInFlight × that limit: shared writers OOM at higher
	// concurrency, and a deep queue with a long wait absorbs dashboard fan-out
	// instead of shedding it. See docs/MEMORY.md.
	defaultSQLAPIQueueEnabled = true
	defaultSQLAPIMaxInFlight  = 2
	defaultSQLAPIMaxQueue     = 128
	defaultSQLAPIQueueTimeout = 120000
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
	listenAddr           string
	adminListenAddr      string
	flightAddr           string
	dataDir              string
	allowedArtifacts     []string
	maxBodyBytes         int64
	ingestToken          string
	adminToken           string
	authMode             string
	routePrefix          string
	hotWindow            time.Duration
	segmentsPerTier      int
	maxSegmentBytes      int64
	retentionDays        int
	maxLogFiles          int
	logsRefreshInterval  time.Duration
	logsRefreshMaxActs   int
	deleteGrace          time.Duration
	logCoalesceMaxAge    time.Duration
	logCoalesceMaxBytes  int64
	logsRecentLookback   time.Duration
	rollupSteps          string
	maxTier              int
	snapshotTick         time.Duration
	flushTick            time.Duration
	mergeTick            time.Duration
	retentionTick        time.Duration
	duckdbThreads        int
	queryDuckdbThreads   int
	duckdbMemoryLimit    string
	queryHotOnly         bool
	sqlAPIEnabled        bool
	sqlAPIMaxRows        int
	sqlAPITimeout        time.Duration
	sqlAPIMaxBodyBytes   int64
	promqlAPIEnabled     bool
	lokiAPIEnabled       bool
	runJobs              bool
	mode                 string
	clientTenants        string
	clusterClients       string
	sqlAPIQueueEnabled   bool
	sqlAPIMaxInFlight    int
	sqlAPIMaxQueue       int
	sqlAPIQueueTimeout   time.Duration
	maxOpenTenants       int
	hotSegmentFormat     string
	mergeSegmentFormat   string
	duckdbStorageVersion string
	rbac                 *rbacConfig
	metrics              metrics.Config
	// metricsReg is the process-wide exporter. Both HTTP planes share one
	// registry so a scrape reports the whole process, not one listener's slice
	// of it.
	metricsReg *metrics.Registry
}

func loadConfig() serverConfig {
	c := serverConfig{
		listenAddr:           envOr("LISTEN_ADDR", defaultListenAddr),
		adminListenAddr:      os.Getenv("ADMIN_LISTEN_ADDR"),
		adminToken:           os.Getenv("ADMIN_TOKEN"),
		flightAddr:           os.Getenv("FLIGHT_ADDR"),
		dataDir:              envOr("DATA_DIR", defaultDataDir),
		maxBodyBytes:         defaultMaxBodyBytes,
		ingestToken:          os.Getenv("INGEST_TOKEN"),
		authMode:             envOr("AUTH_MODE", defaultAuthMode),
		routePrefix:          os.Getenv("ROUTE_PREFIX"),
		hotWindow:            loadHotWindow(),
		segmentsPerTier:      envInt("SEGMENTS_PER_TIER", defaultSegmentsPerTier),
		maxSegmentBytes:      envInt64("MAX_SEGMENT_BYTES", defaultMaxSegmentBytes),
		retentionDays:        envInt("RETENTION_DAYS", defaultRetentionDays),
		maxLogFiles:          envInt("MAX_LOG_FILES", 0),
		logsRefreshInterval:  time.Duration(envIntAllowZero("LOGS_REFRESH_INTERVAL", defaultLogsRefreshSec)) * time.Second,
		logsRefreshMaxActs:   envInt("LOGS_REFRESH_MAX_ACTIONS", defaultLogsRefreshMaxActions),
		deleteGrace:          time.Duration(envIntAllowZero("LOGS_DELETE_GRACE_SECONDS", defaultDeleteGraceSec)) * time.Second,
		logCoalesceMaxAge:    time.Duration(envInt("LOG_COALESCE_MAX_AGE_SECONDS", 0)) * time.Second,
		logCoalesceMaxBytes:  envInt64("LOG_COALESCE_MAX_BYTES", 0),
		logsRecentLookback:   time.Duration(envInt("LOGS_RECENT_LOOKBACK_HOURS", 0)) * time.Hour,
		rollupSteps:          envOr("ROLLUP_STEPS", defaultRollupSteps),
		maxTier:              envInt("MAX_TIER", defaultMaxTier),
		snapshotTick:         time.Duration(envInt("HOT_SNAPSHOT_SECONDS", defaultHotSnapshotSec)) * time.Second,
		flushTick:            time.Duration(envInt("FLUSH_TICK_SECONDS", defaultFlushTickSec)) * time.Second,
		mergeTick:            time.Duration(envInt("MERGE_TICK_SECONDS", defaultMergeTickSec)) * time.Second,
		retentionTick:        loadRetentionTick(),
		duckdbThreads:        envIntZero("DUCKDB_THREADS"),
		queryDuckdbThreads:   envIntZero("QUERY_DUCKDB_THREADS"),
		duckdbMemoryLimit:    os.Getenv("DUCKDB_MEMORY_LIMIT"),
		queryHotOnly:         envBool("QUERY_HOT_ONLY", false),
		sqlAPIEnabled:        envBool("SQL_API_ENABLED", true),
		sqlAPIMaxRows:        envInt("SQL_API_MAX_ROWS", 100000),
		sqlAPITimeout:        time.Duration(envInt("SQL_API_TIMEOUT_SECONDS", 30)) * time.Second,
		sqlAPIMaxBodyBytes:   envInt64("SQL_API_MAX_BODY_BYTES", 1<<20),
		promqlAPIEnabled:     envBool("PROMQL_API_ENABLED", true),
		lokiAPIEnabled:       envBool("LOKI_API_ENABLED", true),
		runJobs:              envBool("RUN_JOBS", true),
		mode:                 envOr("MODE", "standalone"),
		clientTenants:        os.Getenv("CLIENT_TENANTS"),
		clusterClients:       os.Getenv("CLUSTER_CLIENTS"),
		sqlAPIQueueEnabled:   envBool("SQL_API_QUEUE_ENABLED", defaultSQLAPIQueueEnabled),
		sqlAPIMaxInFlight:    envInt("SQL_API_MAX_INFLIGHT", defaultSQLAPIMaxInFlight),
		sqlAPIMaxQueue:       envInt("SQL_API_MAX_QUEUE", defaultSQLAPIMaxQueue),
		sqlAPIQueueTimeout:   time.Duration(envInt("SQL_API_QUEUE_TIMEOUT_MS", defaultSQLAPIQueueTimeout)) * time.Millisecond,
		maxOpenTenants:       envInt("MAX_OPEN_TENANTS", defaultMaxOpenTenants),
		hotSegmentFormat:     strings.ToLower(envOr("HOT_SEGMENT_FORMAT", "parquet")),
		mergeSegmentFormat:   strings.ToLower(envOr("MERGE_SEGMENT_FORMAT", "parquet")),
		duckdbStorageVersion: envOr("DUCKDB_STORAGE_VERSION", segformat.DefaultStorageVersion),
		metrics: metrics.Config{
			Enabled:   envBool("METRICS_ENABLED", true),
			Path:      envOr("METRICS_PATH", metrics.DefaultPath),
			PerTenant: envBool("METRICS_PER_TENANT", true),
		},
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

func (cfg *serverConfig) queryThreads() int {
	if cfg.queryDuckdbThreads > 0 {
		return cfg.queryDuckdbThreads
	}
	return cfg.duckdbThreads
}

func (cfg *serverConfig) validateSegmentFormats() error {
	if _, err := segformat.Parse(cfg.hotSegmentFormat); err != nil {
		return fmt.Errorf("HOT_SEGMENT_FORMAT: %w", err)
	}
	if _, err := segformat.Parse(cfg.mergeSegmentFormat); err != nil {
		return fmt.Errorf("MERGE_SEGMENT_FORMAT: %w", err)
	}
	if strings.TrimSpace(cfg.duckdbStorageVersion) == "" {
		return fmt.Errorf("DUCKDB_STORAGE_VERSION: must be non-empty")
	}
	return nil
}

func (cfg *serverConfig) parsedHotFormat() segformat.Format {
	f, _ := segformat.Parse(cfg.hotSegmentFormat)
	return f
}

func (cfg *serverConfig) parsedMergeFormat() segformat.Format {
	f, _ := segformat.Parse(cfg.mergeSegmentFormat)
	return f
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

// envIntAllowZero reads a non-negative int knob where 0 is a meaningful setting
// (the feature turned off) rather than an unset value.
func envIntAllowZero(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
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
		RunJobs:          c.runJobs,
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

// registerMetricsRoute mounts the scrape endpoint beside the health probes, so
// a scraper needs neither a second port nor a credential. Nothing is mounted
// when the exporter is off, which is what makes the 404 an honest answer.
func registerMetricsRoute(mux *http.ServeMux, reg *metrics.Registry) {
	if !reg.Enabled() {
		return
	}
	mux.Handle("GET "+reg.Path(), reg.Instrument(metrics.RouteMetrics, reg.Handler()))
}

func newServeMux(cfg *serverConfig, eng *engine.Engine, logger *slog.Logger, plane servePlane, ownedTenants map[string]struct{}, rbac *rbacStack, sqlLimiter *queue.Limiter) *http.ServeMux {
	reg := cfg.metricsReg
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", reg.Instrument(metrics.RouteHealthz, http.HandlerFunc(handleHealthz)))
	mux.Handle("GET /readyz", reg.Instrument(metrics.RouteReadyz, handleReadyz(cfg.dataDir)))
	registerMetricsRoute(mux, reg)
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
		HotWindow:   cfg.hotWindow,
	}

	if serveIngest {
		ingestCfg := cfg.ingestConfig(mode)
		ingestHandler := storeingest.Handler(&ingestCfg, eng, logger)
		if rbac != nil {
			ingestHandler = rbac.wrapIngest(ingestHandler)
		}
		mux.Handle(storeingest.IngestRoutePattern(cfg.routePrefix), reg.Instrument(metrics.RouteIngest, ingestHandler))
	}
	if serveAdmin {
		ensureHandler := admin.EnsureHandler(adminCfg, eng, logger)
		mux.Handle(admin.EnsureRoutePattern(), reg.Instrument(metrics.RouteAdminEnsure,
			protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapEnsure, ensureHandler)))

		statsHandler := admin.StatsHandler(adminCfg, eng)
		mux.Handle(admin.StatsRoutePattern(), reg.Instrument(metrics.RouteStats,
			protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapStats, statsHandler)))

		queueHandler := admin.QueueHandler(sqlLimiter)
		mux.Handle(admin.QueueRoutePattern(), reg.Instrument(metrics.RouteAdminQueue,
			protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapStats, queueHandler)))

		queryHandler := query.Handler(queryCfg, eng, logger)
		if ownedTenants != nil {
			queryHandler = cluster.OwnedTenantGuard(ownedTenants, queryHandler)
		}
		mux.Handle(query.QueryRoutePattern(cfg.routePrefix), reg.Instrument(metrics.RouteQuery,
			protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapQuery, queryHandler)))

		if cfg.sqlAPIEnabled {
			sqlCfg := &query.SQLConfig{
				DataDir:      cfg.dataDir,
				RoutePrefix:  cfg.routePrefix,
				MaxRows:      cfg.sqlAPIMaxRows,
				Timeout:      cfg.sqlAPITimeout,
				MemoryLimit:  cfg.duckdbMemoryLimit,
				Threads:      cfg.queryThreads(),
				MaxBodyBytes: cfg.sqlAPIMaxBodyBytes,
				HotOnly:      cfg.queryHotOnly,
				RunJobs:      cfg.runJobs,
				HotWindow:    cfg.hotWindow,
			}
			h := query.SQLHandler(sqlCfg, eng, logger)
			h = queue.Middleware(sqlLimiter, h)
			if ownedTenants != nil {
				h = cluster.OwnedTenantGuard(ownedTenants, h)
			}
			mux.Handle(query.SQLRoutePattern(cfg.routePrefix), reg.InstrumentQuery(metrics.APISQL,
				reg.Instrument(metrics.RouteSQL,
					protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapSQL, h))))
		}

		if cfg.promqlAPIEnabled {
			promqlCfg := query.PromQLConfigFromEnv(cfg.dataDir, cfg.routePrefix, cfg.duckdbMemoryLimit, cfg.queryThreads())
			promqlCfg.HotOnly = cfg.queryHotOnly
			promqlCfg.RunJobs = cfg.runJobs
			promqlCfg.HotWindow = cfg.hotWindow
			ph := query.PromQLHandler(&promqlCfg, eng, logger)
			// PromQL is a heavy read like /sql, so it shares the same in-flight
			// limiter and the query RBAC action; a cluster client also guards
			// non-owned tenants before touching the engine.
			ph = queue.Middleware(sqlLimiter, ph)
			if ownedTenants != nil {
				ph = cluster.OwnedTenantGuard(ownedTenants, ph)
			}
			wrapped := reg.InstrumentQuery(metrics.APIPromQL,
				reg.Instrument(metrics.RoutePromQL, protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapQuery, ph)))
			for _, pattern := range query.PromQLRoutePatterns(cfg.routePrefix) {
				mux.Handle(pattern, wrapped)
			}
		}

		if cfg.lokiAPIEnabled {
			lokiCfg := &query.LokiConfig{
				DataDir:        cfg.dataDir,
				RoutePrefix:    cfg.routePrefix,
				MaxEntries:     cfg.sqlAPIMaxRows,
				Timeout:        cfg.sqlAPITimeout,
				MemoryLimit:    cfg.duckdbMemoryLimit,
				Threads:        cfg.queryThreads(),
				RecentLookback: cfg.logsRecentLookback,
			}
			lh := query.LokiHandler(lokiCfg, logger)
			// Logs reads scan Parquet like /sql, so they share the same in-flight
			// limiter and the query RBAC action; a cluster client also guards
			// non-owned tenants before touching the data dir.
			lh = queue.Middleware(sqlLimiter, lh)
			if ownedTenants != nil {
				lh = cluster.OwnedTenantGuard(ownedTenants, lh)
			}
			wrapped := reg.InstrumentQuery(metrics.APILoki,
				reg.Instrument(metrics.RouteLoki, protectAdminRoute(rbac, adminCfg.AdminToken, rbac.wrapQuery, lh)))
			for _, pattern := range query.LokiRoutePatterns(cfg.routePrefix) {
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
	cfg.metricsReg = metrics.New(cfg.metrics)
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
	// A coordinator holds no engine, queue, or lifecycle, so its exporter is the
	// runtime and process view only — still the RSS/CPU/FD signal an operator
	// needs for a pod that fans queries out.
	registerMetricsRoute(mux, cfg.metricsReg)
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

// attachMetricSources points the scrape-time gauges at the live limiter and
// engine, and publishes the static caps those gauges are read against.
func attachMetricSources(cfg *serverConfig, sqlLimiter *queue.Limiter, eng *engine.Engine) error {
	if err := cfg.metricsReg.SetQueueSource(sqlLimiter); err != nil {
		return fmt.Errorf("metrics queue source: %w", err)
	}
	if err := cfg.metricsReg.SetEngineSource(eng); err != nil {
		return fmt.Errorf("metrics engine source: %w", err)
	}
	cfg.metricsReg.SetLogLandingLimit(cfg.maxLogFiles)
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
		Observer:    cfg.metricsReg,
	})
	eng := engine.New(engine.Config{
		DataDir:              cfg.dataDir,
		HotWindow:            cfg.hotWindow,
		Threads:              cfg.duckdbThreads,
		MemoryLimit:          cfg.duckdbMemoryLimit,
		MaxOpenTenants:       cfg.maxOpenTenants,
		LogCoalesceMaxAge:    cfg.logCoalesceMaxAge,
		LogCoalesceMaxBytes:  cfg.logCoalesceMaxBytes,
		HotSegmentFormat:     cfg.parsedHotFormat(),
		MergeSegmentFormat:   cfg.parsedMergeFormat(),
		DuckDBStorageVersion: cfg.duckdbStorageVersion,
	}, now)
	eng.SetLogger(logger)
	defer func() { _ = eng.Close() }()

	if err := attachMetricSources(cfg, sqlLimiter, eng); err != nil {
		return err
	}

	runner := lifecycle.NewRunner(&lifecycle.Config{
		DataDir:               cfg.dataDir,
		SegmentsPerTier:       cfg.segmentsPerTier,
		MaxSegmentBytes:       cfg.maxSegmentBytes,
		FloorBytes:            lifecycle.FloorBytesFromHotWindow(cfg.hotWindow),
		RetentionDays:         cfg.retentionDays,
		MaxLogFiles:           cfg.maxLogFiles,
		LogsRefreshInterval:   cfg.logsRefreshInterval,
		LogsRefreshMaxActions: cfg.logsRefreshMaxActs,
		DeleteGrace:           cfg.deleteGrace,
		RollupSteps:           cfg.rollupSteps,
		MaxTier:               cfg.maxTier,
		Threads:               cfg.duckdbThreads,
		MemoryLimit:           cfg.duckdbMemoryLimit,
		MergeSegmentFormat:    cfg.parsedMergeFormat(),
		DuckDBStorageVersion:  cfg.duckdbStorageVersion,
		Logger:                logger,
		Recorder:              cfg.metricsReg,
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
			"logs_refresh_interval", cfg.logsRefreshInterval.String(),
			"delete_grace", cfg.deleteGrace.String(),
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

func runConvertDuckDBToParquet(args []string) error {
	fs := flag.NewFlagSet("convert-duckdb-to-parquet", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "optional tenant namespace (all tenants when empty)")
	dataDir := fs.String("data-dir", envOr("DATA_DIR", defaultDataDir), "shared data root")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant != "" {
		n, err := segformat.ConvertTenantDuckDBToParquet(*dataDir, *tenant)
		if err != nil {
			return err
		}
		fmt.Printf("converted %d duckdb segment(s) under %s/%s\n", n, *dataDir, *tenant)
		return nil
	}
	entries, err := os.ReadDir(*dataDir)
	if err != nil {
		return err
	}
	total := 0
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		n, err := segformat.ConvertTenantDuckDBToParquet(*dataDir, e.Name())
		if err != nil {
			return fmt.Errorf("tenant %s: %w", e.Name(), err)
		}
		total += n
	}
	fmt.Printf("converted %d duckdb segment(s) under %s\n", total, *dataDir)
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
		case "convert-duckdb-to-parquet":
			if err := runConvertDuckDBToParquet(os.Args[2:]); err != nil {
				fmt.Fprintf(os.Stderr, "convert-duckdb-to-parquet: %v\n", err)
				os.Exit(1)
			}
			return
		case "serve":
			// default server path below
		}
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()
	if err := cfg.validateSegmentFormats(); err != nil {
		logger.Error("prism-store config", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := runStore(ctx, &cfg, logger)
	stop()
	if err != nil {
		logger.Error("prism-store failed", "err", err)
		os.Exit(1)
	}
}
