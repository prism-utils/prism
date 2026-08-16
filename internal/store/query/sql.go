package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/httperr"
	storeingest "github.com/prism-utils/prism/internal/store/ingest"
	"github.com/prism-utils/prism/internal/store/materialize"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

const (
	defaultSQLMaxRows      = 100_000
	defaultSQLTimeout      = 30 * time.Second
	defaultSQLMaxBodyBytes = 1 << 20 // 1 MiB
	sandboxMetricsView     = "metrics"
	sandboxLogsView        = "logs"
	arrowStreamMediaType   = "application/vnd.apache.arrow.stream"
	truncatedTrailer       = "X-Prism-Truncated"
)

// DefaultSQLMaxBodyBytes is the default POST /sql JSON body cap.
const DefaultSQLMaxBodyBytes = defaultSQLMaxBodyBytes

var (
	errEmptySQL         = errors.New("query: empty sql")
	errNonSelect        = errors.New("query: non-select sql")
	errMultiStatement   = errors.New("query: multi-statement sql")
	errSandboxExec      = errors.New("query: sandbox exec")
	errNoParquetSources = errors.New("query: no segment sources")
	errUnknownTenant    = errors.New("query: unknown tenant")
)

// SQLConfig holds arbitrary SQL API settings.
type SQLConfig struct {
	DataDir      string
	RoutePrefix  string
	MaxRows      int
	Timeout      time.Duration
	MemoryLimit  string
	Threads      int
	MaxBodyBytes int64
	HotOnly      bool
	// RunJobs mirrors the process-wide RUN_JOBS flag. When false this store is a
	// read-only replica (it owns no writes to the tenant data dir), so /sql must
	// serve purely from immutable parquet|duckdb segments and must NOT flush a
	// fresh hot snapshot (that is the writer's job, and the replica's data mount
	// is read-only).
	RunJobs bool
	// HotWindow is the process hot-window duration, used only when snapshot
	// min/max stats are missing so auto-hot still has a coverage estimate.
	HotWindow time.Duration
	// MaterializationNames are sandbox views mat_<name> bound from
	// materializations/<name>/ (never raw tiers or hot).
	MaterializationNames []string
}

// SQLRoutePattern returns the ServeMux pattern for POST /{ns}/sql.
func SQLRoutePattern(prefix string) string {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return "POST /{ns}/sql"
	}
	return "POST " + prefix + "/{ns}/sql"
}

// SQLRequest is the JSON body for POST /{ns}/sql.
type SQLRequest struct {
	SQL     string `json:"sql"`
	MaxRows *int   `json:"max_rows,omitempty"`
	Start   string `json:"start,omitempty"`
	End     string `json:"end,omitempty"`
}

// SQLResponse is the JSON success body for POST /{ns}/sql.
type SQLResponse struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
}

func sqlWindowStart(r *http.Request, req SQLRequest) time.Time {
	return parseOptionalQueryTime(firstNonEmpty(r.URL.Query().Get("start"), req.Start))
}

func sqlWindowEnd(r *http.Request, req SQLRequest) time.Time {
	return parseOptionalQueryTime(firstNonEmpty(r.URL.Query().Get("end"), req.End))
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

func parseOptionalQueryTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	if ts, err := time.Parse(time.RFC3339, s); err == nil {
		return ts.UTC()
	}
	if ts, err := parseTimeParam(s, time.Time{}); err == nil {
		return ts.UTC()
	}
	return time.Time{}
}

// SQLHandler serves POST /{ns}/sql in a per-request tenant sandbox.
func SQLHandler(cfg *SQLConfig, eng *engine.Engine, logger *slog.Logger) http.Handler {
	if cfg == nil {
		cfg = &SQLConfig{}
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = defaultSQLMaxRows
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultSQLTimeout
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultSQLMaxBodyBytes
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		if !storeingest.ValidateTenant(ns) {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}

		var req SQLRequest
		body := http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes)
		dec := json.NewDecoder(body)
		if err := dec.Decode(&req); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				http.Error(w, "bad query", http.StatusBadRequest)
				return
			}
			http.Error(w, "bad query", http.StatusBadRequest)
			return
		}
		defer func() { _ = r.Body.Close() }()
		if strings.TrimSpace(req.SQL) == "" {
			http.Error(w, "bad query", http.StatusBadRequest)
			return
		}
		if err := validateReadOnlySQL(req.SQL); err != nil {
			http.Error(w, "bad query", http.StatusBadRequest)
			return
		}

		tenantRoot := filepath.Join(cfg.DataDir, ns)
		absRoot, err := resolveSandboxTenantRoot(cfg.DataDir, tenantRoot)
		if err != nil {
			if errors.Is(err, errUnknownTenant) {
				http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
				return
			}
			logger.Error("sql tenant root", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), cfg.Timeout)
		defer cancel()

		if httperr.IsCanceled(ctx.Err()) {
			httperr.Write(w)
			return
		}

		// SQL reads serve from immutable hot/tier segments only: the :memory:
		// sandbox opens a pinned hot snapshot inode and tiers/*.{parquet|duckdb}
		// (read_parquet or read-only ATTACH) and never opens the live tenant
		// engine.duckdb. This lets a read-only replica (RUN_JOBS=false, with a
		// read-only data mount owned by the writer) serve /sql without hitting
		// `engine.duckdb: Read-only file system` or a DuckDB write-lock conflict
		// with the writer that holds the same file.
		//
		// A store that runs jobs (the writer / all-in-one) first flushes live hot
		// rows to the configured hot snapshot so its own reads are fresh; a
		// replica serves the writer-produced snapshot as-is (bounded by the
		// snapshot interval). A tenant with no segment files still answers via
		// an empty, correctly-typed `metrics` view, so freshly-provisioned /
		// hot-only-empty tenants return zero rows rather than a misleading 400.
		if cfg.RunJobs {
			//nolint:contextcheck // snapshot export uses engine-internal context; request ctx applies to sandbox query below.
			if err := eng.ExportHotSnapshot(ns); err != nil {
				logger.Error("sql hot snapshot", "ns", ns, "err", err)
				http.Error(w, "query failed", http.StatusInternalServerError)
				return
			}
			if httperr.IsCanceled(ctx.Err()) {
				httperr.Write(w)
				return
			}
		}

		rowCap := cfg.MaxRows
		if req.MaxRows != nil && *req.MaxRows > 0 && *req.MaxRows < rowCap {
			rowCap = *req.MaxRows
		}

		if wantsArrowStream(r) && !arrowTransportSupported() {
			http.Error(w, "arrow transport unavailable", http.StatusNotAcceptable)
			return
		}

		conn, cleanup, err := prepareSandboxConn(ctx, absRoot, &metricsOpenOpts{
			HotOnly:   cfg.HotOnly,
			Start:     sqlWindowStart(r, req),
			End:       sqlWindowEnd(r, req),
			HotWindow: cfg.HotWindow,
		}, sandboxLimits{
			MemoryLimit: cfg.MemoryLimit,
			Threads:     cfg.Threads,
			MatNames:    cfg.MaterializationNames,
		})
		if err != nil {
			writeSQLErr(w, ctx, err, logger, ns, "sql sandbox")
			return
		}
		defer cleanup()

		if wantsArrowStream(r) {
			if err := writeArrowResponse(ctx, w, conn, req.SQL, rowCap, logger); err != nil {
				writeSQLErr(w, ctx, err, logger, ns, "sql arrow failed")
				return
			}
			return
		}

		result, err := queryJSON(ctx, conn, req.SQL, rowCap)
		if err != nil {
			writeSQLErr(w, ctx, err, logger, ns, "sql failed")
			return
		}

		payload, err := json.Marshal(result)
		if err != nil {
			logger.Error("sql encode", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(payload); err != nil {
			logger.Error("sql write", "ns", ns, "err", err)
		}
	})
}

func wantsArrowStream(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), arrowStreamMediaType)
}

// prepareMetricsSandboxConn opens a locked :memory: DuckDB with only the tenant
// metrics view. PromQL must use this path: attaching the logs relation would
// fail the whole query when a stale logs parquet path disappears mid-flight
// (retention/compaction), and metrics never need those files.
func prepareMetricsSandboxConn(ctx context.Context, tenantRoot string, hotOnly bool, limits sandboxLimits) (*sql.Conn, func(), error) {
	return prepareMetricsSandbox(ctx, tenantRoot, &metricsOpenOpts{HotOnly: hotOnly}, limits)
}

func prepareMetricsSandbox(ctx context.Context, tenantRoot string, opts *metricsOpenOpts, limits sandboxLimits) (*sql.Conn, func(), error) {
	conn, cleanup, err := openSandboxConn(ctx, tenantRoot, limits)
	if err != nil {
		return nil, nil, err
	}
	pins, err := bindPinnedMetricsView(ctx, conn, tenantRoot, opts)
	if err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	cleanup = withPinnedCleanup(cleanup, pins)
	if err := lockSandbox(ctx, conn); err != nil {
		cleanup()
		return nil, nil, err
	}
	return conn, cleanup, nil
}

// prepareSandboxConn opens the /sql sandbox: metrics + logs views.
func prepareSandboxConn(ctx context.Context, tenantRoot string, opts *metricsOpenOpts, limits sandboxLimits) (*sql.Conn, func(), error) {
	conn, cleanup, err := openSandboxConn(ctx, tenantRoot, limits)
	if err != nil {
		return nil, nil, err
	}
	pins, err := bindPinnedMetricsView(ctx, conn, tenantRoot, opts)
	if err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	cleanup = withPinnedCleanup(cleanup, pins)
	logFiles, err := listLogSegmentFiles(tenantRoot)
	if err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	logFiles = filterExistingLogFiles(logFiles)
	if err := attachLogsDuckDB(ctx, conn, logFiles); err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	// WithIngestTS matches the Loki path: expose __prism_ts_ns (ingest/landing
	// time, nanoseconds) so /sql callers can time-bound counts and scans.
	logsSQL, err := buildLogsRelationSQLMixed(logFiles, logsCatalogOpts{WithIngestTS: true})
	if err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE VIEW "+sandboxLogsView+" AS "+logsSQL); err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(fmt.Errorf("create logs view: %w", err))
	}
	if err := bindMaterializationViews(ctx, conn, tenantRoot, limits.MatNames); err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	if err := lockSandbox(ctx, conn); err != nil {
		cleanup()
		return nil, nil, err
	}
	return conn, cleanup, nil
}

func bindMaterializationViews(ctx context.Context, conn *sql.Conn, tenantRoot string, names []string) error {
	for _, name := range names {
		dir := filepath.Join(tenantRoot, "materializations", name)
		files, err := materialize.LiveFiles(dir)
		if err != nil {
			return err
		}
		viewSQL := materialize.ViewSQL(files)
		//nolint:gosec // G202: name is Validate()'d [a-z][a-z0-9_]*; viewSQL is listing-built.
		q := "CREATE VIEW mat_" + name + " AS " + viewSQL
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("create mat_%s: %w", name, err)
		}
	}
	return nil
}

func bindPinnedMetricsView(ctx context.Context, conn *sql.Conn, tenantRoot string, opts *metricsOpenOpts) (hotPins, error) {
	sources, err := collectMetricsSources(ctx, tenantRoot, opts)
	if err != nil {
		return hotPins{}, err
	}
	pins, err := pinHotSnapshotSources(sources)
	if err != nil {
		return hotPins{}, err
	}
	if pins.extraDir != "" {
		q := fmt.Sprintf("SET allowed_directories=[%s, %s]",
			quoteSQLPath(tenantRoot), quoteSQLPath(pins.extraDir))
		if _, err := conn.ExecContext(ctx, q); err != nil {
			pins.cleanup()
			return hotPins{}, fmt.Errorf("allow pin dir: %w", err)
		}
	}
	if err := attachMetricsDuckDB(ctx, conn, sources); err != nil {
		pins.cleanup()
		return hotPins{}, err
	}
	viewSQL, err := sandboxMetricsUnionSQLFromSources(sources)
	if err != nil {
		pins.cleanup()
		return hotPins{}, err
	}
	if _, err := conn.ExecContext(ctx, "CREATE VIEW "+sandboxMetricsView+" AS "+viewSQL); err != nil {
		pins.cleanup()
		return hotPins{}, fmt.Errorf("create metrics view: %w", err)
	}
	return pins, nil
}

func queryJSON(ctx context.Context, conn *sql.Conn, userSQL string, rowCap int) (*SQLResponse, error) {
	rows, err := conn.QueryContext(ctx, userSQL)
	if err != nil {
		return nil, wrapSandboxErr(err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, wrapSandboxErr(err)
	}

	out := &SQLResponse{Columns: cols, Rows: [][]any{}}
	limit := rowCap + 1
	for rows.Next() {
		if len(out.Rows) >= limit {
			out.Truncated = true
			break
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, wrapSandboxErr(err)
		}
		row := make([]any, len(cols))
		for i, v := range vals {
			row[i] = jsonCell(v)
		}
		out.Rows = append(out.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapSandboxErr(err)
	}
	if out.Truncated && len(out.Rows) > rowCap {
		out.Rows = out.Rows[:rowCap]
	}
	out.RowCount = len(out.Rows)
	return out, nil
}

func openSandboxConn(ctx context.Context, tenantRoot string, limits sandboxLimits) (*sql.Conn, func(), error) {
	connector, err := duckdb.NewConnector(":memory:", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("query: duckdb connector: %w", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		_ = connector.Close()
		return nil, nil, fmt.Errorf("query: sandbox conn: %w", err)
	}
	if err := applySandboxBootstrap(ctx, conn, tenantRoot, limits); err != nil {
		_ = conn.Close()
		_ = db.Close()
		_ = connector.Close()
		return nil, nil, err
	}
	cleanup := func() {
		_ = conn.Close()
		_ = db.Close()
		_ = connector.Close()
	}
	return conn, cleanup, nil
}

type sandboxLimits struct {
	MemoryLimit string
	Threads     int
	MatNames    []string
}

func applySandboxBootstrap(ctx context.Context, conn *sql.Conn, tenantRoot string, limits sandboxLimits) error {
	var steps []string
	if limits.Threads > 0 {
		steps = append(steps, fmt.Sprintf("SET threads=%d", limits.Threads))
	}
	if limits.MemoryLimit != "" {
		steps = append(steps, fmt.Sprintf("SET memory_limit='%s'", escapeSQLLiteral(limits.MemoryLimit)))
	}
	steps = append(steps,
		"SET max_temp_directory_size='0B'",
		"LOAD parquet",
	)
	for _, q := range steps {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("query: sandbox bootstrap: %w", err)
		}
	}
	dirSet := fmt.Sprintf("SET allowed_directories=[%s]", quoteSQLPath(tenantRoot))
	if _, err := conn.ExecContext(ctx, dirSet); err != nil {
		return fmt.Errorf("query: sandbox allowed_directories: %w", err)
	}
	return nil
}

func lockSandbox(ctx context.Context, conn *sql.Conn) error {
	steps := []string{
		"SET enable_external_access=false",
		"SET allow_community_extensions=false",
		"SET autoinstall_known_extensions=false",
		"SET autoload_known_extensions=false",
		"SET allow_unsigned_extensions=false",
		"SET lock_configuration=true",
	}
	for _, q := range steps {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("query: sandbox lock: %w", err)
		}
	}
	return nil
}

// emptyMetricsViewSQL is the body of the sandbox `metrics` view when a tenant
// has no hot/tier segment sources. It yields zero rows with the metrics column
// names and types, so queries against an empty tenant behave like queries
// against an empty result set.
const emptyMetricsViewSQL = `SELECT ` +
	`CAST(NULL AS VARCHAR) AS "__name__", ` +
	`CAST(NULL AS VARCHAR) AS labels, ` +
	`CAST(NULL AS DOUBLE) AS value, ` +
	`CAST(NULL AS BIGINT) AS timestamp_ms, ` +
	`CAST(NULL AS TIMESTAMP) AS ts ` +
	`WHERE 1=0`

// emptyLogsViewSQL is the body of the sandbox `logs` view when a tenant has no
// landed log segments: zero rows with the guaranteed logs columns.
const emptyLogsViewSQL = `SELECT ` +
	`CAST(NULL AS VARCHAR) AS message, ` +
	`CAST(NULL AS VARCHAR) AS format, ` +
	`CAST(NULL AS VARCHAR) AS template, ` +
	`CAST(NULL AS BIGINT) AS count ` +
	`WHERE 1=0`

const hotSnapshotRel = "hot/current.parquet"

func validateReadOnlySQL(raw string) error {
	s := stripSQLComments(strings.TrimSpace(raw))
	if s == "" {
		return errEmptySQL
	}
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), ";"))
	if strings.Contains(s, ";") {
		return errMultiStatement
	}
	s = stripStringLiterals(s)
	s = strings.TrimSpace(s)
	if s == "" {
		return errEmptySQL
	}
	if containsForbiddenKeyword(s) {
		return errNonSelect
	}
	upper := strings.ToUpper(s)
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return nil
	case strings.HasPrefix(upper, "WITH"):
		main, err := mainQueryAfterWith(s)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(main)), "SELECT") {
			return errNonSelect
		}
		return nil
	default:
		return errNonSelect
	}
}

var forbiddenSQLKeywords = []string{
	"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "ATTACH",
	"COPY", "INSTALL", "LOAD", "PRAGMA", "EXPORT", "CALL", "SET", "RESET",
}

func resolveSandboxTenantRoot(dataDir, tenantRoot string) (string, error) {
	fi, err := os.Lstat(tenantRoot) //nolint:gosec // G703: tenantRoot joins validated ns under dataDir
	if err != nil {
		if os.IsNotExist(err) {
			return "", errUnknownTenant
		}
		return "", fmt.Errorf("query: lstat tenant root: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return "", errUnknownTenant
	}
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return "", fmt.Errorf("query: abs data dir: %w", err)
	}
	absDataDir, err = evalCleanSymlinks(absDataDir)
	if err != nil {
		return "", fmt.Errorf("query: resolve data dir: %w", err)
	}
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		return "", fmt.Errorf("query: abs tenant root: %w", err)
	}
	absRoot, err = evalCleanSymlinks(absRoot)
	if err != nil {
		return "", errUnknownTenant
	}
	if !pathUnderTenantRoot(absDataDir, absRoot) {
		return "", errUnknownTenant
	}
	return absRoot, nil
}

func evalCleanSymlinks(p string) (string, error) {
	resolved, err := filepath.EvalSymlinks(p)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func containsForbiddenKeyword(s string) bool {
	upper := strings.ToUpper(s)
	for _, kw := range forbiddenSQLKeywords {
		if sqlKeywordPresent(upper, kw) {
			return true
		}
	}
	return false
}

func sqlKeywordPresent(s, kw string) bool {
	idx := 0
	for {
		i := strings.Index(s[idx:], kw)
		if i < 0 {
			return false
		}
		i += idx
		before := i == 0 || !isSQLIdentChar(s[i-1])
		after := i+len(kw) >= len(s) || !isSQLIdentChar(s[i+len(kw)])
		if before && after {
			return true
		}
		idx = i + len(kw)
	}
}

func isSQLIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func mainQueryAfterWith(s string) (string, error) {
	s = strings.TrimSpace(s)
	if len(s) < 4 || !strings.EqualFold(s[:4], "with") {
		return "", errNonSelect
	}
	i := 4
	i = skipSQLSpace(s, i)
	for i < len(s) {
		i = skipSQLIdent(s, i)
		i = skipSQLSpace(s, i)
		if i+2 > len(s) || !strings.EqualFold(s[i:i+2], "as") {
			return "", errNonSelect
		}
		i += 2
		i = skipSQLSpace(s, i)
		if i >= len(s) || s[i] != '(' {
			return "", errNonSelect
		}
		var err error
		i, err = skipBalancedParens(s, i)
		if err != nil {
			return "", err
		}
		i = skipSQLSpace(s, i)
		if i < len(s) && s[i] == ',' {
			i++
			i = skipSQLSpace(s, i)
			continue
		}
		break
	}
	return strings.TrimSpace(s[i:]), nil
}

func skipSQLSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

func skipSQLIdent(s string, i int) int {
	if i < len(s) && (s[i] == '"' || s[i] == '`') {
		quote := s[i]
		i++
		for i < len(s) && s[i] != quote {
			i++
		}
		if i < len(s) {
			i++
		}
		return i
	}
	for i < len(s) && isSQLIdentChar(s[i]) {
		i++
	}
	return i
}

func skipBalancedParens(s string, start int) (int, error) {
	if start >= len(s) || s[start] != '(' {
		return start, errNonSelect
	}
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1, nil
			}
		}
	}
	return len(s), errNonSelect
}

func stripStringLiterals(s string) string {
	var b strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '\'' {
			i++
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			b.WriteByte(' ')
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func collectSafeParquetPaths(absTenantRoot, tenantRoot string, hotOnly bool) ([]string, error) {
	sources, err := collectMetricsSources(context.Background(), tenantRoot, &metricsOpenOpts{HotOnly: hotOnly})
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, s := range sources {
		if filepath.Ext(s.Path) != ".parquet" {
			continue
		}
		ok, err := safeTenantParquetFile(absTenantRoot, s.Path)
		if err != nil {
			return nil, err
		}
		if ok {
			paths = append(paths, s.Path)
		}
	}
	return paths, nil
}

func safeTenantParquetFile(absTenantRoot, path string) (bool, error) {
	fi, err := os.Lstat(path) //nolint:gosec // G703: path is collected from tenant-scoped globs under absTenantRoot
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return false, nil
	}
	if !fi.Mode().IsRegular() {
		return false, nil
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false, err
	}
	abs, err := filepath.Abs(real)
	if err != nil {
		return false, err
	}
	if !pathUnderTenantRoot(absTenantRoot, abs) {
		return false, nil
	}
	return true, nil
}

func pathUnderTenantRoot(absTenantRoot, absPath string) bool {
	root := filepath.Clean(absTenantRoot)
	path := filepath.Clean(absPath)
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func stripSQLComments(s string) string {
	var b strings.Builder
	i := 0
	for i < len(s) {
		if i+1 < len(s) && s[i] == '-' && s[i+1] == '-' {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(s) && s[i] == '/' && s[i+1] == '*' {
			i += 2
			for i < len(s)-1 && (s[i] != '*' || s[i+1] != '/') {
				i++
			}
			i += 2
			continue
		}
		if s[i] == '\'' {
			b.WriteByte(s[i])
			i++
			for i < len(s) {
				b.WriteByte(s[i])
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						b.WriteByte(s[i+1])
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return strings.TrimSpace(b.String())
}

func quoteSQLPath(p string) string {
	return "'" + escapeSQLLiteral(p) + "'"
}

func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func jsonCell(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case string:
		return x
	case int64:
		return x
	case float64:
		return x
	case bool:
		return x
	default:
		if b, ok := x.([]uint8); ok {
			return string(b)
		}
		if s, ok := x.(fmt.Stringer); ok {
			return s.String()
		}
		return fmt.Sprint(x)
	}
}

// SQLConfigFromEnv reads SQL API limits from the process environment.
func SQLConfigFromEnv(dataDir, routePrefix, memoryLimit string, threads int) SQLConfig {
	maxRows := defaultSQLMaxRows
	if v := os.Getenv("SQL_API_MAX_ROWS"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			maxRows = n
		}
	}
	timeout := defaultSQLTimeout
	if v := os.Getenv("SQL_API_TIMEOUT_SECONDS"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			timeout = time.Duration(n) * time.Second
		}
	}
	maxBody := int64(defaultSQLMaxBodyBytes)
	if v := os.Getenv("SQL_API_MAX_BODY_BYTES"); v != "" {
		if n, err := parsePositiveInt64(v); err == nil {
			maxBody = n
		}
	}
	return SQLConfig{
		DataDir:      dataDir,
		RoutePrefix:  routePrefix,
		MaxRows:      maxRows,
		Timeout:      timeout,
		MemoryLimit:  memoryLimit,
		Threads:      threads,
		MaxBodyBytes: maxBody,
	}
}

// SQLAPIEnabledFromEnv reports whether the SQL API route should be registered.
func SQLAPIEnabledFromEnv() bool {
	v := strings.TrimSpace(os.Getenv("SQL_API_ENABLED"))
	if v == "" {
		return true
	}
	b, err := parseBool(v)
	if err != nil {
		return true
	}
	return b
}

func wrapSandboxErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", errSandboxExec, err)
}

// writeSQLErr maps a sandbox or user-SQL failure onto the HTTP status the
// caller should see. A gone client is not a bad query.
func writeSQLErr(w http.ResponseWriter, ctx context.Context, err error, logger *slog.Logger, ns, op string) {
	if httperr.IsCanceled(err) || httperr.IsCanceled(ctx.Err()) {
		httperr.Write(w)
		return
	}
	if errors.Is(err, errSandboxExec) || errors.Is(err, errEmptySQL) ||
		errors.Is(err, errNonSelect) || errors.Is(err, errMultiStatement) ||
		errors.Is(err, errNoParquetSources) {
		http.Error(w, "bad query", http.StatusBadRequest)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		http.Error(w, "bad query", http.StatusBadRequest)
		return
	}
	logger.Error(op, "ns", ns, "err", err)
	http.Error(w, "query failed", http.StatusInternalServerError)
}

func parsePositiveInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("not positive int64")
	}
	return n, nil
}

func parsePositiveInt(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, io.EOF
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("not an int")
		}
		n = n*10 + int(r-'0')
	}
	if n <= 0 {
		return 0, fmt.Errorf("not positive")
	}
	return n, nil
}

func parseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "t", "true", "yes", "y", "on":
		return true, nil
	case "0", "f", "false", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool")
	}
}
