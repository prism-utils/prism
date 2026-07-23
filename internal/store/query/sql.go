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

	"github.com/elk-utilities/prism/internal/store/engine"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/layout"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

const (
	defaultSQLMaxRows      = 100_000
	defaultSQLTimeout      = 30 * time.Second
	defaultSQLMaxBodyBytes = 1 << 20 // 1 MiB
	sandboxMetricsView     = "metrics"
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
	errNoParquetSources = errors.New("query: no parquet sources")
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
}

// SQLResponse is the JSON success body for POST /{ns}/sql.
type SQLResponse struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
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

		hasParquet, err := tenantHasParquetSources(ctx, eng, cfg.DataDir, ns)
		if err != nil {
			logger.Error("sql parquet probe", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		if !hasParquet {
			http.Error(w, "bad query", http.StatusBadRequest)
			return
		}

		//nolint:contextcheck // snapshot export uses engine-internal context; request ctx applies to sandbox query below.
		if err := eng.ExportHotSnapshot(ns); err != nil {
			logger.Error("sql hot snapshot", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}

		rowCap := cfg.MaxRows
		if req.MaxRows != nil && *req.MaxRows > 0 && *req.MaxRows < rowCap {
			rowCap = *req.MaxRows
		}

		conn, cleanup, err := prepareSandboxConn(ctx, absRoot, sandboxLimits{
			MemoryLimit: cfg.MemoryLimit,
			Threads:     cfg.Threads,
		})
		if err != nil {
			if errors.Is(err, errSandboxExec) || errors.Is(err, errEmptySQL) ||
				errors.Is(err, errNonSelect) || errors.Is(err, errMultiStatement) ||
				errors.Is(err, errNoParquetSources) {
				http.Error(w, "bad query", http.StatusBadRequest)
				return
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				http.Error(w, "bad query", http.StatusBadRequest)
				return
			}
			logger.Error("sql sandbox", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		defer cleanup()

		if wantsArrowStream(r) {
			if err := writeArrowResponse(ctx, w, conn, req.SQL, rowCap, logger); err != nil {
				if errors.Is(err, errSandboxExec) || errors.Is(err, errEmptySQL) ||
					errors.Is(err, errNonSelect) || errors.Is(err, errMultiStatement) ||
					errors.Is(err, errNoParquetSources) {
					http.Error(w, "bad query", http.StatusBadRequest)
					return
				}
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					http.Error(w, "bad query", http.StatusBadRequest)
					return
				}
				logger.Error("sql arrow failed", "ns", ns, "err", err)
				http.Error(w, "query failed", http.StatusInternalServerError)
				return
			}
			return
		}

		result, err := queryJSON(ctx, conn, req.SQL, rowCap)
		if err != nil {
			if errors.Is(err, errSandboxExec) || errors.Is(err, errEmptySQL) ||
				errors.Is(err, errNonSelect) || errors.Is(err, errMultiStatement) ||
				errors.Is(err, errNoParquetSources) {
				http.Error(w, "bad query", http.StatusBadRequest)
				return
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				http.Error(w, "bad query", http.StatusBadRequest)
				return
			}
			logger.Error("sql failed", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
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

func prepareSandboxConn(ctx context.Context, tenantRoot string, limits sandboxLimits) (*sql.Conn, func(), error) {
	conn, cleanup, err := openSandboxConn(ctx, tenantRoot, limits)
	if err != nil {
		return nil, nil, err
	}
	viewSQL, err := sandboxMetricsUnionSQL(tenantRoot)
	if err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE VIEW "+sandboxMetricsView+" AS "+viewSQL); err != nil {
		cleanup()
		return nil, nil, wrapSandboxErr(fmt.Errorf("create metrics view: %w", err))
	}
	if err := lockSandbox(ctx, conn); err != nil {
		cleanup()
		return nil, nil, err
	}
	return conn, cleanup, nil
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

func sandboxMetricsUnionSQL(tenantRoot string) (string, error) {
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	absRoot = filepath.Clean(absRoot)

	paths, err := collectSafeParquetPaths(absRoot, tenantRoot)
	if err != nil {
		return "", err
	}
	if len(paths) == 0 {
		return "", errNoParquetSources
	}
	var parts []string
	for _, p := range paths {
		parts = append(parts, fmt.Sprintf(
			`SELECT "__name__", labels, value, timestamp_ms, ts FROM read_parquet('%s')`,
			layout.ToSlash(p),
		))
	}
	union := strings.Join(parts, " UNION ALL ")
	sqlText := union
	if !AssertNoUnionByName(sqlText) {
		return "", fmt.Errorf("view SQL must not use union_by_name or filename")
	}
	return sqlText, nil
}

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

func collectSafeParquetPaths(absTenantRoot, tenantRoot string) ([]string, error) {
	root, err := filepath.EvalSymlinks(absTenantRoot)
	if err != nil {
		root = absTenantRoot
	}
	root = filepath.Clean(root)

	var paths []string
	snapshot := filepath.Join(tenantRoot, hotSnapshotRel)
	if ok, err := safeTenantParquetFile(root, snapshot); err != nil {
		return nil, err
	} else if ok {
		paths = append(paths, snapshot)
	}
	for tier := 0; tier < maxTier; tier++ {
		glob := filepath.Join(tenantRoot, "tiers", fmt.Sprintf("L%d", tier), "*.parquet")
		matches, err := filepath.Glob(glob)
		if err != nil {
			return nil, err
		}
		for _, match := range matches {
			ok, err := safeTenantParquetFile(root, match)
			if err != nil {
				return nil, err
			}
			if ok {
				paths = append(paths, match)
			}
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

func tenantHasParquetSources(ctx context.Context, eng *engine.Engine, dataDir, tenant string) (bool, error) {
	for tier := 0; tier < maxTier; tier++ {
		glob := filepath.Join(dataDir, tenant, "tiers", fmt.Sprintf("L%d", tier), "*.parquet")
		if matches, _ := filepath.Glob(glob); len(matches) > 0 {
			return true, nil
		}
	}
	var hotRows, prevRows int64
	//nolint:contextcheck // probe uses request ctx inside WithRead callback.
	err := eng.WithRead(tenant, func(db *sql.DB) error {
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM hot_current").Scan(&hotRows); err != nil {
			return err
		}
		return db.QueryRowContext(ctx, "SELECT COUNT(*) FROM hot_prev").Scan(&prevRows)
	})
	if err != nil {
		return false, err
	}
	if hotRows > 0 || prevRows > 0 {
		return true, nil
	}
	return false, nil
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
