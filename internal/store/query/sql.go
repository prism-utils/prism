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
	"strings"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/layout"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
	duckdb "github.com/marcboeker/go-duckdb"
)

const (
	defaultSQLMaxRows  = 100_000
	defaultSQLTimeout  = 30 * time.Second
	sandboxMetricsView = "metrics"
)

var (
	errEmptySQL         = errors.New("query: empty sql")
	errNonSelect        = errors.New("query: non-select sql")
	errMultiStatement   = errors.New("query: multi-statement sql")
	errSandboxExec      = errors.New("query: sandbox exec")
	errNoParquetSources = errors.New("query: no parquet sources")
)

// SQLConfig holds arbitrary SQL API settings.
type SQLConfig struct {
	DataDir     string
	RoutePrefix string
	MaxRows     int
	Timeout     time.Duration
	MemoryLimit string
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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ns := r.PathValue("ns")
		if !storeingest.ValidateTenant(ns) {
			http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
			return
		}

		var req SQLRequest
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
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
		if _, err := os.Stat(tenantRoot); err != nil { //nolint:gosec // G703: ns validated before join
			if os.IsNotExist(err) {
				http.Error(w, storetenant.UnknownTenantBody, http.StatusNotFound)
				return
			}
			logger.Error("sql stat tenant root", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		absRoot, err := filepath.Abs(tenantRoot)
		if err != nil {
			logger.Error("sql abs tenant root", "ns", ns, "err", err)
			http.Error(w, "query failed", http.StatusInternalServerError)
			return
		}
		absRoot = filepath.Clean(absRoot)

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

		result, err := runSandboxQuery(ctx, absRoot, req.SQL, rowCap, cfg.MemoryLimit)
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

func runSandboxQuery(ctx context.Context, tenantRoot, userSQL string, rowCap int, memoryLimit string) (*SQLResponse, error) {
	conn, cleanup, err := openSandboxConn(ctx, tenantRoot, memoryLimit)
	if err != nil {
		return nil, fmt.Errorf("query: open sandbox: %w", err)
	}
	defer cleanup()

	viewSQL, err := sandboxMetricsUnionSQL(tenantRoot)
	if err != nil {
		return nil, wrapSandboxErr(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE "+sandboxMetricsView+" AS "+viewSQL); err != nil {
		return nil, wrapSandboxErr(fmt.Errorf("materialize metrics: %w", err))
	}
	if err := lockSandbox(ctx, conn); err != nil {
		return nil, err
	}

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

func openSandboxConn(ctx context.Context, tenantRoot, memoryLimit string) (*sql.Conn, func(), error) {
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
	if err := applySandboxBootstrap(ctx, conn, tenantRoot, memoryLimit); err != nil {
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

func applySandboxBootstrap(ctx context.Context, conn *sql.Conn, tenantRoot, memoryLimit string) error {
	var steps []string
	if memoryLimit != "" {
		steps = append(steps, fmt.Sprintf("SET memory_limit='%s'", escapeSQLLiteral(memoryLimit)))
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
	// allowed_directories exists in DuckDB ≥1.2; best-effort before external access is disabled.
	dirSet := fmt.Sprintf("SET allowed_directories=[%s]", quoteSQLPath(tenantRoot))
	if _, err := conn.ExecContext(ctx, dirSet); err != nil && !isUnknownConfig(err) {
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

func isUnknownConfig(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unrecognized configuration parameter")
}

func sandboxMetricsUnionSQL(tenantRoot string) (string, error) {
	var parts []string
	snapshot := filepath.Join(tenantRoot, hotSnapshotRel)
	if _, err := os.Stat(snapshot); err == nil {
		parts = append(parts, fmt.Sprintf(
			`SELECT "__name__", labels, value, timestamp_ms, ts FROM read_parquet('%s')`,
			layout.ToSlash(snapshot),
		))
	}
	for tier := 0; tier < maxTier; tier++ {
		glob := filepath.Join(tenantRoot, "tiers", fmt.Sprintf("L%d", tier), "*.parquet")
		if matches, _ := filepath.Glob(glob); len(matches) > 0 {
			parts = append(parts, fmt.Sprintf(
				`SELECT "__name__", labels, value, timestamp_ms, ts FROM read_parquet('%s')`,
				layout.ToSlash(glob),
			))
		}
	}
	if len(parts) == 0 {
		return "", errNoParquetSources
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
	upper := strings.ToUpper(s)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		return errNonSelect
	}
	return nil
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
func SQLConfigFromEnv(dataDir, routePrefix, memoryLimit string) SQLConfig {
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
	return SQLConfig{
		DataDir:     dataDir,
		RoutePrefix: routePrefix,
		MaxRows:     maxRows,
		Timeout:     timeout,
		MemoryLimit: memoryLimit,
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
