package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
)

// Request is a time-range metrics query.
type Request struct {
	Tenant string
	Start  time.Time
	End    time.Time
	Step   string // optional rollup step e.g. 1m, 5m, 1h
}

// Row is one JSON result row.
type Row struct {
	Metric      string    `json:"metric"`
	Labels      string    `json:"labels,omitempty"`
	Value       float64   `json:"value"`
	TimestampMs int64     `json:"timestamp_ms"`
	Ts          time.Time `json:"ts"`
}

const (
	hotCurrentTable = "hot_current"
	hotPrevTable    = "hot_prev"
	maxTier         = 8
)

// Builder constructs unified-view SQL without union_by_name or filename.
type Builder struct {
	DataDir string
	HotOnly bool
}

// BuildSQL returns parameterized SQL and args for the unified view.
func (b *Builder) BuildSQL(req *Request) (string, []any, error) {
	return b.buildSQL(context.Background(), req, nil)
}

// BuildSQLWithDB returns SQL using the tenant catalog to omit absent hot tables.
func (b *Builder) BuildSQLWithDB(ctx context.Context, req *Request, db *sql.DB) (string, []any, error) {
	return b.buildSQL(ctx, req, db)
}

func (b *Builder) buildSQL(ctx context.Context, req *Request, db *sql.DB) (string, []any, error) {
	if req.Tenant == "" {
		return "", nil, fmt.Errorf("query: missing tenant")
	}
	tenantRoot := filepath.Join(b.DataDir, req.Tenant)
	if _, err := os.Stat(tenantRoot); err != nil {
		return "", nil, fmt.Errorf("query: tenant root: %w", err)
	}

	parts := []string{
		fmt.Sprintf("SELECT * FROM %s WHERE ts >= ? AND ts < ?", hotCurrentTable),
	}
	if db == nil || hotTableExists(ctx, db, hotPrevTable) {
		parts = append(parts,
			fmt.Sprintf("SELECT * FROM %s WHERE ts >= ? AND ts < ?", hotPrevTable),
		)
	}

	if !b.HotOnly {
		for tier := 0; tier < maxTier; tier++ {
			dir := filepath.Join(tenantRoot, "tiers", fmt.Sprintf("L%d", tier))
			entries, err := os.ReadDir(dir)
			if err != nil {
				if !os.IsNotExist(err) {
					return "", nil, err
				}
				continue
			}
			for _, e := range entries {
				if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
					continue
				}
				ext := filepath.Ext(e.Name())
				if ext != ".parquet" && ext != ".duckdb" {
					continue
				}
				p := filepath.Join(dir, e.Name())
				if ext == ".duckdb" {
					if db == nil {
						continue
					}
					alias := sanitizeAlias(fmt.Sprintf("qseg_%d_%s", tier, strings.TrimSuffix(e.Name(), ext)))
					if _, err := db.ExecContext(ctx, fmt.Sprintf(
						"ATTACH '%s' AS %s (READ_ONLY)", layout.ToSlash(p), alias,
					)); err != nil {
						return "", nil, fmt.Errorf("query: attach %s: %w", p, err)
					}
					parts = append(parts, fmt.Sprintf(
						"SELECT * FROM %s.metrics WHERE ts >= ? AND ts < ?", alias,
					))
					continue
				}
				parts = append(parts, fmt.Sprintf(
					"SELECT * FROM read_parquet('%s') WHERE ts >= ? AND ts < ?",
					layout.ToSlash(p),
				))
			}
		}

		if step := pickRollupStep(req.Step, req.Start, req.End); step != "" {
			glob := filepath.Join(tenantRoot, "rollups", step, "*.parquet")
			if matches, _ := filepath.Glob(glob); len(matches) > 0 {
				parts = append(parts, fmt.Sprintf(
					`SELECT "__name__", '{}' AS labels, avg AS value, 0 AS timestamp_ms, bucket AS ts
				 FROM read_parquet('%s') WHERE bucket >= ? AND bucket < ?`,
					layout.ToSlash(glob),
				))
			}
		}
	}

	union := strings.Join(parts, " UNION ALL ")
	sqlText := fmt.Sprintf("SELECT * FROM (%s) ORDER BY ts", union)

	var args []any
	for range parts {
		args = append(args, req.Start, req.End)
	}
	return sqlText, args, nil
}

// AggregateSQL rewrites a unified row query as COUNT/SUM over the union subquery.
// The input must be the shape produced by BuildSQL: SELECT * FROM (…) ORDER BY ts.
func AggregateSQL(sqlText string) (string, error) {
	const open = "SELECT * FROM ("
	const close = ") ORDER BY ts"
	if !strings.HasPrefix(sqlText, open) || !strings.HasSuffix(sqlText, close) {
		return "", fmt.Errorf("query: unexpected unified sql shape")
	}
	inner := sqlText[len(open) : len(sqlText)-len(close)]
	return "SELECT COUNT(*), COALESCE(SUM(value), 0) FROM (" + inner + ") AS agg", nil
}

func hotTableExists(ctx context.Context, db *sql.DB, table string) bool {
	//nolint:gosec // G201: table name is a package const, not user input.
	_, err := db.ExecContext(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", table))
	return err == nil
}

func sanitizeAlias(s string) string {
	var b strings.Builder
	b.WriteString("a_")
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// Execute runs the unified query against the tenant DuckDB and returns rows.
func Execute(ctx context.Context, db *sql.DB, sqlText string, args []any) ([]Row, error) {
	rows, err := db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Row
	for rows.Next() {
		var r Row
		var name, labels sql.NullString
		var value sql.NullFloat64
		var tsMs sql.NullInt64
		if err := rows.Scan(&name, &labels, &value, &tsMs, &r.Ts); err != nil {
			return nil, err
		}
		r.Metric = name.String
		r.Labels = labels.String
		r.Value = value.Float64
		r.TimestampMs = tsMs.Int64
		r.Ts = r.Ts.UTC()
		out = append(out, r)
	}
	if out == nil {
		out = []Row{}
	}
	return out, rows.Err()
}

// pickRollupStep chooses coarsest rollup when step hint or range is wide.
func pickRollupStep(step string, start, end time.Time) string {
	if step != "" {
		return step
	}
	if end.Sub(start) >= 7*24*time.Hour {
		return "1h"
	}
	if end.Sub(start) >= 24*time.Hour {
		return "5m"
	}
	if end.Sub(start) >= time.Hour {
		return "1m"
	}
	return ""
}

// ToJSON encodes query rows as JSON for the HTTP response body.
// The payload always includes a "rows" array. When exposeSQL is true, a "sql"
// field is added with the generated unified query text (for e2e regression guards).
func ToJSON(rows []Row, exposeSQL bool, sqlText string) ([]byte, error) {
	out := map[string]any{"rows": rows}
	if exposeSQL {
		out["sql"] = sqlText
	}
	return json.Marshal(out)
}

// ExposeSQLFromEnv reports whether query responses should include generated SQL.
func ExposeSQLFromEnv() bool {
	return os.Getenv("E2E_EXPOSE_QUERY_SQL") == "1"
}

// AssertNoUnionByName is a test helper guard.
func AssertNoUnionByName(sql string) bool {
	lower := strings.ToLower(sql)
	return !strings.Contains(lower, "union_by_name") && !strings.Contains(lower, "filename")
}
