package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Request is a time-range metrics query.
type Request struct {
	Tenant string
	Start  time.Time
	End    time.Time
	Step   string
}

// Row is one JSON result row.
type Row struct {
	Metric      string    `json:"metric"`
	Labels      string    `json:"labels,omitempty"`
	Value       float64   `json:"value"`
	TimestampMs int64     `json:"timestamp_ms"`
	Ts          time.Time `json:"ts"`
}

// Builder constructs unified-view SQL without union_by_name or filename.
type Builder struct {
	DataDir string
}

// BuildSQL returns parameterized SQL and args for the unified view.
func (b *Builder) BuildSQL(req *Request) (string, []any, error) {
	return "", nil, fmt.Errorf("query: not implemented")
}

// BuildSQLWithDB returns SQL using the tenant catalog to omit absent hot tables.
func (b *Builder) BuildSQLWithDB(ctx context.Context, req *Request, db *sql.DB) (string, []any, error) {
	return "", nil, fmt.Errorf("query: not implemented")
}

// Execute runs the unified query against the tenant DuckDB and returns rows.
func Execute(ctx context.Context, db *sql.DB, sqlText string, args []any) ([]Row, error) {
	return nil, fmt.Errorf("query: not implemented")
}

// ToJSON encodes rows for the HTTP response.
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

func pickRollupStep(step string, start, end time.Time) string {
	return ""
}
