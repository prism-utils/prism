package query_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/testparquet"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

const tenantLogs = "user-logs-9f2a"

// newLogsSQLFixture builds a store with a landed logs-summary window so /sql can
// query the `logs` relation.
func newLogsSQLFixture(t *testing.T) (string, *engine.Engine) {
	t.Helper()
	dataDir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, nil)
	t.Cleanup(func() { _ = eng.Close() })
	return dataDir, eng
}

// landLogsSummary builds a logs-summary parquet fixture and lands it via the
// engine's land-as-file path (the same path HTTP ingest uses for logs).
func landLogsSummary(t *testing.T, eng *engine.Engine, tenant string, rows []testparquet.LogSummaryRow) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "w.parquet")
	testparquet.WriteLogsSummaryFile(t, path, rows)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := eng.LandLogWindow(tenant, "logs-summary", f); err != nil {
		t.Fatalf("land logs-summary: %v", err)
	}
}

func TestSQLLogsTemplateCount(t *testing.T) {
	dataDir, eng := newLogsSQLFixture(t)
	// Two summary windows accumulate per-template counts across files.
	landLogsSummary(t, eng, tenantLogs, []testparquet.LogSummaryRow{{Template: "a", Count: 3}, {Template: "b", Count: 1}})
	landLogsSummary(t, eng, tenantLogs, []testparquet.LogSummaryRow{{Template: "a", Count: 2}, {Template: "c", Count: 4}})

	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantLogs, "SELECT template, CAST(sum(count) AS BIGINT) AS count FROM logs GROUP BY template ORDER BY count DESC, template")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	// a=5, c=4, b=1 (ordered by count desc).
	want := []struct {
		tmpl  string
		count float64
	}{{"a", 5}, {"c", 4}, {"b", 1}}
	if len(out.Rows) != len(want) {
		t.Fatalf("rows = %v, want %d groups", out.Rows, len(want))
	}
	for i, w := range want {
		gotTmpl, _ := out.Rows[i][0].(string)
		gotCount := numericCell(t, out.Rows[i][1])
		if gotTmpl != w.tmpl || gotCount != w.count {
			t.Errorf("row %d = (%v, %v), want (%s, %v)", i, out.Rows[i][0], gotCount, w.tmpl, w.count)
		}
	}
}

func TestSQLLogsEmptyTenantReturnsZeroRows(t *testing.T) {
	dataDir, eng := newLogsSQLFixture(t)
	// Provision the tenant root without any logs so the view exists but is empty.
	if err := os.MkdirAll(filepath.Join(dataDir, tenantLogs), 0o750); err != nil {
		t.Fatal(err)
	}
	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantLogs, "SELECT template, count FROM logs")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 on empty logs", code)
	}
	if len(out.Rows) != 0 {
		t.Fatalf("rows = %v, want empty", out.Rows)
	}
}

// TestSQLLogsUnionByName proves files with different schemas (summary vs raw)
// unify into one `logs` relation instead of erroring on mismatched columns.
func TestSQLLogsUnionByName(t *testing.T) {
	dataDir, eng := newLogsSQLFixture(t)
	landLogsSummary(t, eng, tenantLogs, []testparquet.LogSummaryRow{{Template: "a", Count: 3}})
	landRawLog(t, eng, tenantLogs, "hello world", "none")

	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantLogs, "SELECT count(*) AS c FROM logs")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := numericCell(t, out.Rows[0][0]); got != 2 {
		t.Fatalf("total rows = %v, want 2 (summary + raw unified)", got)
	}
	// The raw file's message column is readable even though the summary file
	// lacks it (union_by_name NULL-fills).
	code, out = execSQL(t, srv, tenantLogs, "SELECT message FROM logs WHERE message IS NOT NULL")
	if code != http.StatusOK {
		t.Fatalf("message status = %d, want 200", code)
	}
	if len(out.Rows) != 1 {
		t.Fatalf("message rows = %v, want 1", out.Rows)
	}
	if msg, _ := out.Rows[0][0].(string); msg != "hello world" {
		t.Fatalf("message = %v, want hello world", out.Rows[0][0])
	}
}

// landRawLog lands a logs-raw parquet (message, format columns) via the engine.
func landRawLog(t *testing.T, eng *engine.Engine, tenant, message, format string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.parquet")
	writeRawLogParquet(t, path, message, format)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open raw fixture: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := eng.LandLogWindow(tenant, "logs-raw", f); err != nil {
		t.Fatalf("land logs-raw: %v", err)
	}
}

func writeRawLogParquet(t *testing.T, path, message, format string) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("connector: %v", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf(
		`COPY (SELECT '%s' AS message, '%s' AS format) TO '%s' (FORMAT parquet)`,
		message, format, filepath.ToSlash(path),
	)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("copy raw log: %v", err)
	}
}
