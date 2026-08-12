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

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/testparquet"
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

// landLogsSummary builds a logs-summary parquet fixture, lands it via the
// engine's land-as-file path (the same path HTTP ingest uses for logs), and
// refreshes it into a tier so `/sql` can see the rows.
func landLogsSummary(t *testing.T, eng *engine.Engine, dataDir, tenant string, rows []testparquet.LogSummaryRow) {
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
	testparquet.PromoteLandedLogsToTier(t, dataDir, tenant, "logs-summary")
}

func TestSQLLogsTemplateCount(t *testing.T) {
	dataDir, eng := newLogsSQLFixture(t)
	// Two summary windows accumulate per-template counts across files.
	landLogsSummary(t, eng, dataDir, tenantLogs, []testparquet.LogSummaryRow{{Template: "a", Count: 3}, {Template: "b", Count: 1}})
	landLogsSummary(t, eng, dataDir, tenantLogs, []testparquet.LogSummaryRow{{Template: "a", Count: 2}, {Template: "c", Count: 4}})

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
	landLogsSummary(t, eng, dataDir, tenantLogs, []testparquet.LogSummaryRow{{Template: "a", Count: 3}})
	landRawLog(t, eng, dataDir, tenantLogs, "hello world", "none")

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

// TestSQLLogsExposesIngestTSAndTimeBoundedCount proves /sql FROM logs exposes
// __prism_ts_ns and that a last-hour-style filter returns only recent rows.
func TestSQLLogsExposesIngestTSAndTimeBoundedCount(t *testing.T) {
	query.InvalidateLogsMetaCache("")
	dataDir, eng := newLogsSQLFixture(t)
	dir := filepath.Join(dataDir, tenantLogs, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	recentAt := now.Add(-30 * time.Minute)
	oldAt := now.Add(-2 * time.Hour)
	testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(recentAt)), []testparquet.LogRow{
		{Message: "recent", Format: "none"},
	})
	testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(oldAt)), []testparquet.LogRow{
		{Message: "old", Format: "none"},
	})

	srv := testSQLServer(t, dataDir, nil, eng)

	code, out := execSQL(t, srv, tenantLogs, "SELECT message, __prism_ts_ns FROM logs ORDER BY message")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (column __prism_ts_ns must exist on /sql logs)", code)
	}
	if len(out.Rows) != 2 {
		t.Fatalf("rows = %v, want 2", out.Rows)
	}
	foundTS := false
	for _, col := range out.Columns {
		if col == "__prism_ts_ns" {
			foundTS = true
			break
		}
	}
	if !foundTS {
		t.Fatalf("columns = %v, want __prism_ts_ns", out.Columns)
	}

	cutoff := now.Add(-time.Hour).UnixNano()
	// COUNT(*) is the reliable ingest-row total when only raw windows are present
	// (no `count` column). Summary-aware tenants use SUM(COALESCE(count, 1)).
	code, out = execSQL(t, srv, tenantLogs, fmt.Sprintf(
		`SELECT CAST(COUNT(*) AS BIGINT) AS ingested FROM logs WHERE __prism_ts_ns >= %d`,
		cutoff,
	))
	if code != http.StatusOK {
		t.Fatalf("time-bounded status = %d, want 200", code)
	}
	if len(out.Rows) != 1 {
		t.Fatalf("time-bounded rows = %v, want 1", out.Rows)
	}
	if got := numericCell(t, out.Rows[0][0]); got != 1 {
		t.Fatalf("ingested last 1h = %v, want 1 (recent only)", got)
	}
}

// landRawLog lands a logs-raw parquet (message, format columns) via the engine
// and refreshes it into a tier so `/sql` can see the rows.
func landRawLog(t *testing.T, eng *engine.Engine, dataDir, tenant, message, format string) {
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
	testparquet.PromoteLandedLogsToTier(t, dataDir, tenant, "logs-raw")
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
