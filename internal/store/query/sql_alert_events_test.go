package query_test

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const tenantAlerts = "user-alerts-3c1b"

func landAlertEvents(t *testing.T, eng *engine.Engine, dataDir, tenant string, rows []testparquet.AlertEventRow) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "w.parquet")
	testparquet.WriteAlertEventsFile(t, path, rows)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := eng.LandAlertWindow(tenant, "alert-events", f); err != nil {
		t.Fatalf("land alert-events: %v", err)
	}
}

func TestSQLAlertEventsEmptyTenantReturnsZeroRows(t *testing.T) {
	dataDir, eng := newLogsSQLFixture(t)
	if err := os.MkdirAll(filepath.Join(dataDir, tenantAlerts), 0o750); err != nil {
		t.Fatal(err)
	}
	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantAlerts, "SELECT * FROM alert_events")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 on empty alert_events (typed stub, not 400)", code)
	}
	if len(out.Rows) != 0 {
		t.Fatalf("rows = %v, want empty", out.Rows)
	}
	code, out = execSQL(t, srv, tenantAlerts, "SELECT fingerprint, status, ends_at FROM alert_events")
	if code != http.StatusOK {
		t.Fatalf("named-column status = %d, want 200", code)
	}
	if len(out.Rows) != 0 {
		t.Fatalf("named-column rows = %v, want empty", out.Rows)
	}
}

func TestSQLAlertEventsReturnsIngestedRows(t *testing.T) {
	dataDir, eng := newLogsSQLFixture(t)
	fired := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	resolved := fired.Add(5 * time.Minute)
	landAlertEvents(t, eng, dataDir, tenantAlerts, []testparquet.AlertEventRow{
		{
			Ts:             fired,
			Fingerprint:    "58a171d28f29e910",
			Alertname:      "HighCPU",
			Severity:       "warning",
			Status:         "firing",
			Summary:        "CPU > 90%",
			Description:    "node-a is hot",
			Recommendation: "scale the pool",
			StartsAt:       fired,
			Labels:         `{"alertname":"HighCPU","severity":"warning"}`,
		},
		{
			Ts:             resolved,
			Fingerprint:    "58a171d28f29e910",
			Alertname:      "HighCPU",
			Severity:       "warning",
			Status:         "resolved",
			Summary:        "CPU > 90%",
			Description:    "node-a is hot",
			Recommendation: "scale the pool",
			StartsAt:       fired,
			EndsAt:         resolved,
			Labels:         `{"alertname":"HighCPU","severity":"warning"}`,
		},
	})

	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantAlerts, `
SELECT fingerprint, alertname, severity, status, summary, description, recommendation, labels
FROM alert_events ORDER BY status`)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if len(out.Rows) != 2 {
		t.Fatalf("rows = %v, want 2", out.Rows)
	}
	firing := out.Rows[0]
	if firing[3] != "firing" {
		t.Fatalf("first status = %v, want firing (ORDER BY status)", firing[3])
	}
	if firing[0] != "58a171d28f29e910" || firing[1] != "HighCPU" || firing[2] != "warning" {
		t.Fatalf("firing identity = %v", firing[:3])
	}
	if firing[4] != "CPU > 90%" || firing[5] != "node-a is hot" || firing[6] != "scale the pool" {
		t.Fatalf("firing annotations = %v", firing[4:7])
	}
	resolvedRow := out.Rows[1]
	if resolvedRow[3] != "resolved" {
		t.Fatalf("second status = %v, want resolved", resolvedRow[3])
	}

	code, out = execSQL(t, srv, tenantAlerts, "SELECT status, ends_at FROM alert_events WHERE status = 'firing'")
	if code != http.StatusOK {
		t.Fatalf("firing ends_at status = %d, want 200", code)
	}
	if len(out.Rows) != 1 {
		t.Fatalf("firing rows = %v, want 1", out.Rows)
	}
	if out.Rows[0][1] != nil {
		t.Fatalf("firing ends_at = %v, want null", out.Rows[0][1])
	}
}

func TestSQLAlertEventsUnaffectedByHotOnly(t *testing.T) {
	dataDir, eng := newLogsSQLFixture(t)
	fired := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	landAlertEvents(t, eng, dataDir, tenantAlerts, []testparquet.AlertEventRow{{
		Ts:          fired,
		Fingerprint: "aabbccddeeff0011",
		Alertname:   "DiskFull",
		Severity:    "critical",
		Status:      "firing",
		StartsAt:    fired,
		Labels:      `{"alertname":"DiskFull"}`,
	}})

	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) { c.HotOnly = true })
	srv := testSQLServer(t, dataDir, cfg, eng)
	code, out := execSQL(t, srv, tenantAlerts, "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM alert_events")
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 under QUERY_HOT_ONLY", code)
	}
	if got := numericCell(t, out.Rows[0][0]); got != 1 {
		t.Fatalf("hot-only alert_events count = %v, want 1 (must not hide the alerts plane)", got)
	}
}

func TestSQLAlertEventsDoesNotAppearInLogs(t *testing.T) {
	dataDir, eng := newLogsSQLFixture(t)
	fired := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	landAlertEvents(t, eng, dataDir, tenantAlerts, []testparquet.AlertEventRow{{
		Ts:          fired,
		Fingerprint: "0011223344556677",
		Alertname:   "X",
		Status:      "firing",
		StartsAt:    fired,
	}})
	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantAlerts, "SELECT CAST(COUNT(*) AS BIGINT) AS n FROM logs")
	if code != http.StatusOK {
		t.Fatalf("logs status = %d, want 200", code)
	}
	if got := numericCell(t, out.Rows[0][0]); got != 0 {
		t.Fatalf("logs count = %v, want 0 (alert-events must not pollute logs)", got)
	}
}
