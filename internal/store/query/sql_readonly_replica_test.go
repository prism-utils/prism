package query_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/query"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

// TestSQLReadReplicaServesHotParquetWithoutEngine is the regression guard for
// the per-tenant read replica (RUN_JOBS=false, QUERY_HOT_ONLY=true, data dir
// mounted read-only). A replica must answer /sql purely from the parquet
// snapshots the shared writer exported (hot/current.parquet + tiers) and must
// NEVER open the tenant engine.duckdb: the mount is read-only, and the writer
// holds DuckDB's single-writer lock on that same file. Opening it here is the
// live prod failure `Cannot open file ".../engine.duckdb": Read-only file
// system`.
//
// To prove the replica path never touches the engine, we deliberately corrupt
// engine.duckdb after seeding. With RunJobs=false the query must still succeed
// against the hot snapshot; if a future change reintroduces an engine open on
// the read path, the corrupt file makes this test fail.
func TestSQLReadReplicaServesHotParquetWithoutEngine(t *testing.T) {
	dataDir := t.TempDir()

	// Seed the tenant via a *writer* engine so hot/current.parquet exists, then
	// release the engine so its file lock is gone (mirrors the shared writer
	// having exported the snapshot for this tenant).
	wEng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	seedTenantMetrics(t, wEng, dataDir, tenantSQLA, []testparquet.Row{
		{Name: "metric_a", Labels: "{}", Value: 10, TimestampMs: 1},
		{Name: "metric_a", Labels: "{}", Value: 20, TimestampMs: 2},
		{Name: "metric_b", Labels: "{}", Value: 30, TimestampMs: 3},
	})
	if err := wEng.Close(); err != nil {
		t.Fatalf("close writer engine: %v", err)
	}

	// Corrupt engine.duckdb: any attempt to open it as a database now errors,
	// standing in for the read-only mount + writer-held lock a replica sees.
	enginePath := filepath.Join(dataDir, tenantSQLA, "engine.duckdb")
	if err := os.WriteFile(enginePath, []byte("corrupt-not-a-duckdb-file"), 0o600); err != nil {
		t.Fatalf("corrupt engine.duckdb: %v", err)
	}

	// Read replica: RUN_JOBS=false → the handler must skip the hot-snapshot
	// export and serve from the sandbox (:memory: + read_parquet) only.
	rEng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = rEng.Close() })
	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) {
		c.RunJobs = false
		c.HotOnly = true
	})
	srv := testSQLServer(t, dataDir, cfg, rEng)

	code, out := execSQL(t, srv, tenantSQLA, "SELECT COUNT(*) AS n FROM metrics")
	if code != 200 {
		t.Fatalf("replica count status=%d want 200 (must not open engine.duckdb)", code)
	}
	if out.RowCount != 1 || len(out.Rows) != 1 {
		t.Fatalf("replica count result=%+v want single row", out)
	}
	// The three seeded rows must come back from the hot snapshot.
	if got := numericCell(t, out.Rows[0][0]); got != 3 {
		t.Fatalf("replica metrics count=%v want 3 (from hot/current.parquet)", got)
	}

	// A data-independent query also succeeds against the replica sandbox.
	code, out = execSQL(t, srv, tenantSQLA, "SELECT 1 AS ok")
	if code != 200 || out.RowCount != 1 {
		t.Fatalf("replica SELECT 1 status=%d result=%+v want 200/1 row", code, out)
	}
}

// TestSQLReadReplicaEmptyTenantReturnsEmpty confirms a freshly-provisioned
// replica tenant (dir exists, no parquet, engine.duckdb unopenable) still
// answers read-only queries with an empty, correctly-typed metrics view rather
// than a 500 (engine open) or a misleading 400.
func TestSQLReadReplicaEmptyTenantReturnsEmpty(t *testing.T) {
	dataDir := t.TempDir()
	tenantRoot := filepath.Join(dataDir, tenantSQLA)
	if err := os.MkdirAll(tenantRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	// Unopenable engine file — a replica must not touch it even with no data.
	if err := os.WriteFile(filepath.Join(tenantRoot, "engine.duckdb"), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) {
		c.RunJobs = false
		c.HotOnly = true
	})
	srv := testSQLServer(t, dataDir, cfg, eng)

	code, out := execSQL(t, srv, tenantSQLA, "SELECT COUNT(*) FROM metrics")
	if code != 200 {
		t.Fatalf("empty replica count status=%d want 200", code)
	}
	if out.RowCount != 1 {
		t.Fatalf("empty replica count result=%+v want single row", out)
	}
	if got := numericCell(t, out.Rows[0][0]); got != 0 {
		t.Fatalf("empty replica metrics count=%v want 0", got)
	}
}
