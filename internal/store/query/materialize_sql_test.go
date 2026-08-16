package query_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/materialize"
	"github.com/prism-utils/prism/internal/store/query"
)

func TestSQLMatViewReadsOnlyMaterializationFiles(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-matquery-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	// Garbage that DuckDB cannot open: the mat view must not union this path.
	if err := os.WriteFile(filepath.Join(l0, "poison-raw-tier.parquet"), []byte("not parquet"), 0o600); err != nil {
		t.Fatal(err)
	}

	matDir := layout.MaterializationDir(dataDir, tenant, "last_events")
	matPath := filepath.Join(matDir, "111-aaaaaaaa.parquet")
	writeMatX(t, matPath, 7)

	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) {
		c.MaterializationNames = []string{"last_events"}
		c.RunJobs = false
	})
	srv := testSQLServer(t, dataDir, cfg, eng)

	resp := postSQL(t, srv.URL+"/"+tenant+"/sql", `{"sql":"SELECT x FROM mat_last_events"}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d body=%s", resp.StatusCode, body)
	}
	out := decodeSQLResp(t, resp)
	if out.RowCount != 1 {
		t.Fatalf("row_count=%d want 1: %+v", out.RowCount, out)
	}
	switch v := out.Rows[0][0].(type) {
	case float64:
		if v != 7 {
			t.Fatalf("row = %#v, want 7", out.Rows[0][0])
		}
	case string:
		if v != "7" {
			t.Fatalf("row = %#v, want 7", out.Rows[0][0])
		}
	default:
		t.Fatalf("row = %#v, want 7", out.Rows[0][0])
	}

	explain := postSQL(t, srv.URL+"/"+tenant+"/sql", `{"sql":"EXPLAIN SELECT * FROM mat_last_events"}`)
	defer func() { _ = explain.Body.Close() }()
	planBody, _ := io.ReadAll(explain.Body)
	plan := strings.ToLower(string(planBody))
	if strings.Contains(plan, "tiers/") || strings.Contains(plan, "poison-raw") {
		t.Fatalf("mat view must not open raw tiers: %s", planBody)
	}
	if !strings.Contains(plan, "materializations") && !strings.Contains(plan, "last_events") {
		// EXPLAIN JSON may only show the view name; also assert LiveFiles isolation.
		files, err := materialize.LiveFiles(matDir)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.Contains(f, "tiers/") {
				t.Fatalf("live mat set includes tier path %s", f)
			}
		}
	}
}

func TestSQLMatViewEmptyTenant(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-matempty-apps"
	if err := os.MkdirAll(filepath.Join(dataDir, tenant), 0o750); err != nil {
		t.Fatal(err)
	}
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) {
		c.MaterializationNames = []string{"last_events"}
		c.RunJobs = false
	})
	srv := testSQLServer(t, dataDir, cfg, eng)
	resp := postSQL(t, srv.URL+"/"+tenant+"/sql", `{"sql":"SELECT * FROM mat_last_events"}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("empty mat view must bind: status %d body=%s", resp.StatusCode, body)
	}
	out := decodeSQLResp(t, resp)
	if out.RowCount != 0 {
		t.Fatalf("empty tenant rows=%d want 0", out.RowCount)
	}
}

func writeMatX(t *testing.T, path string, x int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	tmp := path + ".tmp"
	q := fmt.Sprintf("COPY (SELECT %d AS x) TO '%s' (FORMAT parquet)", x, filepath.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}
