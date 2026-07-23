package query

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

func TestBundledDuckDBVersionAtLeast12(t *testing.T) {
	t.Parallel()
	connector, err := duckdb.NewConnector(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		_ = db.Close()
		_ = connector.Close()
	})
	var version string
	if err := db.QueryRowContext(context.Background(), "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("bundled DuckDB: %s", version)
	if !duckDBVersionAtLeast(version, 1, 2) {
		t.Fatalf("version=%q want DuckDB >= 1.2", version)
	}
}

func duckDBVersionAtLeast(version string, major, minor int) bool {
	var gotMajor, gotMinor int
	if _, err := fmt.Sscanf(version, "v%d.%d", &gotMajor, &gotMinor); err != nil {
		if _, err := fmt.Sscanf(version, "%d.%d", &gotMajor, &gotMinor); err != nil {
			return false
		}
	}
	if gotMajor > major {
		return true
	}
	if gotMajor == major && gotMinor >= minor {
		return true
	}
	return false
}

func TestApplySandboxBootstrap_setsAllowedDirectories(t *testing.T) {
	t.Parallel()
	tenantRoot := t.TempDir()
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		t.Fatal(err)
	}
	connector, err := duckdb.NewConnector(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = db.Close()
		_ = connector.Close()
	})

	if err := applySandboxBootstrap(context.Background(), conn, absRoot, sandboxLimits{}); err != nil {
		t.Fatal(err)
	}
	var dirs any
	if err := conn.QueryRowContext(context.Background(), "SELECT current_setting('allowed_directories')").Scan(&dirs); err != nil {
		t.Fatal(err)
	}
	dirText := fmt.Sprint(dirs)
	if !strings.Contains(dirText, absRoot) {
		t.Fatalf("allowed_directories=%q want %q", dirText, absRoot)
	}
}

func TestApplySandboxBootstrap_appliesThreads(t *testing.T) {
	t.Parallel()
	tenantRoot := t.TempDir()
	connector, err := duckdb.NewConnector(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		_ = db.Close()
		_ = connector.Close()
	})

	const want = 2
	if err := applySandboxBootstrap(context.Background(), conn, tenantRoot, sandboxLimits{Threads: want}); err != nil {
		t.Fatal(err)
	}
	var threads string
	if err := conn.QueryRowContext(context.Background(), "SELECT current_setting('threads')").Scan(&threads); err != nil {
		t.Fatal(err)
	}
	if threads != "2" {
		t.Fatalf("threads=%q want 2", threads)
	}
}
