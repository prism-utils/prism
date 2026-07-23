package query

import (
	"context"
	"database/sql"
	"testing"

	duckdb "github.com/marcboeker/go-duckdb"
)

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
