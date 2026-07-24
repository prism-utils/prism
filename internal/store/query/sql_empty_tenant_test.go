package query

import (
	"context"
	"database/sql"
	"testing"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// A tenant root with no parquet sources (freshly-provisioned, or hot-only with
// no rows) must not error: sandboxMetricsUnionSQL yields the typed empty
// metrics view so read-only queries return zero rows instead of a misleading
// 400 "bad query".
func TestSandboxMetricsUnionSQL_EmptyTenantYieldsEmptyView(t *testing.T) {
	t.Parallel()
	got, err := sandboxMetricsUnionSQL(t.TempDir(), false)
	if err != nil {
		t.Fatalf("unexpected error for empty tenant: %v", err)
	}
	if got != emptyMetricsViewSQL {
		t.Fatalf("got %q, want emptyMetricsViewSQL", got)
	}
}

// The empty metrics view must be valid DuckDB, expose the same columns as the
// read_parquet projection, and contain zero rows. A non-table query such as
// SELECT 1 must also succeed against the empty sandbox.
func TestEmptyMetricsView_ValidColumnsAndEmpty(t *testing.T) {
	t.Parallel()
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

	if _, err := conn.ExecContext(context.Background(),
		"CREATE VIEW "+sandboxMetricsView+" AS "+emptyMetricsViewSQL); err != nil {
		t.Fatalf("create empty metrics view: %v", err)
	}

	var one int
	if err := conn.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("select 1 against empty sandbox: %v", err)
	}
	if one != 1 {
		t.Fatalf("select 1 = %d want 1", one)
	}

	var n int
	if err := conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM "+sandboxMetricsView).Scan(&n); err != nil {
		t.Fatalf("count metrics: %v", err)
	}
	if n != 0 {
		t.Fatalf("empty metrics view rows=%d want 0", n)
	}

	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM "+sandboxMetricsView)
	if err != nil {
		t.Fatalf("select metrics: %v", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	want := []string{"__name__", "labels", "value", "timestamp_ms", "ts"}
	if len(cols) != len(want) {
		t.Fatalf("cols=%v want %v", cols, want)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("col[%d]=%q want %q", i, cols[i], want[i])
		}
	}
}
