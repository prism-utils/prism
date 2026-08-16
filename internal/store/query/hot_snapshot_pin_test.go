package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/segformat"
)

const (
	prefetchErrText  = "Prefetch registered for bytes outside file"
	emptyParquetSize = 168
	hotPinGlob       = ".read-*"
	pinTenant        = "user-hotsnappin-apps"
)

// Replacing hot/current.parquet with a zero-row snapshot mid-scan must not mix
// the bound parquet footer with the new file's bytes. The sandbox keeps the
// original rows and must not surface DuckDB's prefetch-outside-file error.
func TestMetricsSandboxSurvivesEmptyHotSnapshotReplace(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	current := filepath.Join(hotDir, "current.parquet")
	const n = 64
	writeMultiRowGroupHotParquet(t, current, n)

	conn, cleanup, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare metrics sandbox: %v", err)
	}
	defer cleanup()

	empty := filepath.Join(t.TempDir(), "empty.parquet")
	writeEmptyHotParquet(t, empty)
	if fi, err := os.Stat(empty); err != nil {
		t.Fatal(err)
	} else if fi.Size() != emptyParquetSize {
		t.Fatalf("empty hot parquet size=%d want %d", fi.Size(), emptyParquetSize)
	}

	if err := os.Rename(empty, current); err != nil {
		t.Fatalf("replace current.parquet: %v", err)
	}

	rows, err := conn.QueryContext(ctx, `SELECT labels, value FROM metrics`)
	if err != nil {
		if strings.Contains(err.Error(), prefetchErrText) {
			t.Fatalf("prefetch mixed footer with replaced bytes: %v", err)
		}
		t.Fatalf("select metrics: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := 0
	for rows.Next() {
		got++
	}
	if err := rows.Err(); err != nil {
		if strings.Contains(err.Error(), prefetchErrText) {
			t.Fatalf("prefetch mixed footer with replaced bytes: %v", err)
		}
		t.Fatalf("scan metrics: %v", err)
	}
	if got != n {
		t.Fatalf("rows=%d want %d (snapshot at sandbox open)", got, n)
	}
}

func TestMetricsSandboxViewReadsPinnedHotParquet(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	writeMultiRowGroupHotParquet(t, filepath.Join(hotDir, "current.parquet"), 8)

	conn, cleanup, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare metrics sandbox: %v", err)
	}
	defer cleanup()

	pins := listHotPins(t, hotDir)
	if len(pins) != 1 {
		t.Fatalf("hot pins = %v, want one unique sibling under hot/", pins)
	}
	if filepath.Dir(pins[0]) != hotDir {
		t.Fatalf("pin %q is not a sibling under %s", pins[0], hotDir)
	}
	viewSQL := metricsViewSQL(t, ctx, conn)
	if strings.Contains(viewSQL, "current.parquet") {
		t.Fatalf("metrics view SQL must not read current.parquet: %s", viewSQL)
	}
	if !strings.Contains(viewSQL, filepath.Base(pins[0])) {
		t.Fatalf("metrics view SQL must read pin %s: %s", pins[0], viewSQL)
	}
	currentInfo, err := os.Stat(filepath.Join(hotDir, "current.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	pinInfo, err := os.Stat(pins[0])
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(currentInfo, pinInfo) {
		t.Fatal("pin must share the live snapshot inode on the same filesystem")
	}
}

func TestMetricsSandboxPinsAreUniquePerConn(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	writeMultiRowGroupHotParquet(t, filepath.Join(hotDir, "current.parquet"), 4)

	_, cleanup1, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare sandbox 1: %v", err)
	}
	defer cleanup1()
	_, cleanup2, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare sandbox 2: %v", err)
	}
	defer cleanup2()

	pins := listHotPins(t, hotDir)
	if len(pins) != 2 {
		t.Fatalf("hot pins = %v, want two unique siblings", pins)
	}
	if pins[0] == pins[1] {
		t.Fatalf("pin names must differ: %v", pins)
	}
}

func TestMetricsSandboxUnlinksPinsAfterCleanup(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	writeMultiRowGroupHotParquet(t, filepath.Join(hotDir, "current.parquet"), 4)

	conn, cleanup, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare metrics sandbox: %v", err)
	}
	if len(listHotPins(t, hotDir)) == 0 {
		t.Fatal("want a hot/.read-* pin while the sandbox is open")
	}
	_ = conn
	cleanup()
	if pins := listHotPins(t, hotDir); len(pins) != 0 {
		t.Fatalf("leftover pins after cleanup: %v", pins)
	}
}

func TestMetricsSandboxUnlinksPinsAfterScanError(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	writeMultiRowGroupHotParquet(t, filepath.Join(hotDir, "current.parquet"), 4)

	conn, cleanup, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare metrics sandbox: %v", err)
	}
	rows, err := conn.QueryContext(ctx, `SELECT not_a_column FROM metrics`)
	if rows != nil {
		_ = rows.Close()
	}
	if err == nil {
		t.Fatal("want a scan error")
	}
	cleanup()
	if pins := listHotPins(t, hotDir); len(pins) != 0 {
		t.Fatalf("leftover pins after error cleanup: %v", pins)
	}
}

func TestMetricsSandboxEmptyHotParquetZeroRows(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	writeEmptyHotParquet(t, filepath.Join(hotDir, "current.parquet"))

	conn, cleanup, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare metrics sandbox: %v", err)
	}
	defer cleanup()

	var n int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM metrics`).Scan(&n); err != nil {
		t.Fatalf("count empty hot snapshot: %v", err)
	}
	if n != 0 {
		t.Fatalf("empty current.parquet rows=%d want 0", n)
	}
}

func TestMetricsSandboxViewReadsPinnedHotDuckDB(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	writeHotDuckDB(t, filepath.Join(hotDir, "current.duckdb"), 3)

	conn, cleanup, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare metrics sandbox: %v", err)
	}
	defer cleanup()

	pins := listHotPins(t, hotDir)
	if len(pins) != 1 {
		t.Fatalf("hot pins = %v, want one unique sibling under hot/", pins)
	}
	path := attachedMetricsPath(t, ctx, conn)
	if strings.Contains(path, "current.duckdb") {
		t.Fatalf("ATTACH must not use current.duckdb: %s", path)
	}
	if !strings.Contains(path, filepath.Base(pins[0])) {
		t.Fatalf("ATTACH must use pin %s, got %s", pins[0], path)
	}

	var n int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM metrics`).Scan(&n); err != nil {
		t.Fatalf("count duckdb hot snapshot: %v", err)
	}
	if n != 3 {
		t.Fatalf("duckdb hot rows=%d want 3", n)
	}

	empty := filepath.Join(t.TempDir(), "empty.duckdb")
	writeHotDuckDB(t, empty, 0)
	if err := os.Rename(empty, filepath.Join(hotDir, "current.duckdb")); err != nil {
		t.Fatalf("replace current.duckdb: %v", err)
	}
	n = 0
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM metrics`).Scan(&n); err != nil {
		t.Fatalf("count after current.duckdb replace: %v", err)
	}
	if n != 3 {
		t.Fatalf("rows after replace=%d want 3 (pinned inode)", n)
	}
}

func TestSQLSandboxUnlinksHotPins(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	writeMultiRowGroupHotParquet(t, filepath.Join(hotDir, "current.parquet"), 4)

	conn, cleanup, err := prepareSandboxConn(ctx, tenantRoot, &metricsOpenOpts{HotOnly: true}, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare sql sandbox: %v", err)
	}
	viewSQL := metricsViewSQL(t, ctx, conn)
	if strings.Contains(viewSQL, "current.parquet") {
		t.Fatalf("sql sandbox metrics view must not read current.parquet: %s", viewSQL)
	}
	if len(listHotPins(t, hotDir)) == 0 {
		t.Fatal("want a hot/.read-* pin while the sql sandbox is open")
	}
	cleanup()
	if pins := listHotPins(t, hotDir); len(pins) != 0 {
		t.Fatalf("leftover pins after sql sandbox cleanup: %v", pins)
	}
}

func TestMetricsSandboxReadOnlyHotDirStillQueries(t *testing.T) {
	ctx := context.Background()
	tenantRoot, hotDir := newHotTenant(t)
	writeMultiRowGroupHotParquet(t, filepath.Join(hotDir, "current.parquet"), 4)
	if err := os.Chmod(hotDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(hotDir, 0o750) })

	conn, cleanup, err := prepareMetricsSandboxConn(ctx, tenantRoot, true, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepare metrics sandbox on read-only hot dir: %v", err)
	}
	defer cleanup()

	if pins := listHotPins(t, hotDir); len(pins) != 0 {
		t.Fatalf("read-only hot dir pins = %v, want none (pin must live off the mount)", pins)
	}
	var n int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM metrics`).Scan(&n); err != nil {
		t.Fatalf("count on read-only hot dir: %v", err)
	}
	if n != 4 {
		t.Fatalf("rows=%d want 4", n)
	}
}

func TestCollectMetricsSourcesIgnoresReadPins(t *testing.T) {
	tenantRoot, hotDir := newHotTenant(t)
	current := filepath.Join(hotDir, "current.parquet")
	writeEmptyHotParquet(t, current)
	pin := filepath.Join(hotDir, ".read-deadbeef.parquet")
	writeEmptyHotParquet(t, pin)

	sources, err := collectMetricsSources(context.Background(), tenantRoot, &metricsOpenOpts{HotOnly: true})
	if err != nil {
		t.Fatalf("collectMetricsSources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("sources=%v want only current.parquet", sources)
	}
	if filepath.Base(sources[0].Path) != "current.parquet" {
		t.Fatalf("source=%s want current.parquet (pins are not extra sources)", sources[0].Path)
	}
}

func newHotTenant(t *testing.T) (tenantRoot, hotDir string) {
	t.Helper()
	tenantRoot = filepath.Join(t.TempDir(), pinTenant)
	hotDir = filepath.Join(tenantRoot, "hot")
	if err := os.MkdirAll(hotDir, 0o750); err != nil {
		t.Fatal(err)
	}
	return tenantRoot, hotDir
}

func listHotPins(t *testing.T, hotDir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(hotDir, hotPinGlob))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func metricsViewSQL(t *testing.T, ctx context.Context, conn *sql.Conn) string {
	t.Helper()
	var s string
	if err := conn.QueryRowContext(ctx,
		`SELECT sql FROM duckdb_views() WHERE view_name = 'metrics'`).Scan(&s); err != nil {
		t.Fatalf("duckdb_views: %v", err)
	}
	return s
}

func attachedMetricsPath(t *testing.T, ctx context.Context, conn *sql.Conn) string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, `SELECT path FROM duckdb_databases() WHERE database_name LIKE 'mseg_%'`)
	if err != nil {
		t.Fatalf("duckdb_databases: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("want an attached metrics duckdb")
	}
	var path string
	if err := rows.Scan(&path); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeMultiRowGroupHotParquet(t *testing.T, path string, n int) {
	t.Helper()
	if n < 1 {
		t.Fatal("need at least one row")
	}
	execDuckDB(t, fmt.Sprintf(`
		COPY (
			SELECT
				'up' AS "__name__",
				'{}' AS labels,
				i::DOUBLE AS value,
				i::BIGINT AS timestamp_ms,
				TIMESTAMP '2024-01-01' + INTERVAL (i) SECOND AS ts
			FROM range(1, %d) AS t(i)
		) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 1)
	`, n+1, slash(path)))
}

func writeEmptyHotParquet(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	execDuckDB(t, fmt.Sprintf(`
		COPY (
			SELECT
				CAST(NULL AS VARCHAR) AS "__name__",
				CAST(NULL AS VARCHAR) AS labels,
				CAST(NULL AS DOUBLE) AS value,
				CAST(NULL AS BIGINT) AS timestamp_ms,
				CAST(NULL AS TIMESTAMP) AS ts
			WHERE false
		) TO '%s' (FORMAT parquet)
	`, slash(path)))
}

func writeHotDuckDB(t *testing.T, path string, n int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf(`
		ATTACH '%s' AS exp (STORAGE_VERSION '%s');
		CREATE TABLE exp.%s AS
			SELECT
				'up' AS "__name__",
				'{}' AS labels,
				i::DOUBLE AS value,
				i::BIGINT AS timestamp_ms,
				TIMESTAMP '2024-01-01' + INTERVAL (i) SECOND AS ts
			FROM range(1, %d) AS t(i);
		CHECKPOINT exp;
		DETACH exp;
	`, slash(path), segformat.DefaultStorageVersion, segformat.MetricsTable, n+1)
	execDuckDB(t, q)
}

func execDuckDB(t *testing.T, q string) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}

func slash(path string) string {
	return filepath.ToSlash(path)
}
