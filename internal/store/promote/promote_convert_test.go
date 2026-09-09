package promote

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
)

func TestTenantPromotesL1DuckDBToColdParquet(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "seg.duckdb")
	writeMetricsDuckDB(t, src, 1.5)
	cfg := agedPromoteCfg(hot, cold, now)
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("hot L1 duckdb must be unlinked after dest verifies")
	}
	duckDest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.duckdb")
	if _, err := os.Stat(duckDest); !os.IsNotExist(err) {
		t.Fatal("cold dest must not keep a .duckdb copy")
	}
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.parquet")
	if err := verifyParquetMagic(dest); err != nil {
		t.Fatalf("cold dest parquet magic: %v", err)
	}
	assertParquetValue(t, dest, 1.5)
	assertNoPromoteTemps(t, hot, cold, tenant)
	if _, err := os.Stat(filepath.Join(layout.TierDir(hot, tenant, 1), "seg.parquet")); !os.IsNotExist(err) {
		t.Fatal("convert must not leave parquet on the hot root")
	}
}

func TestAfterPromoteSeesColdParquet(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "seg.duckdb")
	writeMetricsDuckDB(t, src, 5)
	var called bool
	cfg := agedPromoteCfg(hot, cold, now)
	cfg.AfterPromote = func(string) error {
		called = true
		dest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.parquet")
		if err := verifyParquetMagic(dest); err != nil {
			t.Fatalf("AfterPromote dest parquet: %v", err)
		}
		return nil
	}
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if !called {
		t.Fatal("AfterPromote must run once dest parquet is durable")
	}
}

func TestTenantPromotesL1DuckDBHoldSource(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "seg.duckdb")
	writeMetricsDuckDB(t, src, 2)
	var held string
	cfg := agedPromoteCfg(hot, cold, now)
	cfg.Grace = time.Minute
	cfg.HoldSource = func(path string, until time.Time) error {
		held = path
		if until.Before(now) {
			t.Fatal("hold until must be after now")
		}
		return nil
	}
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if held != src {
		t.Fatalf("HoldSource path=%q, want %q", held, src)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatal("held hot source must remain")
	}
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.parquet")
	if err := verifyParquetMagic(dest); err != nil {
		t.Fatalf("cold dest parquet magic: %v", err)
	}
}

func TestTenantParquetL1ByteCopies(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	body := parquetFixture("keep-bytes")
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "seg.parquet")
	writeFile(t, src, body)
	cfg := agedPromoteCfg(hot, cold, now)
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.parquet")
	got, err := os.ReadFile(dest) //nolint:gosec // test dest
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("parquet L1 must be a byte-copy, not a convert")
	}
}

func TestTenantNeverPromotesL0DuckDB(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	l0 := filepath.Join(layout.TierDir(hot, tenant, 0), "old.duckdb")
	l1 := filepath.Join(layout.TierDir(hot, tenant, 1), "old.duckdb")
	writeMetricsDuckDB(t, l0, 3)
	writeMetricsDuckDB(t, l1, 4)
	cfg := agedPromoteCfg(hot, cold, now)
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if _, err := os.Stat(l0); err != nil {
		t.Fatal("L0 duckdb must stay on hot")
	}
	if _, err := os.Stat(filepath.Join(layout.TierDir(cold, tenant, 0), "old.duckdb")); !os.IsNotExist(err) {
		t.Fatal("L0 duckdb must not land on cold")
	}
	if _, err := os.Stat(filepath.Join(layout.TierDir(cold, tenant, 0), "old.parquet")); !os.IsNotExist(err) {
		t.Fatal("L0 duckdb must not convert onto cold")
	}
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "old.parquet")
	if err := verifyParquetMagic(dest); err != nil {
		t.Fatalf("L1 duckdb should still convert: %v", err)
	}
}

func TestTenantPromotesLogsL1DuckDBToColdParquet(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	src := filepath.Join(layout.LogsTierDir(hot, tenant, "logs-raw", 1), "seg.duckdb")
	writeLogsDuckDB(t, src)
	cfg := agedPromoteCfg(hot, cold, now)
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("hot logs L1 duckdb must be unlinked after dest verifies")
	}
	dest := filepath.Join(layout.LogsTierDir(cold, tenant, "logs-raw", 1), "seg.parquet")
	if err := verifyParquetMagic(dest); err != nil {
		t.Fatalf("cold logs dest parquet magic: %v", err)
	}
	assertParquetRowCount(t, dest, 1)
}

func TestConvertFailureLeavesSourceUnpublishedDest(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "bad.duckdb")
	junk := make([]byte, 64)
	copy(junk, "not a duckdb database")
	writeFile(t, src, junk)
	cfg := agedPromoteCfg(hot, cold, now)
	st, err := Tenant(&cfg, tenant)
	if err == nil {
		t.Fatal("convert of unreadable duckdb must fail")
	}
	if st.Successes != 0 {
		t.Fatalf("successes=%d, want 0", st.Successes)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatal("hot source must remain after convert failure")
	}
	for _, name := range []string{"bad.parquet", "bad.duckdb"} {
		dest := filepath.Join(layout.TierDir(cold, tenant, 1), name)
		if _, err := os.Stat(dest); !os.IsNotExist(err) {
			t.Fatalf("failed convert must not publish %s", name)
		}
	}
	assertNoPromoteTemps(t, hot, cold, tenant)
}

func TestRecoverValidParquetSkipsDuckDBConvert(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "seg.duckdb")
	writeMetricsDuckDB(t, src, 9)
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.parquet")
	writeFile(t, dest, parquetFixture("already-cold"))
	cfg := agedPromoteCfg(hot, cold, now)
	st, err := Tenant(&cfg, tenant)
	if err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if st.Retries != 0 {
		t.Fatalf("valid dest parquet must not count as a retry replace, retries=%d", st.Retries)
	}
	got, err := os.ReadFile(dest) //nolint:gosec // test dest
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, parquetFixture("already-cold")) {
		t.Fatal("valid dest parquet must not be SHA-replaced or reconverted")
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("matching-enough dest still unlinks the hot source")
	}
	duckDest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.duckdb")
	if _, err := os.Stat(duckDest); !os.IsNotExist(err) {
		t.Fatal("duckdb source must not be byte-copied beside the valid parquet dest")
	}
}

func TestLeftoverColdDuckDBIsReplacedWithParquet(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "seg.duckdb")
	writeMetricsDuckDB(t, src, 7)
	leftover := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.duckdb")
	writeFile(t, leftover, []byte("leftover-duckdb-copy"))
	cfg := agedPromoteCfg(hot, cold, now)
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Fatal("leftover cold duckdb must be removed")
	}
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.parquet")
	if err := verifyParquetMagic(dest); err != nil {
		t.Fatalf("converted dest: %v", err)
	}
	assertParquetValue(t, dest, 7)
}

func TestGCRemovesConvertTempsLeavesParquetDest(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	dir := layout.TierDir(cold, tenant, 1)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(dir, "keep.parquet")
	writeFile(t, final, parquetFixture("done"))
	tmp := layout.PromoteTempPath(dir, "keep.parquet", []byte{9, 8, 7, 6})
	if err := os.WriteFile(tmp, []byte("partial-convert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := GCTenant(hot, cold, tenant, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("promote convert temp must be removed")
	}
	if _, err := os.Stat(final); err != nil {
		t.Fatal("completed parquet dest must remain")
	}
}

func agedPromoteCfg(hot, cold string, now time.Time) Config {
	return Config{
		DataDir: hot,
		ColdDir: cold,
		After:   time.Hour,
		MaxTier: 8,
		Now:     func() time.Time { return now },
		MaxTs:   func(string) (time.Time, bool) { return now.Add(-2 * time.Hour), true },
	}
}

func writeMetricsDuckDB(t *testing.T, path string, value float64) {
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
	slash := filepath.ToSlash(path)
	q := fmt.Sprintf(`
		ATTACH '%s' AS exp (STORAGE_VERSION '%s');
		CREATE TABLE exp.%s AS
			SELECT 'up' AS "__name__", '{}' AS labels, %g::DOUBLE AS value,
			       42::BIGINT AS timestamp_ms, TIMESTAMP '2023-11-14 22:13:20' AS ts;
		CHECKPOINT exp;
		DETACH exp;
	`, slash, segformat.DefaultStorageVersion, segformat.MetricsTable, value)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write metrics duckdb: %v", err)
	}
}

func assertParquetValue(t *testing.T, path string, want float64) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	var n int
	var got float64
	q := fmt.Sprintf("SELECT COUNT(*), MAX(value) FROM read_parquet('%s')", filepath.ToSlash(path))
	if err := db.QueryRowContext(context.Background(), q).Scan(&n, &got); err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	if n != 1 {
		t.Fatalf("parquet rows=%d, want 1", n)
	}
	if got != want {
		t.Fatalf("parquet value=%g, want %g", got, want)
	}
}

func writeLogsDuckDB(t *testing.T, path string) {
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
	slash := filepath.ToSlash(path)
	q := fmt.Sprintf(`
		ATTACH '%s' AS exp (STORAGE_VERSION '%s');
		CREATE TABLE exp.%s AS SELECT 'hello' AS message, 'raw' AS format;
		CHECKPOINT exp;
		DETACH exp;
	`, slash, segformat.DefaultStorageVersion, segformat.LogsTable)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write logs duckdb: %v", err)
	}
}

func assertParquetRowCount(t *testing.T, path string, want int) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	var n int
	q := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", filepath.ToSlash(path))
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	if n != want {
		t.Fatalf("parquet rows=%d, want %d", n, want)
	}
}

func assertNoPromoteTemps(t *testing.T, hot, cold, tenant string) {
	t.Helper()
	if n := CountTemps(hot, cold, tenant, 8); n != 0 {
		t.Fatalf("leftover promote temps: %d", n)
	}
}
