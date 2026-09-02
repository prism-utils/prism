package materialize

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestRunWritesParquetWithExpectedColumn(t *testing.T) {
	dataDir, tenant, dest := mergeFixture(t)
	cfg := RunConfig{
		DataDir:  dataDir,
		Tenant:   tenant,
		DestPath: dest,
		DestTier: 0,
		RunJobs:  true,
		Now:      time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		Items:    []Item{{Name: "last_events", SQL: "SELECT 1 AS x FROM merge_output", On: "metrics"}},
		Logger:   slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	if err := Run(context.Background(), &cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	dir := layout.MaterializationDir(dataDir, tenant, "last_events")
	files, err := LiveFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("live files = %d, want 1", len(files))
	}
	if filepath.Base(files[0]) != filepath.Base(dest) {
		t.Fatalf("basename %q want dest %q", filepath.Base(files[0]), filepath.Base(dest))
	}
	cols := parquetColumns(t, files[0])
	if len(cols) != 1 || cols[0] != "x" {
		t.Fatalf("columns = %v, want [x]", cols)
	}
}

func TestRunEmptyItemsCreatesNoMaterializationsDir(t *testing.T) {
	dataDir, tenant, dest := mergeFixture(t)
	before, err := os.ReadDir(filepath.Join(dataDir, tenant))
	if err != nil {
		t.Fatal(err)
	}
	cfg := RunConfig{
		DataDir:  dataDir,
		Tenant:   tenant,
		DestPath: dest,
		RunJobs:  true,
		Now:      time.Now().UTC(),
	}
	if err := Run(context.Background(), &cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	after, err := os.ReadDir(filepath.Join(dataDir, tenant))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("tenant entries before=%d after=%d (empty config must be byte-identical)", len(before), len(after))
	}
	if _, err := os.Stat(filepath.Join(dataDir, tenant, "materializations")); !os.IsNotExist(err) {
		t.Fatalf("materializations/ exists, want absent: %v", err)
	}
}

func TestRunRuntimeSQLErrorDoesNotFailMerge(t *testing.T) {
	dataDir, tenant, dest := mergeFixture(t)
	var buf bytes.Buffer
	cfg := RunConfig{
		DataDir:  dataDir,
		Tenant:   tenant,
		DestPath: dest,
		DestTier: 0,
		RunJobs:  true,
		Now:      time.Now().UTC(),
		Items:    []Item{{Name: "broken", SQL: "SELECT nosuchcolumn FROM merge_output"}},
		Logger:   slog.New(slog.NewTextHandler(&buf, nil)),
	}
	if err := Run(context.Background(), &cfg); err != nil {
		t.Fatalf("Run must not fail merge: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("dest missing: %v", err)
	}
	if !strings.Contains(buf.String(), "broken") {
		t.Fatalf("log %q should mention skipped name", buf.String())
	}
	files, err := LiveFiles(layout.MaterializationDir(dataDir, tenant, "broken"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("want no live files after runtime SQL error, got %v", files)
	}
}

func TestRunMinTierSkipsL0WhenOne(t *testing.T) {
	dataDir, tenant, dest := mergeFixture(t)
	item := Item{Name: "gated", SQL: "SELECT 1 AS x FROM merge_output", MinTier: 1}
	cfg := RunConfig{
		DataDir:  dataDir,
		Tenant:   tenant,
		DestPath: dest,
		DestTier: 0,
		RunJobs:  true,
		Now:      time.Now().UTC(),
		Items:    []Item{item},
	}
	if err := Run(context.Background(), &cfg); err != nil {
		t.Fatal(err)
	}
	files, err := LiveFiles(layout.MaterializationDir(dataDir, tenant, "gated"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("minTier=1 must skip L0, got %v", files)
	}
	cfg.DestTier = 1
	if err := Run(context.Background(), &cfg); err != nil {
		t.Fatal(err)
	}
	files, err = LiveFiles(layout.MaterializationDir(dataDir, tenant, "gated"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("minTier=1 must run on L1, got %d files", len(files))
	}
}

func TestRunJobsFalseWritesNothing(t *testing.T) {
	dataDir, tenant, dest := mergeFixture(t)
	cfg := RunConfig{
		DataDir:  dataDir,
		Tenant:   tenant,
		DestPath: dest,
		DestTier: 0,
		RunJobs:  false,
		Now:      time.Now().UTC(),
		Items:    []Item{{Name: "last_events", SQL: "SELECT 1 AS x FROM merge_output"}},
	}
	if err := Run(context.Background(), &cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, tenant, "materializations")); !os.IsNotExist(err) {
		t.Fatalf("RUN_JOBS=false must not write materializations: %v", err)
	}
}

func TestRunCompactsSourceBasename(t *testing.T) {
	dataDir, tenant, dest1 := mergeFixture(t)
	item := Item{Name: "last_events", SQL: "SELECT 1 AS x FROM merge_output"}
	now := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	if err := Run(context.Background(), &RunConfig{
		DataDir: dataDir, Tenant: tenant, DestPath: dest1, DestTier: 0, RunJobs: true, Now: now, Items: []Item{item},
	}); err != nil {
		t.Fatal(err)
	}
	l1 := filepath.Join(dataDir, tenant, "tiers", "L1")
	dest2 := filepath.Join(l1, "222-bbbbbbbb.parquet")
	testparquet.WriteSegmentWithTs(t, dest2, now, "up", 2)
	if err := Run(context.Background(), &RunConfig{
		DataDir:     dataDir,
		Tenant:      tenant,
		DestPath:    dest2,
		SourcePaths: []string{dest1},
		DestTier:    1,
		RunJobs:     true,
		Now:         now.Add(time.Hour),
		Items:       []Item{item},
	}); err != nil {
		t.Fatal(err)
	}
	dir := layout.MaterializationDir(dataDir, tenant, "last_events")
	old := filepath.Join(dir, filepath.Base(dest1))
	if _, err := os.Stat(layout.CompactedMarker(old)); err != nil {
		t.Fatalf("source mat must be compacted: %v", err)
	}
	files, err := LiveFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("live files = %d, want 1 (no double-count)", len(files))
	}
	if filepath.Base(files[0]) != filepath.Base(dest2) {
		t.Fatalf("live basename %q want %q", filepath.Base(files[0]), filepath.Base(dest2))
	}
}

func TestRunSkipsLogsItemOnMetricsPlane(t *testing.T) {
	dataDir, tenant, dest := mergeFixture(t)
	if err := Run(context.Background(), &RunConfig{
		DataDir:  dataDir,
		Tenant:   tenant,
		DestPath: dest,
		DestTier: 0,
		Plane:    PlaneMetrics,
		RunJobs:  true,
		Now:      time.Now().UTC(),
		Items:    []Item{{Name: "logonly", SQL: "SELECT 1 AS x FROM merge_output", On: "logs"}},
	}); err != nil {
		t.Fatal(err)
	}
	files, err := LiveFiles(layout.MaterializationDir(dataDir, tenant, "logonly"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("logs-only item must skip metrics merge, got %v", files)
	}
}

func TestRunDuckDBLogsDestBindsMergeOutput(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-mat00001-apps"
	dest := filepath.Join(dataDir, tenant, "logs", "logs-raw", "tiers", "L1", "111-aaaaaaaa.duckdb")
	writeLogsDuckDB(t, dest)
	cfg := RunConfig{
		DataDir:  dataDir,
		Tenant:   tenant,
		DestPath: dest,
		DestTier: 1,
		Plane:    PlaneLogs,
		RunJobs:  true,
		Now:      time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		Items:    []Item{{Name: "logonly", SQL: "SELECT 1 AS x FROM merge_output", On: "logs"}},
		Logger:   slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	if err := Run(context.Background(), &cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	files, err := LiveFiles(layout.MaterializationDir(dataDir, tenant, "logonly"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("live files = %d, want 1", len(files))
	}
	if !strings.HasSuffix(files[0], ".parquet") {
		t.Fatalf("mat file %q want .parquet", files[0])
	}
	cols := parquetColumns(t, files[0])
	if len(cols) != 1 || cols[0] != "x" {
		t.Fatalf("columns = %v, want [x]", cols)
	}
}

func TestRunDuckDBSourceIncludedInMergeInput(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-mat00001-apps"
	dest := filepath.Join(dataDir, tenant, "logs", "logs-raw", "tiers", "L1", "222-bbbbbbbb.duckdb")
	src := filepath.Join(dataDir, tenant, "logs", "logs-raw", "tiers", "L0", "111-aaaaaaaa.duckdb")
	writeLogsDuckDB(t, dest)
	writeLogsDuckDB(t, src)
	cfg := RunConfig{
		DataDir:     dataDir,
		Tenant:      tenant,
		DestPath:    dest,
		SourcePaths: []string{src},
		DestTier:    1,
		Plane:       PlaneLogs,
		RunJobs:     true,
		Now:         time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC),
		Items:       []Item{{Name: "frominput", SQL: "SELECT COUNT(*)::INTEGER AS x FROM merge_input", On: "logs"}},
	}
	if err := Run(context.Background(), &cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	files, err := LiveFiles(layout.MaterializationDir(dataDir, tenant, "frominput"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("live files = %d, want 1", len(files))
	}
	n := parquetInt(t, files[0], "x")
	if n != 1 {
		t.Fatalf("merge_input count = %d, want 1 (duckdb source must not be skipped)", n)
	}
}

func TestRunUnusableDestDoesNotReadParquet(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-mat00001-apps"
	dest := filepath.Join(dataDir, tenant, "logs", "logs-raw", "tiers", "L0", "333-cccccccc.duckdb")
	if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, make([]byte, 64), 0o600); err != nil {
		t.Fatal(err)
	}
	err := Run(context.Background(), &RunConfig{
		DataDir:  dataDir,
		Tenant:   tenant,
		DestPath: dest,
		DestTier: 0,
		Plane:    PlaneLogs,
		RunJobs:  true,
		Now:      time.Now().UTC(),
		Items:    []Item{{Name: "logonly", SQL: "SELECT 1 AS x FROM merge_output", On: "logs"}},
	})
	if err == nil {
		t.Fatal("want bind error for unusable dest")
	}
	if strings.Contains(err.Error(), "read_parquet") {
		t.Fatalf("unusable dest must not call read_parquet: %v", err)
	}
}

func TestViewSQLOmitsTierPaths(t *testing.T) {
	t.Parallel()
	sql := ViewSQL([]string{"/data/t/materializations/foo/1.parquet"})
	if strings.Contains(sql, "tiers/") || strings.Contains(sql, "/hot/") {
		t.Fatalf("view SQL must not mention raw tiers/hot: %s", sql)
	}
	if !strings.Contains(sql, "materializations/foo") {
		t.Fatalf("view SQL should read mat path: %s", sql)
	}
	empty := ViewSQL(nil)
	if strings.Contains(empty, "read_parquet") {
		t.Fatalf("empty view must not open parquet: %s", empty)
	}
}

func mergeFixture(t *testing.T) (dataDir, tenant, dest string) {
	t.Helper()
	dataDir = t.TempDir()
	tenant = "user-mat00001-apps"
	dest = filepath.Join(dataDir, tenant, "tiers", "L0", "111-aaaaaaaa.parquet")
	testparquet.WriteSegmentWithTs(t, dest, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "up", 1)
	return dataDir, tenant, dest
}

func parquetColumns(t *testing.T, path string) []string {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(), "SELECT * FROM read_parquet('"+filepath.ToSlash(path)+"') LIMIT 0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	return cols
}

func parquetInt(t *testing.T, path, col string) int {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	var n int
	q := "SELECT " + col + " FROM read_parquet('" + filepath.ToSlash(path) + "')"
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
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
