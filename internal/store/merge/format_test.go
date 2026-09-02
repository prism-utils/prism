package merge

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestExecuteMergeEmitsDuckDB(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 3; i++ {
		path := filepath.Join(l0, pathID(i)+".parquet")
		ts := base.Add(time.Duration(i) * time.Minute)
		testparquet.WriteSegmentWithTs(t, path, ts, "up", float64(i))
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{
		DataDir:              dataDir,
		Tenant:               tenant,
		RowGroupSize:         1000,
		SegmentFormat:        segformat.DuckDB,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer func() { _ = x.Close() }()

	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, now)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if filepath.Ext(out.Path) != ".duckdb" {
		t.Fatalf("merge out ext=%q, want .duckdb", filepath.Ext(out.Path))
	}
	if _, err := os.Stat(out.Path + ".wal"); !os.IsNotExist(err) {
		t.Fatalf("unexpected merge wal: %v", err)
	}
	all, err := ScanAllTiers(dataDir, tenant, 1, DuckDBCaps{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range all {
		if s.Path == out.Path {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("ScanAllTiers missed duckdb merge output")
	}
}

func TestExecuteLogMergeEmitsDuckDB(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	artifact := "logs-raw"
	landing := filepath.Join(dataDir, tenant, "logs", artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 3; i++ {
		path := filepath.Join(landing, layout.SegmentName(base.Add(time.Duration(i)*time.Second)))
		writeTinyLogParquet(t, path, fmt.Sprintf("line-%d", i))
		seg, err := StatLogSegment(path, -1)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{
		DataDir:              dataDir,
		Tenant:               tenant,
		RowGroupSize:         1000,
		SegmentFormat:        segformat.DuckDB,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{Sources: sources, DestTier: 0}, base)
	if err != nil {
		t.Fatalf("log merge: %v", err)
	}
	if filepath.Ext(out.Path) != ".duckdb" {
		t.Fatalf("log merge ext=%q, want .duckdb", filepath.Ext(out.Path))
	}
}

func TestExecuteLogMergeDuckDBSourcesWriteParquetDest(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	artifact := "logs-raw"
	landing := filepath.Join(dataDir, tenant, "logs", artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 2; i++ {
		path := filepath.Join(landing, layout.SegmentNameFormat(base.Add(time.Duration(i)*time.Second), "duckdb"))
		writeLandingLogsDuckDB(t, path, fmt.Sprintf("line-%d", i))
		seg, err := StatLogSegment(path, -1)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{
		DataDir:              dataDir,
		Tenant:               tenant,
		RowGroupSize:         1000,
		SegmentFormat:        segformat.Parquet,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{Sources: sources, DestTier: 0}, base)
	if err != nil {
		t.Fatalf("log merge: %v", err)
	}
	if filepath.Ext(out.Path) != ".duckdb" {
		t.Fatalf("log merge ext=%q, want .duckdb (dest follows source payload)", filepath.Ext(out.Path))
	}
	if !segformat.IsDuckDB(out.Path) {
		t.Fatal("duckdb sources must write a duckdb dest")
	}
}

func TestExecuteLogMergeMixedSourcesError(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	artifact := "logs-raw"
	landing := filepath.Join(dataDir, tenant, "logs", artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	parq := filepath.Join(landing, layout.SegmentName(base))
	duck := filepath.Join(landing, layout.SegmentNameFormat(base.Add(time.Second), "duckdb"))
	writeTinyLogParquet(t, parq, "line-p")
	writeLandingLogsDuckDB(t, duck, "line-d")
	s0, err := StatLogSegment(parq, -1)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := StatLogSegment(duck, -1)
	if err != nil {
		t.Fatal(err)
	}
	x, err := NewExecutor(ExecutorConfig{
		DataDir:              dataDir,
		Tenant:               tenant,
		RowGroupSize:         1000,
		SegmentFormat:        segformat.Parquet,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	if _, err := x.ExecuteLogMerge(artifact, LogMergeAction{Sources: []Segment{s0, s1}, DestTier: 0}, base); err == nil {
		t.Fatal("mixed parquet+duckdb sources must error")
	}
}

func TestExecuteMergeDuckDBSourcesWriteParquetDest(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 2; i++ {
		path := filepath.Join(l0, pathID(i)+".duckdb")
		ts := base.Add(time.Duration(i) * time.Minute)
		writeMetricsDuckDB(t, path, float64(i), ts)
		seg, err := StatSegment(path, 0, DuckDBCaps{})
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{
		DataDir:              dataDir,
		Tenant:               tenant,
		RowGroupSize:         1000,
		SegmentFormat:        segformat.Parquet,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer func() { _ = x.Close() }()

	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, now)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if filepath.Ext(out.Path) != ".duckdb" {
		t.Fatalf("merge out ext=%q, want .duckdb (dest follows source payload)", filepath.Ext(out.Path))
	}
}

func TestExecuteLogMergeParquetSourcesSkipCopy(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-test00001-apps"
	artifact := "logs-raw"
	landing := filepath.Join(dataDir, tenant, "logs", artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sources []Segment
	for i := 0; i < 2; i++ {
		path := filepath.Join(landing, layout.SegmentName(base.Add(time.Duration(i)*time.Second)))
		writeTinyLogParquet(t, path, fmt.Sprintf("line-%d", i))
		seg, err := StatLogSegment(path, -1)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, seg)
	}

	x, err := NewExecutor(ExecutorConfig{
		DataDir:              dataDir,
		Tenant:               tenant,
		RowGroupSize:         1000,
		SegmentFormat:        segformat.Parquet,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{Sources: sources, DestTier: 0}, base)
	if err != nil {
		t.Fatalf("log merge: %v", err)
	}
	if filepath.Ext(out.Path) != ".parquet" {
		t.Fatalf("log merge ext=%q, want .parquet", filepath.Ext(out.Path))
	}
	assertFileParquetMagic(t, out.Path)
	if x.CopyCount != 0 {
		t.Fatalf("all-parquet sources should k-way, CopyCount=%d", x.CopyCount)
	}
}

func writeTinyLogParquet(t *testing.T, path string, message string) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf(
		`COPY (SELECT '%s' AS message, 'raw' AS format) TO '%s' (FORMAT parquet)`,
		message, filepath.ToSlash(path),
	)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write log parquet: %v", err)
	}
}

func writeLandingLogsDuckDB(t *testing.T, path, message string) {
	t.Helper()
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
		CREATE TABLE exp.%s AS SELECT '%s' AS message, 'raw' AS format;
		CHECKPOINT exp;
		DETACH exp;
	`, slash, segformat.DefaultStorageVersion, segformat.AgentLogsTable, message)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write landing duckdb: %v", err)
	}
}

func writeMetricsDuckDB(t *testing.T, path string, value float64, ts time.Time) {
	t.Helper()
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
			       %d::BIGINT AS timestamp_ms, TIMESTAMP '%s' AS ts;
		CHECKPOINT exp;
		DETACH exp;
	`, slash, segformat.DefaultStorageVersion, segformat.MetricsTable, value,
		ts.UnixMilli(), ts.UTC().Format("2006-01-02 15:04:05"))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write metrics duckdb: %v", err)
	}
}

func assertFileParquetMagic(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // G304: test-owned segment path
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	head := make([]byte, 4)
	if _, err := f.Read(head); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if string(head) != "PAR1" {
		t.Fatalf("header magic=%q, want PAR1 (duckdb bytes at a parquet path)", string(head))
	}
	if _, err := f.Seek(-4, io.SeekEnd); err != nil {
		t.Fatalf("seek footer: %v", err)
	}
	tail := make([]byte, 4)
	if _, err := f.Read(tail); err != nil {
		t.Fatalf("read footer: %v", err)
	}
	if string(tail) != "PAR1" {
		t.Fatalf("footer magic=%q, want PAR1", string(tail))
	}
}
