package merge

import (
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
