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
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestExecuteLogMergeProjectsPerSourceIngestTS(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-logts01-apps"
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}

	t0 := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(5 * time.Minute)
	path0 := filepath.Join(landing, layout.SegmentName(t0))
	path1 := filepath.Join(landing, layout.SegmentName(t1))
	testparquet.WriteLogsRawFile(t, path0, []testparquet.LogRow{{Message: "a", Format: "none"}})
	testparquet.WriteLogsRawFile(t, path1, []testparquet.LogRow{{Message: "b", Format: "none"}})

	s0, err := StatLogSegment(path0, -1)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := StatLogSegment(path1, -1)
	if err != nil {
		t.Fatal(err)
	}

	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	mergeNow := t0.Add(2 * time.Hour)
	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{
		Sources:  []Segment{s0, s1},
		DestTier: 0,
	}, mergeNow)
	if err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}

	// Filename must use min source MinTs, not wall-clock mergeNow.
	if ns, ok := layout.WindowIDNanos(out.Path); !ok || ns != t0.UnixNano() {
		t.Fatalf("merged filename window = %d ok=%v, want min source MinTs %d (not mergeNow %d)",
			ns, ok, t0.UnixNano(), mergeNow.UnixNano())
	}
	if out.MinTs.UnixNano() != t0.UnixNano() {
		t.Fatalf("out.MinTs = %v, want %v", out.MinTs, t0)
	}

	got := readLogIngestTSByMessage(t, out.Path)
	if got["a"] != t0.UnixNano() {
		t.Fatalf("row a __prism_ts_ns = %d, want source0 %d", got["a"], t0.UnixNano())
	}
	if got["b"] != t1.UnixNano() {
		t.Fatalf("row b __prism_ts_ns = %d, want source1 %d", got["b"], t1.UnixNano())
	}
}

func TestExecuteLogMergeKeepsExistingIngestTSOnRemerge(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-logts02-apps"
	artifact := "logs-raw"
	l0 := layout.LogsTierDir(dataDir, tenant, artifact, 0)
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}

	// Two L0 segments that already carry per-row ingest times (as if prior merges
	// wrote them). Remerge must keep those values, not overwrite with file MinTs.
	tsA := time.Date(2026, 4, 2, 1, 0, 0, 0, time.UTC).UnixNano()
	tsB := time.Date(2026, 4, 2, 2, 0, 0, 0, time.UTC).UnixNano()
	fileMin := time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)

	path0 := filepath.Join(l0, layout.SegmentName(fileMin))
	path1 := filepath.Join(l0, layout.SegmentName(fileMin.Add(time.Minute)))
	writeLogParquetWithIngestTS(t, path0, "keep-a", tsA)
	writeLogParquetWithIngestTS(t, path1, "keep-b", tsB)

	s0, err := StatLogSegment(path0, 0)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := StatLogSegment(path1, 0)
	if err != nil {
		t.Fatal(err)
	}

	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	out, err := x.ExecuteLogMerge(artifact, LogMergeAction{
		Sources:  []Segment{s0, s1},
		DestTier: 1,
	}, fileMin.Add(time.Hour))
	if err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}

	got := readLogIngestTSByMessage(t, out.Path)
	if got["keep-a"] != tsA {
		t.Fatalf("keep-a __prism_ts_ns = %d, want preserved %d", got["keep-a"], tsA)
	}
	if got["keep-b"] != tsB {
		t.Fatalf("keep-b __prism_ts_ns = %d, want preserved %d", got["keep-b"], tsB)
	}
}

func writeLogParquetWithIngestTS(t *testing.T, path, message string, tsNs int64) {
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
	q := fmt.Sprintf(
		`COPY (SELECT '%s' AS message, 'raw' AS format, %d::BIGINT AS __prism_ts_ns) TO '%s' (FORMAT parquet)`,
		message, tsNs, filepath.ToSlash(path),
	)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write log parquet with ts: %v", err)
	}
}

func readLogIngestTSByMessage(t *testing.T, path string) map[string]int64 {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	q := fmt.Sprintf(
		`SELECT message, __prism_ts_ns FROM read_parquet('%s')`,
		layout.ToSlash(path),
	)
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatalf("query ingest ts: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := map[string]int64{}
	for rows.Next() {
		var msg string
		var ts int64
		if err := rows.Scan(&msg, &ts); err != nil {
			t.Fatal(err)
		}
		out[msg] = ts
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
