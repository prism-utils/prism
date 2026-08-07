package merge

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/testparquet"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

const logsMergeTenant = "user-logmerge01-apps"

func TestLogsTierMergeCompactsPastSegmentsPerTier(t *testing.T) {
	dataDir := t.TempDir()
	tenant := logsMergeTenant
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}

	const segmentsPerTier = 6
	const n = segmentsPerTier + 1
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	var totalRows int
	for i := 0; i < n; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		name := layout.SegmentName(at)
		rows := []testparquet.LogRow{{Message: "line", Format: "none"}}
		if i%2 == 0 {
			rows = append(rows, testparquet.LogRow{Message: "extra", Format: "k8s"})
		}
		totalRows += len(rows)
		testparquet.WriteLogsRawFile(t, filepath.Join(landing, name), rows)
	}

	beforeLanding, err := countParquetFiles(landing)
	if err != nil {
		t.Fatal(err)
	}
	if beforeLanding != n {
		t.Fatalf("landing files = %d, want %d", beforeLanding, n)
	}

	planner := NewPlanner(DefaultPlannerConfig())
	landingSegs, err := ScanLogLanding(dataDir, tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	actions := planner.FindLogMerges(landingSegs, nil)
	if len(actions) != 1 {
		t.Fatalf("want 1 merge action, got %d", len(actions))
	}
	action := actions[0]
	action.Artifact = artifact
	// Default planner derives a high MaxMergeAtOnce; tiny landings pack all that fit.
	if len(action.Sources) != n {
		t.Fatalf("merge sources = %d, want all %d landing files under max bytes", len(action.Sources), n)
	}

	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()

	now := base.Add(time.Hour)
	if _, err := x.ExecuteLogMerge(artifact, action, now); err != nil {
		t.Fatalf("ExecuteLogMerge: %v", err)
	}

	afterLanding, err := countParquetFiles(landing)
	if err != nil {
		t.Fatal(err)
	}
	if afterLanding >= beforeLanding {
		t.Fatalf("landing count should drop after merge: before=%d after=%d", beforeLanding, afterLanding)
	}

	l0Dir := layout.LogsTierDir(dataDir, tenant, artifact, 0)
	l0Count, err := countParquetFiles(l0Dir)
	if err != nil {
		t.Fatal(err)
	}
	if l0Count < 1 {
		t.Fatalf("want at least 1 file under %s", l0Dir)
	}

	gotRows, err := countLogRows(dataDir, tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if gotRows != totalRows {
		t.Fatalf("row count = %d, want %d preserved", gotRows, totalRows)
	}
}

func countParquetFiles(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".parquet" && e.Name()[0] != '.' {
			n++
		}
	}
	return n, nil
}

func countLogRows(dataDir, tenant, artifact string) (int, error) {
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	var paths []string
	matches, _ := filepath.Glob(filepath.Join(landing, "*.parquet"))
	paths = append(paths, matches...)
	for tier := 0; tier <= 8; tier++ {
		dir := layout.LogsTierDir(dataDir, tenant, artifact, tier)
		m, _ := filepath.Glob(filepath.Join(dir, "*.parquet"))
		paths = append(paths, m...)
	}
	if len(paths) == 0 {
		return 0, nil
	}

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = "'" + layout.ToSlash(p) + "'"
	}
	var n int
	q := "SELECT COUNT(*) FROM read_parquet([" + joinQuoted(quoted) + "], union_by_name=true)"
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func joinQuoted(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += ", " + p
	}
	return out
}
