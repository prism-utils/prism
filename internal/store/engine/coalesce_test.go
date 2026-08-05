package engine

import (
	"bytes"
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

func TestLogsIngestCoalesceRespectsMaxAgeOrMaxBytes(t *testing.T) {
	t.Run("max_age", func(t *testing.T) {
		dataDir := t.TempDir()
		tenant := "user-coalesce01-apps"
		now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
		clock := func() time.Time { return now }
		eng := New(Config{
			DataDir:           dataDir,
			LogCoalesceMaxAge: time.Minute,
		}, clock)
		t.Cleanup(func() { _ = eng.Close() })

		var totalRows int
		for i := 0; i < 5; i++ {
			n, err := landRawChunk(t, eng, tenant, "logs-raw", i)
			if err != nil {
				t.Fatal(err)
			}
			totalRows += n
		}
		landing := layout.LogsLandingDir(dataDir, tenant, "logs-raw")
		if c, _ := countLanding(landing); c != 0 {
			t.Fatalf("before age seal landing files = %d, want 0 buffered", c)
		}

		now = now.Add(2 * time.Minute)
		if err := eng.FlushLogCoalesce(); err != nil {
			t.Fatal(err)
		}
		if c, _ := countLanding(landing); c != 1 {
			t.Fatalf("after age seal landing files = %d, want 1", c)
		}
		if got := countRows(t, landing); got != totalRows {
			t.Fatalf("row count = %d, want %d", got, totalRows)
		}
	})

	t.Run("max_bytes", func(t *testing.T) {
		dataDir := t.TempDir()
		tenant := "user-coalesce02-apps"
		eng := New(Config{
			DataDir:             dataDir,
			LogCoalesceMaxBytes: 500,
		}, time.Now)
		t.Cleanup(func() { _ = eng.Close() })

		var totalRows int
		for i := 0; i < 8; i++ {
			n, err := landRawChunk(t, eng, tenant, "logs-raw", i)
			if err != nil {
				t.Fatal(err)
			}
			totalRows += n
		}
		landing := layout.LogsLandingDir(dataDir, tenant, "logs-raw")
		if c, _ := countLanding(landing); c == 0 {
			t.Fatal("max_bytes seal should have produced at least one landing file")
		}
		if got := countRows(t, landing); got != totalRows {
			t.Fatalf("row count = %d, want %d preserved", got, totalRows)
		}
	})
}

func landRawChunk(t *testing.T, eng *Engine, tenant, artifact string, seq int) (int, error) {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "chunk.parquet")
	rows := []testparquet.LogRow{{Message: "line", Format: "none"}}
	if seq%2 == 0 {
		rows = append(rows, testparquet.LogRow{Message: "extra", Format: "k8s"})
	}
	testparquet.WriteLogsRawFile(t, tmp, rows)
	b, err := os.ReadFile(tmp)
	if err != nil {
		return 0, err
	}
	if _, err := eng.LandLogWindow(tenant, artifact, bytes.NewReader(b)); err != nil {
		return 0, err
	}
	return len(rows), nil
}

func countLanding(dir string) (int, error) {
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

func countRows(t *testing.T, landing string) int {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(landing, "*.parquet"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("glob landing: %v", err)
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	quoted := make([]string, len(matches))
	for i, p := range matches {
		quoted[i] = "'" + layout.ToSlash(p) + "'"
	}
	var n int
	q := "SELECT COUNT(*) FROM read_parquet([" + joinQuoted(quoted) + "], union_by_name=true)"
	if err := db.QueryRowContext(context.Background(), q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func joinQuoted(parts []string) string {
	out := parts[0]
	for _, p := range parts[1:] {
		out += ", " + p
	}
	return out
}
