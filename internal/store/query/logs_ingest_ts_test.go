package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/testparquet"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

func TestBuildLogsRelationSQLPrefersIngestTSColumn(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, "user-lokits01-apps")
	dir := filepath.Join(tenantRoot, "logs", "logs-raw", "tiers", "L0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	// File named with wall-clock "merge time" but rows carry earlier ingest ns.
	mergeName := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	rowTS := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC).UnixNano()
	path := filepath.Join(dir, layout.SegmentName(mergeName))
	writeLogsParquetWithIngestTS(t, path, "col-row", rowTS)

	sqlText, files, err := sandboxLokiLogsSQL(tenantRoot, 0, 0, false, 0)
	if err != nil {
		t.Fatalf("sandboxLokiLogsSQL: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if !files[0].HasIngestTS {
		t.Fatal("expected HasIngestTS=true for file carrying __prism_ts_ns")
	}
	// Column path must not invent time solely from the filename JOIN map.
	if strings.Contains(sqlText, "AS v(path, ts)") {
		t.Fatalf("expected column path (no filename VALUES JOIN), got: %s", truncate(sqlText, 400))
	}

	got := evalLokiIngestTS(t, sqlText)
	if got["col-row"] != rowTS {
		t.Fatalf("ingest ts = %d, want column value %d (not filename %d)",
			got["col-row"], rowTS, mergeName.UnixNano())
	}
}

func TestBuildLogsRelationSQLLegacyFilenameJOIN(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, "user-lokits02-apps")
	dir := filepath.Join(tenantRoot, "logs", "logs-raw")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}

	landingAt := time.Date(2026, 5, 2, 9, 0, 0, 0, time.UTC)
	path := filepath.Join(dir, layout.SegmentName(landingAt))
	testparquet.WriteLogsRawFile(t, path, []testparquet.LogRow{{Message: "legacy", Format: "none"}})

	sqlText, files, err := sandboxLokiLogsSQL(tenantRoot, 0, 0, false, 0)
	if err != nil {
		t.Fatalf("sandboxLokiLogsSQL: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if files[0].HasIngestTS {
		t.Fatal("expected HasIngestTS=false for legacy landing file")
	}
	if !strings.Contains(sqlText, "AS v(path, ts)") {
		t.Fatalf("expected legacy filename VALUES JOIN, got: %s", truncate(sqlText, 400))
	}

	got := evalLokiIngestTS(t, sqlText)
	if got["legacy"] != landingAt.UnixNano() {
		t.Fatalf("ingest ts = %d, want filename window %d", got["legacy"], landingAt.UnixNano())
	}
}

func TestBuildLogsRelationSQLMixedColumnAndLegacy(t *testing.T) {
	InvalidateLogsMetaCache("")
	root := t.TempDir()
	tenantRoot := filepath.Join(root, "user-lokits03-apps")
	landing := filepath.Join(tenantRoot, "logs", "logs-raw")
	tier := filepath.Join(landing, "tiers", "L0")
	if err := os.MkdirAll(tier, 0o750); err != nil {
		t.Fatal(err)
	}

	legacyAt := time.Date(2026, 5, 3, 1, 0, 0, 0, time.UTC)
	legacyPath := filepath.Join(landing, layout.SegmentName(legacyAt))
	testparquet.WriteLogsRawFile(t, legacyPath, []testparquet.LogRow{{Message: "legacy", Format: "none"}})

	colRowTS := time.Date(2026, 5, 3, 0, 30, 0, 0, time.UTC).UnixNano()
	colNameAt := time.Date(2026, 5, 3, 2, 0, 0, 0, time.UTC)
	colPath := filepath.Join(tier, layout.SegmentName(colNameAt))
	writeLogsParquetWithIngestTS(t, colPath, "with-col", colRowTS)

	sqlText, _, err := sandboxLokiLogsSQL(tenantRoot, 0, 0, false, 0)
	if err != nil {
		t.Fatalf("sandboxLokiLogsSQL: %v", err)
	}
	got := evalLokiIngestTS(t, sqlText)
	if got["legacy"] != legacyAt.UnixNano() {
		t.Fatalf("legacy ts = %d, want %d", got["legacy"], legacyAt.UnixNano())
	}
	if got["with-col"] != colRowTS {
		t.Fatalf("with-col ts = %d, want column %d", got["with-col"], colRowTS)
	}
}

func writeLogsParquetWithIngestTS(t *testing.T, path, message string, tsNs int64) {
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
		`COPY (SELECT '%s' AS message, 'raw' AS format, %d::BIGINT AS %s) TO '%s' (FORMAT parquet)`,
		message, tsNs, lokiTSColumn, filepath.ToSlash(path),
	)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write logs parquet with ts: %v", err)
	}
}

func evalLokiIngestTS(t *testing.T, viewSQL string) map[string]int64 {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	q := fmt.Sprintf(`SELECT message, %s FROM (%s)`, lokiTSColumn, viewSQL)
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatalf("eval view: %v\nsql=%s", err, truncate(viewSQL, 500))
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
