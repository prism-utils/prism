package metricsmeta

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestWriteReadManifestRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-catalog-apps"
	m := Manifest{
		Version: 4,
		Files: []ManifestFile{
			{Path: "tiers/L0/111-aaaa.parquet", MinTsNs: 100, MaxTsNs: 200, Bytes: 12},
			{Path: "hot/current.parquet", MinTsNs: 180, MaxTsNs: 199, Bytes: 8},
		},
	}
	if err := WriteManifest(dir, tenant, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	got, err := ReadManifest(dir, tenant)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.Version != 4 || len(got.Files) != 2 {
		t.Fatalf("got %+v", got)
	}
	if got.Files[0].Path != "hot/current.parquet" {
		t.Fatalf("files should be sorted by path, first=%s", got.Files[0].Path)
	}
	if got.Files[1].MinTsNs != 100 || got.Files[1].MaxTsNs != 200 {
		t.Fatalf("L0 bounds = %+v", got.Files[1])
	}
}

func TestRebuildManifestRecordsParquetMinMax(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-rebuild-apps"
	ts0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	ts1 := ts0.Add(time.Hour)
	l0 := filepath.Join(dir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0, "a.parquet"), ts0, "up", 1)
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0, "b.parquet"), ts1, "up", 1)

	m, err := RebuildManifest(dir, tenant, 7)
	if err != nil {
		t.Fatalf("RebuildManifest: %v", err)
	}
	if m.Version != 7 {
		t.Fatalf("version=%d want 7", m.Version)
	}
	if len(m.Files) != 2 {
		t.Fatalf("files=%d want 2: %+v", len(m.Files), m.Files)
	}
	byPath := map[string]ManifestFile{}
	for _, f := range m.Files {
		byPath[f.Path] = f
	}
	a := byPath["tiers/L0/a.parquet"]
	b := byPath["tiers/L0/b.parquet"]
	if a.MinTsNs != ts0.UnixNano() || a.MaxTsNs != ts0.UnixNano() {
		t.Fatalf("a bounds min=%d max=%d want %d", a.MinTsNs, a.MaxTsNs, ts0.UnixNano())
	}
	if b.MinTsNs != ts1.UnixNano() || b.MaxTsNs != ts1.UnixNano() {
		t.Fatalf("b bounds min=%d max=%d want %d", b.MinTsNs, b.MaxTsNs, ts1.UnixNano())
	}
}

func TestFileBoundsEmptyParquetIsKnownEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.parquet")
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	q := `COPY (
		SELECT
			CAST(NULL AS VARCHAR) AS "__name__",
			CAST(NULL AS VARCHAR) AS labels,
			CAST(NULL AS DOUBLE) AS value,
			CAST(NULL AS BIGINT) AS timestamp_ms,
			CAST(NULL AS TIMESTAMP) AS ts
		WHERE false
	) TO '` + filepath.ToSlash(path) + `' (FORMAT parquet)`
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("write empty parquet: %v", err)
	}
	minNs, maxNs, ok := FileBounds(path)
	if !ok {
		t.Fatal("empty parquet should be known-empty, not skipped")
	}
	if minNs != 0 || maxNs != 0 {
		t.Fatalf("empty bounds min=%d max=%d want 0,0", minNs, maxNs)
	}
}

func TestRebuildManifestSkipsCompactedAndUnknownBounds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-skip-apps"
	l0 := filepath.Join(dir, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(l0, "1786140844863329878-live.parquet")
	testparquet.WriteSegmentWithTs(t, live, time.Unix(1_700_000_000, 0).UTC(), "up", 1)
	garbage := filepath.Join(l0, "not-parquet.parquet")
	if err := os.WriteFile(garbage, []byte("nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	compacted := filepath.Join(l0, "1786140844863329879-gone.parquet")
	testparquet.WriteSegmentWithTs(t, compacted, time.Unix(1_700_000_000, 0).UTC(), "up", 1)
	if err := os.WriteFile(compacted+".compacted", []byte("999\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	m, err := RebuildManifest(dir, tenant, 1)
	if err != nil {
		t.Fatalf("RebuildManifest: %v", err)
	}
	if len(m.Files) != 1 || m.Files[0].Path != "tiers/L0/1786140844863329878-live.parquet" {
		t.Fatalf("manifest files = %+v, want only live parquet with stats", m.Files)
	}
}
