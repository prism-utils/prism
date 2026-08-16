package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/metricsmeta"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestFlushWritesL0MinMaxIntoManifest(t *testing.T) {
	start := time.Unix(1_700_000_000, 0).UTC()
	e, now := testEngine(t, start, 10*time.Minute)
	dir := t.TempDir()

	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	*now = start
	if _, err := e.Ingest(testTenant, readFile(t, path)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	*now = start.Add(10 * time.Minute)
	if err := e.FlushDue(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	m, err := metricsmeta.ReadManifest(e.cfg.DataDir, testTenant)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if len(m.Files) == 0 {
		t.Fatal("flush wrote no catalog entries")
	}
	var l0 metricsmeta.ManifestFile
	for _, f := range m.Files {
		if strings.Contains(f.Path, "tiers/L0/") {
			l0 = f
			break
		}
	}
	if l0.Path == "" {
		t.Fatalf("no L0 entry in manifest: %+v", m.Files)
	}
	if l0.MinTsNs == 0 || l0.MaxTsNs == 0 {
		t.Fatalf("L0 bounds unset: %+v", l0)
	}
	if l0.MinTsNs > l0.MaxTsNs {
		t.Fatalf("min > max: %+v", l0)
	}
	abs := filepath.Join(e.cfg.DataDir, testTenant, filepath.FromSlash(l0.Path))
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("catalogued L0 missing on disk: %v", err)
	}
}
