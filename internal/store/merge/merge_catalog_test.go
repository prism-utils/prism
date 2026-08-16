package merge

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/metricsmeta"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestExecuteMergeWritesDestBoundsIntoManifest(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-mergemeta-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
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

	x, err := NewExecutor(ExecutorConfig{DataDir: dataDir, Tenant: tenant, RowGroupSize: 1000})
	if err != nil {
		t.Fatalf("executor: %v", err)
	}
	defer func() { _ = x.Close() }()

	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	out, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, now)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	m, err := metricsmeta.ReadManifest(dataDir, tenant)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	var dest metricsmeta.ManifestFile
	for _, f := range m.Files {
		if strings.Contains(f.Path, "tiers/L1/") {
			dest = f
			break
		}
	}
	if dest.Path == "" {
		t.Fatalf("merged L1 missing from manifest: %+v", m.Files)
	}
	if dest.MinTsNs != out.MinTs.UnixNano() || dest.MaxTsNs != out.MaxTs.UnixNano() {
		t.Fatalf("manifest bounds min=%d max=%d want min=%d max=%d",
			dest.MinTsNs, dest.MaxTsNs, out.MinTs.UnixNano(), out.MaxTs.UnixNano())
	}
}
