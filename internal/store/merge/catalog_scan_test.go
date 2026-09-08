package merge

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/metrics"
	"github.com/prism-utils/prism/internal/store/metricsmeta"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestScanSecondPassDoesNotStatUnchangedFiles(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-scanhit-apps"
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		p := filepath.Join(dataDir, tenant, "tiers", "L0", pathID(i)+".parquet")
		testparquet.WriteSegmentWithTs(t, p, base.Add(time.Duration(i)*time.Minute), "up", float64(i))
	}

	var stats atomic.Int64
	orig := statSegment
	t.Cleanup(func() { statSegment = orig })
	statSegment = func(path string, tier int, caps DuckDBCaps) (Segment, error) {
		stats.Add(1)
		return orig(path, tier, caps)
	}

	first, err := ScanAllTiersRoots(dataDir, "", tenant, 1, DuckDBCaps{})
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("first scan files=%d want 3", len(first))
	}
	afterFirst := stats.Load()
	if afterFirst != 3 {
		t.Fatalf("first scan stats=%d want 3", afterFirst)
	}

	second, err := ScanAllTiersRoots(dataDir, "", tenant, 1, DuckDBCaps{})
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if len(second) != 3 {
		t.Fatalf("second scan files=%d want 3", len(second))
	}
	if stats.Load() != afterFirst {
		t.Fatalf("second scan restatted unchanged files: stats %d -> %d", afterFirst, stats.Load())
	}
}

func TestScanMissOnNewSizeAndMtimeThenHit(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-scanmiss-apps"
	l0 := filepath.Join(dataDir, tenant, "tiers", "L0")
	p := filepath.Join(l0, "a.parquet")
	testparquet.WriteSegmentWithTs(t, p, time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC), "up", 1)

	var stats atomic.Int64
	orig := statSegment
	t.Cleanup(func() { statSegment = orig })
	statSegment = func(path string, tier int, caps DuckDBCaps) (Segment, error) {
		stats.Add(1)
		return orig(path, tier, caps)
	}

	if _, err := ScanAllTiersRoots(dataDir, "", tenant, 0, DuckDBCaps{}); err != nil {
		t.Fatal(err)
	}
	if stats.Load() != 1 {
		t.Fatalf("new file stats=%d want 1", stats.Load())
	}
	if _, err := ScanAllTiersRoots(dataDir, "", tenant, 0, DuckDBCaps{}); err != nil {
		t.Fatal(err)
	}
	if stats.Load() != 1 {
		t.Fatal("unchanged file must hit")
	}

	m, err := metricsmeta.ReadManifest(dataDir, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 1 {
		t.Fatalf("catalog files=%d", len(m.Files))
	}
	m.Files[0].Bytes--
	if err := metricsmeta.WriteManifest(dataDir, tenant, m); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanAllTiersRoots(dataDir, "", tenant, 0, DuckDBCaps{}); err != nil {
		t.Fatal(err)
	}
	if stats.Load() != 2 {
		t.Fatalf("size change stats=%d want 2", stats.Load())
	}

	later := time.Now().UTC().Add(2 * time.Second)
	if err := os.Chtimes(p, later, later); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanAllTiersRoots(dataDir, "", tenant, 0, DuckDBCaps{}); err != nil {
		t.Fatal(err)
	}
	if stats.Load() != 3 {
		t.Fatalf("mtime change stats=%d want 3", stats.Load())
	}
}

func TestScanDropsVanishedFilesFromCatalog(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-scandrop-apps"
	keep := filepath.Join(dataDir, tenant, "tiers", "L0", "keep.parquet")
	gone := filepath.Join(dataDir, tenant, "tiers", "L0", "gone.parquet")
	ts := time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)
	testparquet.WriteSegmentWithTs(t, keep, ts, "up", 1)
	testparquet.WriteSegmentWithTs(t, gone, ts.Add(time.Minute), "up", 2)

	if _, err := ScanAllTiersRoots(dataDir, "", tenant, 0, DuckDBCaps{}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(gone); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanAllTiersRoots(dataDir, "", tenant, 0, DuckDBCaps{}); err != nil {
		t.Fatal(err)
	}
	m, err := metricsmeta.ReadManifest(dataDir, tenant)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range m.Files {
		if filepath.Base(f.Path) == "gone.parquet" {
			t.Fatalf("vanished file still catalogued: %+v", m.Files)
		}
	}
}

func TestScanIncludesColdL0(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-scancold-apps"
	ts := time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
	p := filepath.Join(cold, tenant, "tiers", "L0", "cold.parquet")
	testparquet.WriteSegmentWithTs(t, p, ts, "up", 1)

	segs, err := ScanAllTiersRoots(hot, cold, tenant, 0, DuckDBCaps{})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(segs) != 1 || segs[0].Tier != 0 {
		t.Fatalf("cold L0 scan = %+v", segs)
	}
}

func TestScanLogTiersHitsCatalogWithoutRestat(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-logscan-apps"
	artifact := "logs-raw"
	name := "1786140844863329878-a.parquet"
	p := filepath.Join(dataDir, tenant, "logs", artifact, "tiers", "L0", name)
	testparquet.WriteLogsRawFile(t, p, []testparquet.LogRow{{Message: "catalog-hit", Format: "none"}})

	var stats atomic.Int64
	orig := statLogSegment
	t.Cleanup(func() { statLogSegment = orig })
	statLogSegment = func(path string, tier int) (Segment, error) {
		stats.Add(1)
		return orig(path, tier)
	}

	first, err := ScanLogTiersRoots(dataDir, "", tenant, artifact, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("first=%d want 1", len(first))
	}
	n := stats.Load()
	if n != 1 {
		t.Fatalf("first stats=%d want 1", n)
	}
	if _, err := ScanLogTiersRoots(dataDir, "", tenant, artifact, 0); err != nil {
		t.Fatal(err)
	}
	if stats.Load() != n {
		t.Fatalf("second log scan restatted: %d -> %d", n, stats.Load())
	}
}

func TestExecuteMergeDropsSourcesFromCatalog(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-mergedrop-apps"
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
		t.Fatal(err)
	}
	defer func() { _ = x.Close() }()
	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	if _, err := x.ExecuteMerge(MergeAction{Sources: sources, DestTier: 1}, now); err != nil {
		t.Fatalf("merge: %v", err)
	}
	m, err := metricsmeta.ReadManifest(dataDir, tenant)
	if err != nil {
		t.Fatal(err)
	}
	for _, src := range sources {
		rel := filepath.ToSlash(filepath.Join("tiers", "L0", filepath.Base(src.Path)))
		for _, f := range m.Files {
			if f.Path == rel {
				t.Fatalf("source %s still in catalog: %+v", rel, m.Files)
			}
		}
	}
}

func TestScanRecordsLookupHitsAndMisses(t *testing.T) {
	reg := metrics.New(metrics.Config{Enabled: true, Path: metrics.DefaultPath, PerTenant: true})
	metrics.Bind(reg)
	t.Cleanup(func() { metrics.Bind(nil) })

	dataDir := t.TempDir()
	tenant := "user-scanmetrics-apps"
	p := filepath.Join(dataDir, tenant, "tiers", "L0", "a.parquet")
	testparquet.WriteSegmentWithTs(t, p, time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC), "up", 1)

	if _, err := ScanAllTiersRoots(dataDir, "", tenant, 0, DuckDBCaps{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ScanAllTiersRoots(dataDir, "", tenant, 0, DuckDBCaps{}); err != nil {
		t.Fatal(err)
	}
	if metrics.LookupTotal(metrics.PlaneMetrics, metrics.CatalogMiss) < 1 {
		t.Fatal("first scan must record a miss")
	}
	if metrics.LookupTotal(metrics.PlaneMetrics, metrics.CatalogHit) < 1 {
		t.Fatal("second scan must record a hit")
	}
}

func TestPlannerStillSkipsSealedAndKeepsUnsealedCold(t *testing.T) {
	p := NewPlanner(PlannerConfig{SegmentsPerTier: 2, MaxSegmentBytes: 100, FloorBytes: 10, MaxMergeAtOnce: 4})
	coldUnsealed := []Segment{
		{Tier: 0, Path: "cold-a", Bytes: 20, MinTs: fixtureBase, MaxTs: fixtureBase.Add(time.Minute)},
		{Tier: 0, Path: "cold-b", Bytes: 20, MinTs: fixtureBase.Add(2 * time.Minute), MaxTs: fixtureBase.Add(3 * time.Minute)},
	}
	if actions := p.FindMerges(coldUnsealed); len(actions) != 1 {
		t.Fatalf("unsealed cold-root files must still pack, got %v", actions)
	}
	sealed := []Segment{
		{Tier: 0, Path: "big-a", Bytes: 100, MinTs: fixtureBase, MaxTs: fixtureBase.Add(time.Minute)},
		{Tier: 0, Path: "big-b", Bytes: 100, MinTs: fixtureBase.Add(2 * time.Minute), MaxTs: fixtureBase.Add(3 * time.Minute)},
	}
	if actions := p.FindMerges(sealed); len(actions) != 0 {
		t.Fatalf("sealed files must stay skipped, got %v", actions)
	}
}
