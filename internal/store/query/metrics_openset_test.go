package query

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/metricsmeta"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const openSetTenant = "user-openset-apps"

func writeOpenSetFile(t *testing.T, path string, ts time.Time, metric string) {
	t.Helper()
	testparquet.WriteSegmentWithTs(t, path, ts, metric, 1)
}

func openSetRoot(t *testing.T, dataDir string) string {
	t.Helper()
	root := filepath.Join(dataDir, openSetTenant)
	if err := os.MkdirAll(filepath.Join(root, "tiers", "L0"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "hot"), 0o750); err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	return filepath.Clean(abs)
}

func sourcePaths(sources []metricsSource) []string {
	out := make([]string, len(sources))
	for i, s := range sources {
		out[i] = filepath.ToSlash(s.Path)
	}
	return out
}

func hasPathSuffix(paths []string, suffix string) bool {
	for _, p := range paths {
		if strings.HasSuffix(p, suffix) {
			return true
		}
	}
	return false
}

func TestMetricsOpenSetPrunesNonOverlappingFiles(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	root := openSetRoot(t, dataDir)
	l0 := filepath.Join(root, "tiers", "L0")
	tA := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tB := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	tC := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	writeOpenSetFile(t, filepath.Join(l0, "a.parquet"), tA, "up")
	writeOpenSetFile(t, filepath.Join(l0, "b.parquet"), tB, "up")
	writeOpenSetFile(t, filepath.Join(l0, "c.parquet"), tC, "up")
	if err := metricsmeta.SyncManifest(dataDir, openSetTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}

	start, end := tB, tB.Add(time.Hour)
	sources, err := collectMetricsSources(root, metricsOpenOpts{Start: start, End: end})
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	paths := sourcePaths(sources)
	if hasPathSuffix(paths, "/a.parquet") || hasPathSuffix(paths, "/c.parquet") {
		t.Fatalf("open set included A or C: %v", paths)
	}
	if !hasPathSuffix(paths, "/b.parquet") {
		t.Fatalf("open set missing B: %v", paths)
	}
}

func TestMetricsOpenSetIncludesOverlappingFile(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	root := openSetRoot(t, dataDir)
	l0 := filepath.Join(root, "tiers", "L0")
	// One file whose range crosses start (sample at start-30s still overlaps
	// a query that begins at start when min==max==that sample).
	sample := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	writeOpenSetFile(t, filepath.Join(l0, "overlap.parquet"), sample, "up")
	if err := metricsmeta.SyncManifest(dataDir, openSetTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}
	sources, err := collectMetricsSources(root, metricsOpenOpts{
		Start: sample.Add(-time.Minute),
		End:   sample.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if !hasPathSuffix(sourcePaths(sources), "/overlap.parquet") {
		t.Fatalf("overlapping file dropped: %v", sourcePaths(sources))
	}
}

func TestMetricsOpenSetHotOnlyOmitsTiersOnWideRange(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	root := openSetRoot(t, dataDir)
	hotTs := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	writeOpenSetFile(t, filepath.Join(root, "hot", "current.parquet"), hotTs, "up")
	writeOpenSetFile(t, filepath.Join(root, "tiers", "L0", "old.parquet"), hotTs.Add(-24*time.Hour), "up")
	if err := metricsmeta.SyncManifest(dataDir, openSetTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}
	sources, err := collectMetricsSources(root, metricsOpenOpts{
		HotOnly: true,
		Start:   hotTs.Add(-24 * time.Hour),
		End:     hotTs.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	paths := sourcePaths(sources)
	if len(paths) != 1 || !hasPathSuffix(paths, "hot/current.parquet") {
		t.Fatalf("hot_only want only snapshot, got %v", paths)
	}
}

func TestMetricsOpenSetProcessHotOnlyCannotWiden(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	root := openSetRoot(t, dataDir)
	hotTs := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	writeOpenSetFile(t, filepath.Join(root, "hot", "current.parquet"), hotTs, "up")
	writeOpenSetFile(t, filepath.Join(root, "tiers", "L0", "cold.parquet"), hotTs, "up")
	if err := metricsmeta.SyncManifest(dataDir, openSetTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}
	// Process QUERY_HOT_ONLY is modeled as HotOnly=true; a request cannot
	// pass a flag that widens past that (there is no widen API).
	sources, err := collectMetricsSources(root, metricsOpenOpts{HotOnly: true})
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, p := range sourcePaths(sources) {
		if strings.Contains(p, "/tiers/") {
			t.Fatalf("process hot-only leaked a tier path: %v", sourcePaths(sources))
		}
	}
}

func TestMetricsOpenSetSkippedPathsAbsentFromUnionSQL(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	root := openSetRoot(t, dataDir)
	l0 := filepath.Join(root, "tiers", "L0")
	tA := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tB := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	writeOpenSetFile(t, filepath.Join(l0, "skip-me.parquet"), tA, "up")
	writeOpenSetFile(t, filepath.Join(l0, "keep-me.parquet"), tB, "up")
	if err := metricsmeta.SyncManifest(dataDir, openSetTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}
	sqlText, err := sandboxMetricsUnionSQL(root, metricsOpenOpts{Start: tB, End: tB.Add(time.Hour)})
	if err != nil {
		t.Fatalf("union sql: %v", err)
	}
	if strings.Contains(sqlText, "skip-me.parquet") {
		t.Fatalf("skipped file passed to read_parquet: %s", sqlText)
	}
	if strings.Contains(strings.ToUpper(sqlText), "ATTACH") && strings.Contains(sqlText, "skip-me") {
		t.Fatalf("skipped file passed to ATTACH: %s", sqlText)
	}
	if !strings.Contains(sqlText, "keep-me.parquet") {
		t.Fatalf("kept file missing from union: %s", sqlText)
	}
}

func TestMetricsAutoHotUsesSnapshotCoverage(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	root := openSetRoot(t, dataDir)
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	hotMin := now.Add(-10 * time.Minute)
	writeOpenSetFile(t, filepath.Join(root, "hot", "current.parquet"), hotMin, "up")
	// A second sample in the same snapshot at `now` so coverage is the window.
	testparquet.WriteSegmentRows(t, filepath.Join(root, "hot", "current.parquet"), []testparquet.SegRow{
		{Name: "up", Labels: "{}", Value: 1, Ts: hotMin},
		{Name: "up", Labels: "{}", Value: 1, Ts: now},
	})
	writeOpenSetFile(t, filepath.Join(root, "tiers", "L0", "older.parquet"), now.Add(-2*time.Hour), "up")
	if err := metricsmeta.SyncManifest(dataDir, openSetTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}
	sources, err := collectMetricsSources(root, metricsOpenOpts{
		Start:     hotMin,
		End:       now,
		Now:       now,
		HotWindow: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	paths := sourcePaths(sources)
	if hasPathSuffix(paths, "/older.parquet") {
		t.Fatalf("auto-hot still opened cold file: %v", paths)
	}
	if !hasPathSuffix(paths, "hot/current.parquet") {
		t.Fatalf("auto-hot missing snapshot: %v", paths)
	}
}

func TestMetricsAutoHotWideRangeIncludesOverlappingCold(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	root := openSetRoot(t, dataDir)
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	testparquet.WriteSegmentRows(t, filepath.Join(root, "hot", "current.parquet"), []testparquet.SegRow{
		{Name: "up", Labels: "{}", Value: 1, Ts: now.Add(-10 * time.Minute)},
		{Name: "up", Labels: "{}", Value: 1, Ts: now},
	})
	writeOpenSetFile(t, filepath.Join(root, "tiers", "L0", "day.parquet"), now.Add(-12*time.Hour), "up")
	if err := metricsmeta.SyncManifest(dataDir, openSetTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}
	sources, err := collectMetricsSources(root, metricsOpenOpts{
		Start:     now.Add(-24 * time.Hour),
		End:       now,
		Now:       now,
		HotWindow: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	paths := sourcePaths(sources)
	if !hasPathSuffix(paths, "hot/current.parquet") || !hasPathSuffix(paths, "/day.parquet") {
		t.Fatalf("24h range want hot+overlapping cold, got %v", paths)
	}
}

func TestMetricsHourlyPartitionsOneHourQueryOpensAtMostHotPlusTwo(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	root := openSetRoot(t, dataDir)
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	l0 := filepath.Join(root, "tiers", "L0")
	for i := 0; i < 7; i++ {
		ts := base.Add(time.Duration(i) * time.Hour)
		name := string(rune('a'+i)) + ".parquet"
		writeOpenSetFile(t, filepath.Join(l0, name), ts, "up")
	}
	testparquet.WriteSegmentRows(t, filepath.Join(root, "hot", "current.parquet"), []testparquet.SegRow{
		{Name: "up", Labels: "{}", Value: 1, Ts: base.Add(7 * time.Hour)},
	})
	if err := metricsmeta.SyncManifest(dataDir, openSetTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}

	start := base.Add(3 * time.Hour)
	end := start.Add(time.Hour)
	sources, err := collectMetricsSources(root, metricsOpenOpts{Start: start, End: end})
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	var l0n int
	for _, p := range sourcePaths(sources) {
		if strings.Contains(p, "/tiers/L0/") {
			l0n++
		}
	}
	if l0n > 2 {
		t.Fatalf("1h query opened %d L0 files, want at most 2: %v", l0n, sourcePaths(sources))
	}
}

func TestMetricsFilterOverlapSemantics(t *testing.T) {
	t.Parallel()
	files := []metricsFileMeta{
		{Path: "A", MinTsNs: 0, MaxTsNs: 9},
		{Path: "B", MinTsNs: 10, MaxTsNs: 19},
		{Path: "C", MinTsNs: 20, MaxTsNs: 29},
	}
	got := filterMetricsFiles(files, 10, 20)
	if len(got) != 1 || got[0].Path != "B" {
		t.Fatalf("filter = %+v, want only B", got)
	}
	overlap := filterMetricsFiles([]metricsFileMeta{{Path: "X", MinTsNs: 5, MaxTsNs: 15}}, 10, 20)
	if len(overlap) != 1 {
		t.Fatalf("crossing start should be kept, got %+v", overlap)
	}
}
