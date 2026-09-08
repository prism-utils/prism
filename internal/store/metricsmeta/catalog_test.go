package metricsmeta

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/metrics"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestManifestRoundTripIncludesMtimeNs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-mtime-apps"
	want := Manifest{
		Version: 2,
		Files: []ManifestFile{
			{Path: "tiers/L0/a.parquet", MinTsNs: 10, MaxTsNs: 20, Bytes: 44, MtimeNs: 99},
		},
	}
	if err := WriteManifest(dir, tenant, want); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	raw, err := os.ReadFile(ManifestPath(dir, tenant))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"mtime_ns"`)) {
		t.Fatalf("on-disk JSON missing mtime_ns: %s", raw)
	}
	got, err := ReadManifest(dir, tenant)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.Files[0].MtimeNs != 99 {
		t.Fatalf("MtimeNs=%d want 99", got.Files[0].MtimeNs)
	}
}

func TestLookupHitRequiresPathSizeAndMtime(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-lookup-apps"
	path, rel, size, mtimeNs := writeL0(t, dir, tenant, "live.parquet")

	cat, err := Load(context.Background(), dir, "", tenant)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, hit := cat.Lookup(rel, size, mtimeNs); hit {
		t.Fatal("empty catalog must miss a file that was never upserted")
	}
	minNs, maxNs, ok := FileBounds(context.Background(), path)
	if !ok {
		t.Fatal("bounds")
	}
	cat.Upsert(ManifestFile{Path: rel, MinTsNs: minNs, MaxTsNs: maxNs, Bytes: size, MtimeNs: mtimeNs})
	if err := cat.Persist(); err != nil {
		t.Fatal(err)
	}

	cat, err = Load(context.Background(), dir, "", tenant)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got, hit := cat.Lookup(rel, size, mtimeNs)
	if !hit {
		t.Fatal("matching path+size+mtime must hit")
	}
	if got.MinTsNs != minNs || got.Bytes != size {
		t.Fatalf("entry = %+v", got)
	}
	if _, hit := cat.Lookup(rel, size+1, mtimeNs); hit {
		t.Fatal("size change must miss")
	}
	if _, hit := cat.Lookup(rel, size, mtimeNs+1); hit {
		t.Fatal("mtime change must miss")
	}
	if _, hit := cat.Lookup("tiers/L0/other.parquet", size, mtimeNs); hit {
		t.Fatal("unknown path must miss")
	}
}

func TestLoadMissingManifestIsEmptyNotRebuild(t *testing.T) {
	bindCatalogMetrics(t)
	dir := t.TempDir()
	tenant := "user-missing-apps"
	writeL0(t, dir, tenant, "a.parquet")

	cat, err := Load(context.Background(), dir, "", tenant)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cat.Rebuilt() {
		t.Fatal("missing JSON must not rebuild")
	}
	if len(cat.Files()) != 0 {
		t.Fatalf("files=%v want empty", cat.Files())
	}
	body := scrapeEnabled(t)
	if strings.Contains(body, `prism_store_catalog_rebuild_total{plane="metrics",reason="corrupt"}`) {
		if v := seriesValue(t, body, `prism_store_catalog_rebuild_total{plane="metrics",reason="corrupt"}`); v != 0 {
			t.Fatalf("rebuild_total=%v want 0", v)
		}
	}
}

func TestLoadCorruptJSONRebuildsOnceAndLogs(t *testing.T) {
	bindCatalogMetrics(t)
	dir := t.TempDir()
	tenant := "user-corrupt-apps"
	writeL0(t, dir, tenant, "a.parquet")
	man := ManifestPath(dir, tenant)
	if err := os.MkdirAll(filepath.Dir(man), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(man, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	cat, err := Load(context.Background(), dir, "", tenant)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cat.Rebuilt() {
		t.Fatal("corrupt JSON must rebuild")
	}
	if len(cat.Files()) != 1 {
		t.Fatalf("rebuilt files=%v", cat.Files())
	}
	logLine := buf.String()
	if !strings.Contains(logLine, "metrics catalog full rebuild") {
		t.Fatalf("missing rebuild log: %s", logLine)
	}
	if !strings.Contains(logLine, "reason=corrupt") && !strings.Contains(logLine, `reason="corrupt"`) {
		t.Fatalf("missing reason=corrupt: %s", logLine)
	}
	body := scrapeEnabled(t)
	if seriesValue(t, body, `prism_store_catalog_rebuild_total{plane="metrics",reason="corrupt"}`) != 1 {
		t.Fatalf("rebuild_total after corrupt:\n%s", body)
	}

	buf.Reset()
	cat2, err := Load(context.Background(), dir, "", tenant)
	if err != nil {
		t.Fatalf("second Load: %v", err)
	}
	if cat2.Rebuilt() {
		t.Fatal("second Load must not rebuild a valid JSON")
	}
	if seriesValue(t, scrapeEnabled(t), `prism_store_catalog_rebuild_total{plane="metrics",reason="corrupt"}`) != 1 {
		t.Fatal("rebuild counter must stay at 1")
	}
}

func TestDropRemovesVanishedPathsAndPersist(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-drop-apps"
	_, rel, size, mtimeNs := writeL0(t, dir, tenant, "gone.parquet")
	cat, err := Load(context.Background(), dir, "", tenant)
	if err != nil {
		t.Fatal(err)
	}
	cat.Upsert(ManifestFile{Path: rel, Bytes: size, MtimeNs: mtimeNs, MinTsNs: 1, MaxTsNs: 2})
	cat.Drop(rel)
	if err := cat.Persist(); err != nil {
		t.Fatal(err)
	}
	got, err := ReadManifest(dir, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Files) != 0 {
		t.Fatalf("dropped path still in catalog: %+v", got.Files)
	}
}

func TestApplyDeltaUpsertsDestAndDropsSourcesWithoutRebuild(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-delta-apps"
	_, srcRel, srcSize, srcMtime := writeL0(t, dir, tenant, "src.parquet")
	destPath, destRel, destSize, destMtime := writeL0(t, dir, tenant, "dest.parquet")
	minNs, maxNs, ok := FileBounds(context.Background(), destPath)
	if !ok {
		t.Fatal("dest bounds")
	}

	seed := Manifest{
		Version: 1,
		Files: []ManifestFile{
			{Path: srcRel, Bytes: srcSize, MtimeNs: srcMtime, MinTsNs: 9, MaxTsNs: 9},
			{Path: "tiers/L0/unrelated.parquet", Bytes: 1, MtimeNs: 1, MinTsNs: 111, MaxTsNs: 222},
		},
	}
	if err := WriteManifest(dir, tenant, seed); err != nil {
		t.Fatal(err)
	}

	err := ApplyDelta(context.Background(), dir, "", tenant, []ManifestFile{{
		Path: destRel, Bytes: destSize, MtimeNs: destMtime, MinTsNs: minNs, MaxTsNs: maxNs,
	}}, []string{srcRel})
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	got, err := ReadManifest(dir, tenant)
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]ManifestFile{}
	for _, f := range got.Files {
		byPath[f.Path] = f
	}
	if _, ok := byPath[srcRel]; ok {
		t.Fatal("source must be dropped")
	}
	if byPath[destRel].Bytes != destSize || byPath[destRel].MtimeNs != destMtime {
		t.Fatalf("dest = %+v", byPath[destRel])
	}
	if byPath["tiers/L0/unrelated.parquet"].MinTsNs != 111 {
		t.Fatal("ApplyDelta must not restat unrelated entries")
	}
}

func TestRebuildManifestRootsIncludesColdL0(t *testing.T) {
	t.Parallel()
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-coldl0-apps"
	ts := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	l0 := filepath.Join(cold, tenant, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0, "cold.parquet"), ts, "up", 1)

	m, err := RebuildManifestRoots(context.Background(), hot, cold, tenant, 3)
	if err != nil {
		t.Fatalf("RebuildManifestRoots: %v", err)
	}
	found := false
	for _, f := range m.Files {
		if f.Path == "tiers/L0/cold.parquet" {
			found = true
			if f.MtimeNs == 0 {
				t.Fatal("cold L0 mtime_ns unset")
			}
		}
	}
	if !found {
		t.Fatalf("cold L0 missing from rebuild: %+v", m.Files)
	}
}

func TestSyncAfterChangeRootsDoesNotRebuildLiveBounds(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-syncinc-apps"
	_, rel, size, mtimeNs := writeL0(t, dir, tenant, "keep.parquet")
	if err := WriteManifest(dir, tenant, Manifest{
		Version: 1,
		Files: []ManifestFile{
			{Path: rel, Bytes: size, MtimeNs: mtimeNs, MinTsNs: 4242, MaxTsNs: 4343},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := SyncAfterChangeRoots(context.Background(), dir, "", tenant); err != nil {
		t.Fatalf("SyncAfterChangeRoots: %v", err)
	}
	got, err := ReadManifest(dir, tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Files) != 1 || got.Files[0].MinTsNs != 4242 {
		t.Fatalf("success-path sync restatted the tree: %+v", got.Files)
	}
}

func writeL0(t *testing.T, dataDir, tenant, name string) (abs, rel string, size, mtimeNs int64) {
	t.Helper()
	abs = filepath.Join(dataDir, tenant, "tiers", "L0", name)
	testparquet.WriteSegmentWithTs(t, abs, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "up", 1)
	fi, err := os.Stat(abs)
	if err != nil {
		t.Fatal(err)
	}
	return abs, filepath.ToSlash(filepath.Join("tiers", "L0", name)), fi.Size(), fi.ModTime().UnixNano()
}

func bindCatalogMetrics(t *testing.T) {
	t.Helper()
	reg := metrics.New(metrics.Config{Enabled: true, Path: metrics.DefaultPath, PerTenant: true})
	metrics.Bind(reg)
	t.Cleanup(func() { metrics.Bind(nil) })
}

func scrapeEnabled(t *testing.T) string {
	t.Helper()
	r := metrics.Bound()
	if r == nil {
		t.Fatal("no bound registry")
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	r.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

func seriesValue(t *testing.T, body, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		rest, ok := strings.CutPrefix(line, series+" ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return v
	}
	return 0
}
