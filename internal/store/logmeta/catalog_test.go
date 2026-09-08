package logmeta

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/metrics"
)

func TestManifestRoundTripIncludesMtimeNs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tenant := "user-logmtime-apps"
	artifact := "logs-raw"
	want := Manifest{
		Version: 2,
		Files: []ManifestFile{
			{Path: "tiers/L0/a.parquet", MinTsNs: 10, MaxTsNs: 20, Bytes: 44, MtimeNs: 99},
		},
	}
	if err := WriteManifest(dir, tenant, artifact, want); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}
	got, err := ReadManifest(dir, tenant, artifact)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got.Files[0].MtimeNs != 99 {
		t.Fatalf("MtimeNs=%d want 99", got.Files[0].MtimeNs)
	}
}

func TestLookupHitRequiresPathSizeAndMtime(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-loglookup-apps"
	artifact := "logs-raw"
	_, rel, size, mtimeNs := writeLogL0(t, dir, tenant, artifact, "1786140844863329878-live.parquet")

	cat, err := Load(context.Background(), dir, "", tenant, artifact)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, hit := cat.Lookup(rel, size, mtimeNs); hit {
		t.Fatal("empty catalog must miss")
	}
	cat.Upsert(ManifestFile{Path: rel, Bytes: size, MtimeNs: mtimeNs, MinTsNs: 1, MaxTsNs: 1})
	if err := cat.Persist(); err != nil {
		t.Fatal(err)
	}
	cat, err = Load(context.Background(), dir, "", tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, hit := cat.Lookup(rel, size, mtimeNs); !hit {
		t.Fatal("matching fingerprint must hit")
	}
	if _, hit := cat.Lookup(rel, size+1, mtimeNs); hit {
		t.Fatal("size change must miss")
	}
	if _, hit := cat.Lookup(rel, size, mtimeNs+1); hit {
		t.Fatal("mtime change must miss")
	}
}

func TestLoadMissingManifestIsEmptyNotRebuild(t *testing.T) {
	bindCatalogMetrics(t)
	dir := t.TempDir()
	tenant := "user-logmissing-apps"
	artifact := "logs-raw"
	writeLogL0(t, dir, tenant, artifact, "1786140844863329878-a.parquet")

	cat, err := Load(context.Background(), dir, "", tenant, artifact)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cat.Rebuilt() {
		t.Fatal("missing JSON must not rebuild")
	}
	if len(cat.Files()) != 0 {
		t.Fatalf("files=%v want empty", cat.Files())
	}
}

func TestLoadCorruptJSONRebuildsOnceAndLogs(t *testing.T) {
	bindCatalogMetrics(t)
	dir := t.TempDir()
	tenant := "user-logcorrupt-apps"
	artifact := "logs-raw"
	writeLogL0(t, dir, tenant, artifact, "1786140844863329878-a.parquet")
	man := ManifestPath(dir, tenant, artifact)
	if err := os.WriteFile(man, []byte("]nope"), 0o600); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	cat, err := Load(context.Background(), dir, "", tenant, artifact)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cat.Rebuilt() {
		t.Fatal("corrupt JSON must rebuild")
	}
	if !strings.Contains(buf.String(), "logs catalog full rebuild") {
		t.Fatalf("missing rebuild log: %s", buf.String())
	}

	cat2, err := Load(context.Background(), dir, "", tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if cat2.Rebuilt() {
		t.Fatal("second Load must not rebuild")
	}
}

func TestApplyDeltaUpsertsDestAndDropsSources(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-logdelta-apps"
	artifact := "logs-raw"
	_, srcRel, srcSize, srcMtime := writeLogL0(t, dir, tenant, artifact, "1786140844863329878-src.parquet")
	_, destRel, destSize, destMtime := writeLogL0(t, dir, tenant, artifact, "1786140844863329879-dest.parquet")
	if err := WriteManifest(dir, tenant, artifact, Manifest{
		Version: 1,
		Files: []ManifestFile{
			{Path: srcRel, Bytes: srcSize, MtimeNs: srcMtime, MinTsNs: 8, MaxTsNs: 8},
		},
	}); err != nil {
		t.Fatal(err)
	}
	err := ApplyDelta(dir, "", tenant, artifact, []ManifestFile{{
		Path: destRel, Bytes: destSize, MtimeNs: destMtime, MinTsNs: 3, MaxTsNs: 4,
	}}, []string{srcRel})
	if err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	got, err := ReadManifest(dir, tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Files) != 1 || got.Files[0].Path != destRel {
		t.Fatalf("catalog after delta = %+v", got.Files)
	}
}

func TestRebuildManifestRootsIncludesColdL0(t *testing.T) {
	t.Parallel()
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-logcoldl0-apps"
	artifact := "logs-raw"
	l0 := filepath.Join(cold, tenant, "logs", artifact, "tiers", "L0")
	if err := os.MkdirAll(l0, 0o750); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(l0, "1786140844863329878-cold.parquet"))

	m, err := RebuildManifestRoots(hot, cold, tenant, artifact, 1)
	if err != nil {
		t.Fatalf("RebuildManifestRoots: %v", err)
	}
	found := false
	for _, f := range m.Files {
		if f.Path == "tiers/L0/1786140844863329878-cold.parquet" {
			found = true
			if f.MtimeNs == 0 {
				t.Fatal("cold L0 mtime_ns unset")
			}
		}
	}
	if !found {
		t.Fatalf("cold L0 missing: %+v", m.Files)
	}
}

func TestSyncManifestRootsDoesNotRebuildLiveBounds(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-logsync-apps"
	artifact := "logs-raw"
	_, rel, size, mtimeNs := writeLogL0(t, dir, tenant, artifact, "1786140844863329878-keep.parquet")
	if err := WriteManifest(dir, tenant, artifact, Manifest{
		Version: 1,
		Files: []ManifestFile{
			{Path: rel, Bytes: size, MtimeNs: mtimeNs, MinTsNs: 777, MaxTsNs: 888},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := SyncManifestRoots(dir, "", tenant, artifact); err != nil {
		t.Fatalf("SyncManifestRoots: %v", err)
	}
	got, err := ReadManifest(dir, tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Files) != 1 || got.Files[0].MinTsNs != 777 {
		t.Fatalf("success-path sync restatted the tree: %+v", got.Files)
	}
}

func writeLogL0(t *testing.T, dataDir, tenant, artifact, name string) (abs, rel string, size, mtimeNs int64) {
	t.Helper()
	abs = filepath.Join(dataDir, tenant, "logs", artifact, "tiers", "L0", name)
	if err := os.MkdirAll(filepath.Dir(abs), 0o750); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, abs)
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

func TestLookupMissThenHitAfterUpsert(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-logmisshit-apps"
	artifact := "logs-raw"
	abs, rel, size, mtimeNs := writeLogL0(t, dir, tenant, artifact, "1786140844863329878-new.parquet")
	cat, err := Load(context.Background(), dir, "", tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, hit := cat.Lookup(rel, size, mtimeNs); hit {
		t.Fatal("new file must miss")
	}
	fi, err := os.Stat(abs)
	if err != nil {
		t.Fatal(err)
	}
	cat.Upsert(ManifestFile{Path: rel, Bytes: fi.Size(), MtimeNs: fi.ModTime().UnixNano(), MinTsNs: 5, MaxTsNs: 5})
	if _, hit := cat.Lookup(rel, fi.Size(), fi.ModTime().UnixNano()); !hit {
		t.Fatal("after upsert must hit")
	}
}

func TestMtimeChangeMissesUntilUpsert(t *testing.T) {
	dir := t.TempDir()
	tenant := "user-logmtimechg-apps"
	artifact := "logs-raw"
	abs, rel, size, mtimeNs := writeLogL0(t, dir, tenant, artifact, "1786140844863329878-m.parquet")
	cat, err := Load(context.Background(), dir, "", tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	cat.Upsert(ManifestFile{Path: rel, Bytes: size, MtimeNs: mtimeNs, MinTsNs: 1, MaxTsNs: 1})
	newMtime := time.Unix(0, mtimeNs).Add(time.Second)
	if err := os.Chtimes(abs, newMtime, newMtime); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(abs)
	if err != nil {
		t.Fatal(err)
	}
	if _, hit := cat.Lookup(rel, fi.Size(), fi.ModTime().UnixNano()); hit {
		t.Fatal("mtime change must miss")
	}
}
