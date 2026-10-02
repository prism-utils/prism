package gc

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
)

const gcTenant = "user-gctest-apps"

func TestHotDirGCRemovesOrphanSnapshotTmpAndWal(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second
	hot := filepath.Join(dataDir, gcTenant, "hot")

	liveDuck := filepath.Join(hot, "current.duckdb")
	liveParquet := filepath.Join(hot, "current.parquet")
	liveWal := filepath.Join(hot, "current.duckdb.wal")
	foreign := filepath.Join(hot, "orphan.tmp")
	writeFile(t, liveDuck, []byte("live-duck"), now)
	writeFile(t, liveParquet, []byte("live-parquet"), now)
	writeFile(t, liveWal, []byte("live-wal"), now)
	writeFile(t, foreign, []byte("foreign"), now.Add(-time.Hour))

	stale := []string{
		filepath.Join(hot, "current.duckdb.deadbeef.tmp"),
		filepath.Join(hot, "current.duckdb.deadbeef.tmp.wal"),
		filepath.Join(hot, "current.parquet.abcd0123.tmp"),
		filepath.Join(hot, "current.parquet.abcd0123.tmp.wal"),
	}
	for _, p := range stale {
		writeFile(t, p, []byte("scratch"), now.Add(-grace-time.Second))
	}

	if err := HotDir(dataDir, gcTenant, now, grace); err != nil {
		t.Fatalf("HotDir: %v", err)
	}
	for _, p := range stale {
		mustGone(t, p)
	}
	mustRemain(t, liveDuck)
	mustRemain(t, liveParquet)
	mustRemain(t, liveWal)
	mustRemain(t, foreign)
}

func TestHotDirGCLeavesInFlightTmp(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second
	hot := filepath.Join(dataDir, gcTenant, "hot")

	inFlight := filepath.Join(hot, "current.duckdb.aabbccdd.tmp")
	inFlightWal := filepath.Join(hot, "current.duckdb.aabbccdd.tmp.wal")
	equal := filepath.Join(hot, "current.parquet.eeff0011.tmp")
	writeFile(t, inFlight, []byte("inflight"), now.Add(-grace+time.Second))
	writeFile(t, inFlightWal, []byte("inflight-wal"), now.Add(-grace/2))
	writeFile(t, equal, []byte("equal"), now.Add(-grace))

	if err := HotDir(dataDir, gcTenant, now, grace); err != nil {
		t.Fatalf("HotDir: %v", err)
	}
	mustRemain(t, inFlight)
	mustRemain(t, inFlightWal)
	mustRemain(t, equal)
}

func TestHotDirGCRemovesStaleReadPins(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second
	hot := filepath.Join(dataDir, gcTenant, "hot")

	live := filepath.Join(hot, "current.duckdb")
	writeFile(t, live, []byte("live"), now)
	pinDuck := filepath.Join(hot, ".read-deadbeef.duckdb")
	pinParquet := filepath.Join(hot, ".read-cafebabe.parquet")
	writeFile(t, pinDuck, []byte("pin-duck"), now.Add(-grace-time.Second))
	writeFile(t, pinParquet, []byte("pin-parq"), now.Add(-time.Hour))

	if err := HotDir(dataDir, gcTenant, now, grace); err != nil {
		t.Fatalf("HotDir: %v", err)
	}
	mustGone(t, pinDuck)
	mustGone(t, pinParquet)
	mustRemain(t, live)
}

func TestEngineSpillGCRemovesOrphanDuckDBTmp(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second

	t.Run("file", func(t *testing.T) {
		dataDir := t.TempDir()
		root := filepath.Join(dataDir, gcTenant)
		live := filepath.Join(root, "engine.duckdb")
		wal := filepath.Join(root, "engine.duckdb.wal")
		spill := filepath.Join(root, "engine.duckdb.tmp")
		writeFile(t, live, []byte("catalog"), now)
		writeFile(t, wal, []byte("wal"), now)
		writeFile(t, spill, []byte("spill"), now.Add(-grace-time.Second))

		if err := EngineSpill(dataDir, gcTenant, now, grace, false); err != nil {
			t.Fatalf("EngineSpill: %v", err)
		}
		mustGone(t, spill)
		mustRemain(t, live)
		mustRemain(t, wal)
	})

	t.Run("dir", func(t *testing.T) {
		dataDir := t.TempDir()
		root := filepath.Join(dataDir, gcTenant)
		live := filepath.Join(root, "engine.duckdb")
		spill := filepath.Join(root, "engine.duckdb.tmp")
		writeFile(t, live, []byte("catalog"), now)
		writeFile(t, filepath.Join(spill, "block"), []byte("spill-block"), now.Add(-grace-time.Second))
		chtimes(t, spill, now.Add(-grace-time.Second))

		if err := EngineSpill(dataDir, gcTenant, now, grace, false); err != nil {
			t.Fatalf("EngineSpill: %v", err)
		}
		mustGone(t, spill)
		mustRemain(t, live)
	})

	t.Run("open", func(t *testing.T) {
		dataDir := t.TempDir()
		root := filepath.Join(dataDir, gcTenant)
		live := filepath.Join(root, "engine.duckdb")
		spill := filepath.Join(root, "engine.duckdb.tmp")
		writeFile(t, live, []byte("catalog"), now)
		writeFile(t, spill, []byte("spill"), now.Add(-grace-time.Second))

		if err := EngineSpill(dataDir, gcTenant, now, grace, true); err != nil {
			t.Fatalf("EngineSpill: %v", err)
		}
		mustRemain(t, spill)
		mustRemain(t, live)
	})
}

func TestHotDirGCMissingOrEmptyDir(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	dataDir := t.TempDir()
	if err := HotDir(dataDir, gcTenant, now, 120*time.Second); err != nil {
		t.Fatalf("missing hot dir: %v", err)
	}
	hot := filepath.Join(dataDir, gcTenant, "hot")
	if err := os.MkdirAll(hot, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := HotDir(dataDir, gcTenant, now, 0); err != nil {
		t.Fatalf("empty hot dir: %v", err)
	}
}

func TestHotDirGCZeroGraceFloorsAt120s(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	hot := filepath.Join(dataDir, gcTenant, "hot")
	young := filepath.Join(hot, "current.duckdb.aaaabbbb.tmp")
	old := filepath.Join(hot, "current.duckdb.ccccdddd.tmp")
	writeFile(t, young, []byte("young"), now.Add(-60*time.Second))
	writeFile(t, old, []byte("old"), now.Add(-121*time.Second))

	if err := HotDir(dataDir, gcTenant, now, 0); err != nil {
		t.Fatalf("HotDir: %v", err)
	}
	mustRemain(t, young)
	mustGone(t, old)
}

func TestTenantGCReclaimsHotAndSpill(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second
	hotTmp := filepath.Join(dataDir, gcTenant, "hot", "current.duckdb.deadbeef.tmp")
	spill := filepath.Join(dataDir, gcTenant, "engine.duckdb.tmp")
	live := filepath.Join(dataDir, gcTenant, "engine.duckdb")
	writeFile(t, hotTmp, []byte("hot"), now.Add(-grace-time.Second))
	writeFile(t, spill, []byte("spill"), now.Add(-grace-time.Second))
	writeFile(t, live, []byte("catalog"), now)

	if err := Tenant(dataDir, gcTenant, now, grace, false); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	mustGone(t, hotTmp)
	mustGone(t, spill)
	mustRemain(t, live)
}

func TestMaterializeGCRemovesStaleDestTmp(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second
	dir := layout.MaterializationDir(dataDir, gcTenant, "last_events")
	staleParquet := filepath.Join(dir, "seg.parquet.tmp")
	staleDuck := filepath.Join(dir, "seg.duckdb.tmp")
	foreign := filepath.Join(dir, "orphan.tmp")
	writeFile(t, staleParquet, []byte("partial-parquet"), now.Add(-grace-time.Second))
	writeFile(t, staleDuck, []byte("partial-duck"), now.Add(-grace-time.Second))
	writeFile(t, foreign, []byte("foreign"), now.Add(-time.Hour))

	if err := Materializations(dataDir, gcTenant, now, grace); err != nil {
		t.Fatalf("Materializations: %v", err)
	}
	mustGone(t, staleParquet)
	mustGone(t, staleDuck)
	mustRemain(t, foreign)
}

func TestMaterializeGCLeavesInFlightTmp(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second
	dir := layout.MaterializationDir(dataDir, gcTenant, "last_events")
	inFlight := filepath.Join(dir, "seg.parquet.tmp")
	equal := filepath.Join(dir, "other.duckdb.tmp")
	writeFile(t, inFlight, []byte("inflight"), now.Add(-grace+time.Second))
	writeFile(t, equal, []byte("equal"), now.Add(-grace))

	if err := Materializations(dataDir, gcTenant, now, grace); err != nil {
		t.Fatalf("Materializations: %v", err)
	}
	mustRemain(t, inFlight)
	mustRemain(t, equal)
}

func TestMaterializeGCLeavesLiveParquet(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second
	dir := layout.MaterializationDir(dataDir, gcTenant, "last_events")
	liveParquet := filepath.Join(dir, "seg.parquet")
	liveDuck := filepath.Join(dir, "seg.duckdb")
	writeFile(t, liveParquet, []byte("live-parquet"), now.Add(-time.Hour))
	writeFile(t, liveDuck, []byte("live-duck"), now.Add(-time.Hour))

	if err := Materializations(dataDir, gcTenant, now, grace); err != nil {
		t.Fatalf("Materializations: %v", err)
	}
	mustRemain(t, liveParquet)
	mustRemain(t, liveDuck)
}

func TestMaterializeGCMissingOrEmptyRoot(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	dataDir := t.TempDir()
	if err := Materializations(dataDir, gcTenant, now, 120*time.Second); err != nil {
		t.Fatalf("missing materializations: %v", err)
	}
	root := filepath.Join(dataDir, gcTenant, "materializations")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := Materializations(dataDir, gcTenant, now, 0); err != nil {
		t.Fatalf("empty materializations: %v", err)
	}
}

func TestMaterializeGCZeroGraceFloorsAt120s(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	dir := layout.MaterializationDir(dataDir, gcTenant, "last_events")
	young := filepath.Join(dir, "seg.parquet.tmp")
	old := filepath.Join(dir, "seg.duckdb.tmp")
	writeFile(t, young, []byte("young"), now.Add(-60*time.Second))
	writeFile(t, old, []byte("old"), now.Add(-121*time.Second))

	if err := Materializations(dataDir, gcTenant, now, 0); err != nil {
		t.Fatalf("Materializations: %v", err)
	}
	mustRemain(t, young)
	mustGone(t, old)
}

func TestTenantGCReclaimsMaterializeTmp(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second
	tmp := filepath.Join(layout.MaterializationDir(dataDir, gcTenant, "last_events"), "seg.parquet.tmp")
	live := filepath.Join(layout.MaterializationDir(dataDir, gcTenant, "last_events"), "seg.parquet")
	writeFile(t, tmp, []byte("scratch"), now.Add(-grace-time.Second))
	writeFile(t, live, []byte("done"), now)

	if err := Tenant(dataDir, gcTenant, now, grace, false); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	mustGone(t, tmp)
	mustRemain(t, live)
}

func TestEngineSpillGCMissingIsNil(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if err := EngineSpill(t.TempDir(), gcTenant, now, 120*time.Second, false); err != nil {
		t.Fatalf("missing spill: %v", err)
	}
}

func writeFile(t *testing.T, path string, body []byte, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	chtimes(t, path, mtime)
}

func chtimes(t *testing.T, path string, at time.Time) {
	t.Helper()
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
}

func mustGone(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s still present, stat err = %v", path, err)
	}
}

func mustRemain(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("%s missing: %v", path, err)
	}
}
