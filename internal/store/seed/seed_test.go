package seed

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureMetricsRawSeedForTenantWritesUnderPartition(t *testing.T) {
	dir := t.TempDir()
	const ns = "user-6f3a9c2b-apps"
	if err := EnsureMetricsRawSeedForTenant(dir, ns); err != nil {
		t.Fatalf("tenant seed: %v", err)
	}
	path := filepath.Join(dir, ns, "metrics-raw", SeedName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("tenant seed not under partition: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("seed is empty")
	}
	if _, err := os.Stat(filepath.Join(dir, "metrics-raw", SeedName)); !os.IsNotExist(err) {
		t.Fatal("tenant seed leaked into shared root")
	}

	before := info.ModTime()
	if err := EnsureMetricsRawSeedForTenant(dir, ns); err != nil {
		t.Fatalf("second seed: %v", err)
	}
	info2, _ := os.Stat(path)
	if info2.Size() != info.Size() {
		t.Fatalf("seed size changed on second call: %d -> %d", info.Size(), info2.Size())
	}
	if !info2.ModTime().Equal(before) && info2.ModTime().Before(before) {
		t.Fatal("seed was overwritten on second call")
	}
}

func TestEnsureTieredLayoutForTenantWritesAllSeeds(t *testing.T) {
	dir := t.TempDir()
	const ns = "user-6f3a9c2b-apps"
	if err := EnsureTieredLayoutForTenant(dir, ns); err != nil {
		t.Fatalf("tiered layout: %v", err)
	}
	want := []string{
		filepath.Join(ns, "tiers", SeedName),
		filepath.Join(ns, "hot", "current.parquet"),
		filepath.Join(ns, "rollups", "1m", SeedName),
		filepath.Join(ns, "rollups", "5m", SeedName),
		filepath.Join(ns, "rollups", "1h", SeedName),
	}
	for _, rel := range want {
		path := filepath.Join(dir, rel)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("missing %s: %v", rel, err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s is empty", rel)
		}
	}

	if err := EnsureTieredLayoutForTenant(dir, ns); err != nil {
		t.Fatalf("second tiered layout: %v", err)
	}
}

func TestSeedIsNonEmpty(t *testing.T) {
	if len(metricsRawSeed) == 0 {
		t.Fatal("embedded seed parquet is empty")
	}
}
