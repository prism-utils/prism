package stats

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prism-utils/prism/internal/store/testparquet"
)

const testTenant = "user-6f3a9c2b-apps"

func TestTenantOnDiskBytesSumsTieredLayout(t *testing.T) {
	root := t.TempDir()
	tenantRoot := filepath.Join(root, testTenant)
	writeFile(t, filepath.Join(tenantRoot, "engine.duckdb"), []byte("duckdb-bytes"))
	writeFile(t, filepath.Join(tenantRoot, "hot", "current.parquet"), []byte("hot"))
	writeFile(t, filepath.Join(tenantRoot, "tiers", "L0", "1.parquet"), []byte("tier0"))
	writeFile(t, filepath.Join(tenantRoot, "rollups", "1m", "1.parquet"), []byte("rollup"))

	got, err := TenantOnDiskBytes(root, testTenant)
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	want := int64(len("duckdb-bytes") + len("hot") + len("tier0") + len("rollup"))
	if got != want {
		t.Fatalf("want %d on-disk bytes, got %d", want, got)
	}
}

func TestCompactionCpuSecondsAccumulatesWithoutDoubleCount(t *testing.T) {
	root := t.TempDir()
	if err := AddCompactionCPUSeconds(root, testTenant, 1.25); err != nil {
		t.Fatalf("add1: %v", err)
	}
	if err := AddCompactionCPUSeconds(root, testTenant, 0.75); err != nil {
		t.Fatalf("add2: %v", err)
	}
	got, err := CompactionCPUSeconds(root, testTenant)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != 2.0 {
		t.Fatalf("want cumulative 2.0 compaction CPU seconds, got %v", got)
	}
}

func TestAddCompactionCPUSecondsNonPositiveNoWrite(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, testTenant, ".metering.json")
	for _, sec := range []float64{0, -1, -0.5} {
		if err := AddCompactionCPUSeconds(root, testTenant, sec); err != nil {
			t.Fatalf("add %v: %v", sec, err)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf(".metering.json must stay absent when adds are non-positive")
	}
	got, err := CompactionCPUSeconds(root, testTenant)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != 0 {
		t.Fatalf("non-positive adds must not increment counter, got %v", got)
	}
}

func TestAddCompactionCPUSecondsZeroAfterPositiveUnchanged(t *testing.T) {
	root := t.TempDir()
	if err := AddCompactionCPUSeconds(root, testTenant, 1.25); err != nil {
		t.Fatalf("add positive: %v", err)
	}
	if err := AddCompactionCPUSeconds(root, testTenant, 0); err != nil {
		t.Fatalf("add zero: %v", err)
	}
	got, err := CompactionCPUSeconds(root, testTenant)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != 1.25 {
		t.Fatalf("zero add must not change cumulative counter, got %v", got)
	}
}

func TestTenantOnDiskBytesIgnoresLegacyMetricsRaw(t *testing.T) {
	root := t.TempDir()
	tenantRoot := filepath.Join(root, testTenant)
	legacy := filepath.Join(tenantRoot, "metrics-raw", "metrics-raw-123-window.parquet")
	testparquet.WriteFile(t, legacy, []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	st, err := os.Stat(legacy)
	if err != nil {
		t.Fatal(err)
	}
	got, err := TenantOnDiskBytes(root, testTenant)
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	if got != st.Size() {
		t.Fatalf("metrics-raw must be billed, got %d want %d", got, st.Size())
	}
}

func TestTenantOnDiskBytesCountsLandingAndTemps(t *testing.T) {
	root := t.TempDir()
	tenantRoot := filepath.Join(root, testTenant)
	writeFile(t, filepath.Join(tenantRoot, "logs", "logs-raw", "win.parquet"), []byte("landing"))
	writeFile(t, filepath.Join(tenantRoot, "logs", "logs-raw", "win.parquet.tmp"), []byte("tmpxx"))
	writeFile(t, filepath.Join(tenantRoot, "tiers", "L0", "a.parquet.compacted"), []byte("hold"))
	got, err := TenantOnDiskBytes(root, testTenant)
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	want := int64(len("landing") + len("tmpxx") + len("hold"))
	if got != want {
		t.Fatalf("want %d billed bytes including landing/temps, got %d", want, got)
	}
}

func TestTenantOnDiskBytesRootsAddsColdTiers(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	writeFile(t, filepath.Join(hot, testTenant, "tiers", "L0", "a.parquet"), []byte("hotl0"))
	writeFile(t, filepath.Join(cold, testTenant, "tiers", "L1", "b.parquet"), []byte("coldl1"))
	writeFile(t, filepath.Join(cold, testTenant, "logs", "logs-raw", "c.parquet"), []byte("log"))

	got, err := TenantOnDiskBytesRoots(hot, cold, testTenant)
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	want := int64(len("hotl0") + len("coldl1") + len("log"))
	if got != want {
		t.Fatalf("want %d on-disk bytes, got %d", want, got)
	}
	hotOnly, err := TenantOnDiskBytesRoots(hot, "", testTenant)
	if err != nil {
		t.Fatal(err)
	}
	if hotOnly != int64(len("hotl0")) {
		t.Fatalf("empty cold dir must match hot-only sum, got %d", hotOnly)
	}
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatal(err)
	}
}
