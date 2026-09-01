package promote

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/layout"
)

func TestEligibleRejectsL0(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	old := now.Add(-24 * time.Hour)
	if Eligible(0, old, now, 12*time.Hour) {
		t.Fatal("L0 must stay hot even when older than After")
	}
}

func TestEligibleL1AfterWindow(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	if Eligible(1, now.Add(-11*time.Hour), now, 12*time.Hour) {
		t.Fatal("L1 younger than After must stay hot")
	}
	if !Eligible(1, now.Add(-12*time.Hour), now, 12*time.Hour) {
		t.Fatal("L1 at After must be eligible")
	}
	if !Eligible(2, now.Add(-13*time.Hour), now, 12*time.Hour) {
		t.Fatal("L2 older than After must be eligible")
	}
}

func TestCopyAtomicRoundTripAndChecksum(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	src := filepath.Join(srcDir, "seg.parquet")
	body := parquetFixture("payload-bytes")
	if err := os.WriteFile(src, body, 0o600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dstDir, "seg.parquet")
	if err := CopyAtomic(src, dest); err != nil {
		t.Fatalf("CopyAtomic: %v", err)
	}
	got, err := os.ReadFile(dest) //nolint:gosec // test dest
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("dest bytes differ from source")
	}
	tmps, _ := filepath.Glob(filepath.Join(dstDir, "*"+layout.PromoteTempSuffix))
	if len(tmps) != 0 {
		t.Fatalf("leftover temps: %v", tmps)
	}
}

func TestCopyAtomicRejectsBrokenParquet(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	src := filepath.Join(srcDir, "seg.parquet")
	if err := os.WriteFile(src, []byte("not parquet"), 0o600); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dstDir, "seg.parquet")
	if err := CopyAtomic(src, dest); err == nil {
		t.Fatal("CopyAtomic must reject a file without parquet magic")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("broken dest must not be published")
	}
}

func TestGCRemovesPromoteTempsLeavesFinal(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	dir := layout.TierDir(cold, tenant, 1)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(dir, "keep.parquet")
	if err := os.WriteFile(final, []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	tmp := layout.PromoteTempPath(dir, "keep.parquet", []byte{1, 2, 3, 4})
	if err := os.WriteFile(tmp, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := GCTenant(hot, cold, tenant, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("promote temp must be removed")
	}
	if _, err := os.Stat(final); err != nil {
		t.Fatal("final dest must remain")
	}
}

func TestTenantPromotesEligibleL1LeavesL0(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	l0 := filepath.Join(layout.TierDir(hot, tenant, 0), "old.parquet")
	l1 := filepath.Join(layout.TierDir(hot, tenant, 1), "old.parquet")
	body := parquetFixture("seg")
	writeFile(t, l0, body)
	writeFile(t, l1, body)
	old := now.Add(-24 * time.Hour)
	cfg := Config{
		DataDir: hot,
		ColdDir: cold,
		After:   12 * time.Hour,
		MaxTier: 8,
		Now:     func() time.Time { return now },
		MaxTs: func(path string) (time.Time, bool) {
			return old, true
		},
	}
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if _, err := os.Stat(l0); err != nil {
		t.Fatal("L0 must remain on hot")
	}
	if _, err := os.Stat(l1); !os.IsNotExist(err) {
		t.Fatal("eligible L1 must leave hot after dest verifies")
	}
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "old.parquet")
	got, err := os.ReadFile(dest) //nolint:gosec // test dest
	if err != nil {
		t.Fatalf("cold L1: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("cold L1 bytes differ")
	}
}

func TestTenantEmptyColdDirNoop(t *testing.T) {
	hot := t.TempDir()
	tenant := "user-a"
	l1 := filepath.Join(layout.TierDir(hot, tenant, 1), "old.parquet")
	writeFile(t, l1, parquetFixture("x"))
	cfg := Config{DataDir: hot, ColdDir: "", Now: time.Now}
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(l1); err != nil {
		t.Fatal("disabled promote must not move files")
	}
}

func TestRecoverMatchingDestUnlinksSource(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	body := parquetFixture("same")
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "seg.parquet")
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.parquet")
	writeFile(t, src, body)
	writeFile(t, dest, body)
	cfg := Config{
		DataDir: hot,
		ColdDir: cold,
		After:   time.Hour,
		MaxTier: 8,
		Now:     func() time.Time { return now },
		MaxTs:   func(string) (time.Time, bool) { return now.Add(-2 * time.Hour), true },
	}
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("matching dest must finish by unlinking source")
	}
}

func TestBrokenDestIsReplaced(t *testing.T) {
	hot := t.TempDir()
	cold := t.TempDir()
	tenant := "user-a"
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	body := parquetFixture("good")
	src := filepath.Join(layout.TierDir(hot, tenant, 1), "seg.parquet")
	dest := filepath.Join(layout.TierDir(cold, tenant, 1), "seg.parquet")
	writeFile(t, src, body)
	writeFile(t, dest, []byte("PAR1xxxxPAR1")) // wrong payload, valid-looking magic length
	cfg := Config{
		DataDir: hot,
		ColdDir: cold,
		After:   time.Hour,
		MaxTier: 8,
		Now:     func() time.Time { return now },
		MaxTs:   func(string) (time.Time, bool) { return now.Add(-2 * time.Hour), true },
	}
	if _, err := Tenant(&cfg, tenant); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest) //nolint:gosec // test dest
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("broken dest must be replaced with source bytes")
	}
}

func TestListHotCompactedSkipsEmptyParquet(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-empty-l1"
	dir := layout.LogsTierDir(dataDir, tenant, "logs-raw", 1)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dir, "keep.parquet")
	empty := filepath.Join(dir, "empty.parquet")
	writeFile(t, keep, parquetFixture("ok"))
	writeFile(t, empty, nil)

	got, err := listHotCompacted(dataDir, tenant, 2)
	if err != nil {
		t.Fatalf("listHotCompacted: %v", err)
	}
	if len(got) != 1 || got[0].Path != keep {
		t.Fatalf("listed = %+v, want only %s", got, keep)
	}
}

func parquetFixture(mid string) []byte {
	b := make([]byte, 0, 8+len(mid))
	b = append(b, parquetMagic...)
	b = append(b, mid...)
	b = append(b, parquetMagic...)
	return b
}

func writeFile(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}
