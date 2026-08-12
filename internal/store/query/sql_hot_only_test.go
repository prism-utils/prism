package query

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestCollectSafeParquetPathsHotOnlySkipsTiers(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	tenant := "user-abc123-apps"
	tenantRoot := filepath.Join(dataDir, tenant)
	l0Dir := filepath.Join(tenantRoot, "tiers", "L0")
	if err := os.MkdirAll(l0Dir, 0o750); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(tenantRoot, hotSnapshotRel)
	testparquet.WriteSegmentWithTs(t, snapshot, time.Unix(1700000000, 0).UTC(), "hot", 1)
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0Dir, "tier.parquet"), time.Unix(1700000000, 0).UTC(), "tier", 2)

	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := collectSafeParquetPaths(absRoot, tenantRoot, true)
	if err != nil {
		t.Fatalf("collect hot-only: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("paths=%v want single hot snapshot", paths)
	}
	if !strings.HasSuffix(filepath.ToSlash(paths[0]), hotSnapshotRel) {
		t.Fatalf("path=%q want hot snapshot only", paths[0])
	}
}

func TestCollectSafeParquetPathsFullIncludesTiers(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	tenant := "user-abc123-apps"
	tenantRoot := filepath.Join(dataDir, tenant)
	l0Dir := filepath.Join(tenantRoot, "tiers", "L0")
	if err := os.MkdirAll(l0Dir, 0o750); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(tenantRoot, hotSnapshotRel)
	testparquet.WriteSegmentWithTs(t, snapshot, time.Unix(1700000000, 0).UTC(), "hot", 1)
	tierPath := filepath.Join(l0Dir, "tier.parquet")
	testparquet.WriteSegmentWithTs(t, tierPath, time.Unix(1700000000, 0).UTC(), "tier", 2)

	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := collectSafeParquetPaths(absRoot, tenantRoot, false)
	if err != nil {
		t.Fatalf("collect full: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("paths=%v want snapshot + tier", paths)
	}
	hasSnapshot, hasTier := false, false
	for _, p := range paths {
		slash := filepath.ToSlash(p)
		if strings.HasSuffix(slash, hotSnapshotRel) {
			hasSnapshot = true
		}
		if strings.Contains(slash, "/tiers/L0/") {
			hasTier = true
		}
	}
	if !hasSnapshot || !hasTier {
		t.Fatalf("paths=%v missing snapshot=%v tier=%v", paths, hasSnapshot, hasTier)
	}
}

func TestSandboxMetricsUnionSQLHotOnlyOmitsTiers(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	tenant := "user-abc123-apps"
	tenantRoot := filepath.Join(dataDir, tenant)
	l0Dir := filepath.Join(tenantRoot, "tiers", "L0")
	if err := os.MkdirAll(l0Dir, 0o750); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(tenantRoot, hotSnapshotRel)
	testparquet.WriteSegmentWithTs(t, snapshot, time.Unix(1700000000, 0).UTC(), "hot", 1)
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0Dir, "tier.parquet"), time.Unix(1700000000, 0).UTC(), "tier", 2)

	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		t.Fatal(err)
	}

	hotSQL, err := sandboxMetricsUnionSQL(absRoot, true)
	if err != nil {
		t.Fatalf("hot-only union: %v", err)
	}
	if strings.Contains(hotSQL, "/tiers/L") {
		t.Fatalf("hot-only union must not reference tiers: %s", hotSQL)
	}
	if !strings.Contains(hotSQL, hotSnapshotRel) {
		t.Fatalf("hot-only union must include snapshot: %s", hotSQL)
	}

	fullSQL, err := sandboxMetricsUnionSQL(absRoot, false)
	if err != nil {
		t.Fatalf("full union: %v", err)
	}
	if !strings.Contains(fullSQL, "/tiers/L0/") {
		t.Fatalf("full union must include tier path: %s", fullSQL)
	}
}
