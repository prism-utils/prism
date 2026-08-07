package query

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/logmeta"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const visibilityTenant = "user-refresh-4b2e"

// seedLandingAndTier writes one buffered landing window and one refreshed tier
// segment, and returns their paths.
func seedLandingAndTier(t *testing.T, tenantRoot, artifact string) (landingPath, tierPath string) {
	t.Helper()
	landingDir := filepath.Join(tenantRoot, "logs", artifact)
	tierDir := filepath.Join(landingDir, "tiers", "L0")
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}
	landingPath = filepath.Join(landingDir, layout.SegmentName(time.Unix(100, 0).UTC()))
	tierPath = filepath.Join(tierDir, layout.SegmentName(time.Unix(200, 0).UTC()))
	testparquet.WriteLogsRawFile(t, landingPath, []testparquet.LogRow{{Message: "buffered", Format: "none"}})
	testparquet.WriteLogsRawFile(t, tierPath, []testparquet.LogRow{{Message: "refreshed", Format: "none"}})
	return landingPath, tierPath
}

func TestLogsCatalogOmitsLandingWindows(t *testing.T) {
	InvalidateLogsMetaCache("")
	tenantRoot := filepath.Join(t.TempDir(), visibilityTenant)
	landingPath, tierPath := seedLandingAndTier(t, tenantRoot, "logs-raw")

	sqlText, files, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatalf("sandboxLogsRelationSQL: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("open set = %v, want only the tier segment", fileBases(files))
	}
	if files[0].Path != tierPath {
		t.Fatalf("open set = %s, want %s", files[0].Path, tierPath)
	}
	if strings.Contains(sqlText, filepath.Base(landingPath)) {
		t.Fatalf("relation SQL must not scan the landing buffer: %s", truncate(sqlText, 300))
	}
}

func TestLogsCatalogEmptyWhenOnlyLandingBuffered(t *testing.T) {
	InvalidateLogsMetaCache("")
	tenantRoot := filepath.Join(t.TempDir(), visibilityTenant)
	dir := filepath.Join(tenantRoot, "logs", "logs-raw")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(time.Unix(1, 0).UTC())), []testparquet.LogRow{
		{Message: "buffered", Format: "none"},
	})

	sqlText, files, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatalf("sandboxLogsRelationSQL: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("open set = %v, want empty until a refresh opens L0", fileBases(files))
	}
	if sqlText != emptyLogsViewSQL {
		t.Fatalf("relation SQL = %s, want the empty logs relation", truncate(sqlText, 200))
	}
}

func TestLokiOpenSetOmitsLandingWindows(t *testing.T) {
	InvalidateLogsMetaCache("")
	tenantRoot := filepath.Join(t.TempDir(), visibilityTenant)
	_, tierPath := seedLandingAndTier(t, tenantRoot, "logs-raw")

	_, files, err := sandboxLokiLogsSQL(tenantRoot, 0, 0, false, 0)
	if err != nil {
		t.Fatalf("sandboxLokiLogsSQL: %v", err)
	}
	if len(files) != 1 || files[0].Path != tierPath {
		t.Fatalf("loki open set = %v, want only the tier segment", fileBases(files))
	}
}

func TestSQLSandboxLogsViewOmitsLandingRows(t *testing.T) {
	InvalidateLogsMetaCache("")
	tenantRoot := filepath.Join(t.TempDir(), visibilityTenant)
	seedLandingAndTier(t, tenantRoot, "logs-raw")

	ctx := context.Background()
	conn, cleanup, err := prepareSandboxConn(ctx, tenantRoot, false, sandboxLimits{})
	if err != nil {
		t.Fatalf("prepareSandboxConn: %v", err)
	}
	defer cleanup()

	rows, err := conn.QueryContext(ctx, "SELECT message FROM "+sandboxLogsView+" ORDER BY message")
	if err != nil {
		t.Fatalf("query logs view: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []string
	for rows.Next() {
		var msg string
		if err := rows.Scan(&msg); err != nil {
			t.Fatal(err)
		}
		got = append(got, msg)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "refreshed" {
		t.Fatalf("/sql logs rows = %v, want only the refreshed tier row", got)
	}
}

func TestLogsLandingExclusionAppliesToManifestCatalog(t *testing.T) {
	InvalidateLogsMetaCache("")
	dataDir := t.TempDir()
	tenant := visibilityTenant
	tenantRoot := absTenantRoot(t, dataDir, tenant)
	_, tierPath := seedLandingAndTier(t, filepath.Join(dataDir, tenant), "logs-raw")

	// A synced manifest catalogs landing and tiers alike; the searchable open set
	// still has to drop the landing entries.
	if err := logmeta.Bump(dataDir, tenant); err != nil {
		t.Fatal(err)
	}
	if err := logmeta.SyncManifest(dataDir, tenant, "logs-raw"); err != nil {
		t.Fatal(err)
	}
	InvalidateLogsMetaCache(tenantRoot)

	_, files, err := sandboxLogsRelationSQL(tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatalf("sandboxLogsRelationSQL: %v", err)
	}
	if len(files) != 1 || filepath.Base(files[0].Path) != filepath.Base(tierPath) {
		t.Fatalf("manifest-backed open set = %v, want only the tier segment", fileBases(files))
	}
}
