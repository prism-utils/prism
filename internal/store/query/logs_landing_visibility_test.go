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

// logsFixture is one tenant holding a buffered landing window and a refreshed
// tier segment, with the tenant root already symlink-resolved the way the
// sandbox resolves it.
type logsFixture struct {
	dataDir    string
	tenantRoot string
	landing    string
	tier       string
}

func seedLandingAndTier(t *testing.T, artifact string) logsFixture {
	t.Helper()
	dataDir := t.TempDir()
	artifactDir := filepath.Join(dataDir, visibilityTenant, "logs", artifact)
	tierDir := filepath.Join(artifactDir, "tiers", "L0")
	if err := os.MkdirAll(tierDir, 0o750); err != nil {
		t.Fatal(err)
	}
	fx := logsFixture{
		dataDir: dataDir,
		landing: filepath.Join(artifactDir, layout.SegmentName(time.Unix(100, 0).UTC())),
		tier:    filepath.Join(tierDir, layout.SegmentName(time.Unix(200, 0).UTC())),
	}
	testparquet.WriteLogsRawFile(t, fx.landing, []testparquet.LogRow{{Message: "buffered", Format: "none"}})
	testparquet.WriteLogsRawFile(t, fx.tier, []testparquet.LogRow{{Message: "refreshed", Format: "none"}})
	fx.tenantRoot = absTenantRoot(t, dataDir, visibilityTenant)
	return fx
}

func TestLogsCatalogOmitsLandingWindows(t *testing.T) {
	InvalidateLogsMetaCache("")
	fx := seedLandingAndTier(t, "logs-raw")

	sqlText, files, err := sandboxLogsRelationSQL(fx.tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatalf("sandboxLogsRelationSQL: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("open set = %v, want only the tier segment", fileBases(files))
	}
	if filepath.Base(files[0].Path) != filepath.Base(fx.tier) {
		t.Fatalf("open set = %s, want %s", files[0].Path, fx.tier)
	}
	if strings.Contains(sqlText, filepath.Base(fx.landing)) {
		t.Fatalf("relation SQL must not scan the landing buffer: %s", truncate(sqlText, 300))
	}
}

func TestLogsCatalogEmptyWhenOnlyLandingBuffered(t *testing.T) {
	InvalidateLogsMetaCache("")
	dataDir := t.TempDir()
	dir := filepath.Join(dataDir, visibilityTenant, "logs", "logs-raw")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	testparquet.WriteLogsRawFile(t, filepath.Join(dir, layout.SegmentName(time.Unix(1, 0).UTC())), []testparquet.LogRow{
		{Message: "buffered", Format: "none"},
	})
	tenantRoot := absTenantRoot(t, dataDir, visibilityTenant)

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
	fx := seedLandingAndTier(t, "logs-raw")

	_, files, err := sandboxLokiLogsSQL(fx.tenantRoot, 0, 0, false, 0)
	if err != nil {
		t.Fatalf("sandboxLokiLogsSQL: %v", err)
	}
	if len(files) != 1 || filepath.Base(files[0].Path) != filepath.Base(fx.tier) {
		t.Fatalf("loki open set = %v, want only the tier segment", fileBases(files))
	}
}

func TestSQLSandboxLogsViewOmitsLandingRows(t *testing.T) {
	InvalidateLogsMetaCache("")
	fx := seedLandingAndTier(t, "logs-raw")

	ctx := context.Background()
	conn, cleanup, err := prepareSandboxConn(ctx, fx.tenantRoot, false, sandboxLimits{})
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
	fx := seedLandingAndTier(t, "logs-raw")

	// A synced manifest catalogs landing and tiers alike; the searchable open set
	// still has to drop the landing entries.
	if err := logmeta.Bump(fx.dataDir, visibilityTenant); err != nil {
		t.Fatal(err)
	}
	if err := logmeta.SyncManifest(fx.dataDir, visibilityTenant, "logs-raw"); err != nil {
		t.Fatal(err)
	}
	m, err := logmeta.ReadManifest(fx.dataDir, visibilityTenant, "logs-raw")
	if err != nil || len(m.Files) != 2 {
		t.Fatalf("manifest = %+v err = %v, want both windows catalogued", m, err)
	}
	InvalidateLogsMetaCache(fx.tenantRoot)

	_, files, err := sandboxLogsRelationSQL(fx.tenantRoot, logsCatalogOpts{})
	if err != nil {
		t.Fatalf("sandboxLogsRelationSQL: %v", err)
	}
	if len(files) != 1 || filepath.Base(files[0].Path) != filepath.Base(fx.tier) {
		t.Fatalf("manifest-backed open set = %v, want only the tier segment", fileBases(files))
	}
}
