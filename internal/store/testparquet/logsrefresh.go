package testparquet

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/logmeta"
)

// PromoteLandedLogsToTier moves an artifact's buffered landing windows into
// tiers/L0 and re-stamps the log catalog, which is what makes those rows
// searchable. File contents are preserved, so a fixture keeps its window id and
// whichever ingest-time shape it was written with.
func PromoteLandedLogsToTier(t testing.TB, dataDir, tenant, artifact string) {
	t.Helper()
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	entries, err := os.ReadDir(landing)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("read landing: %v", err)
	}
	tier := layout.LogsTierDir(dataDir, tenant, artifact, 0)
	if err := os.MkdirAll(tier, 0o750); err != nil {
		t.Fatalf("mkdir tier: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || name[0] == '.' {
			continue
		}
		ext := filepath.Ext(name)
		if ext != ".parquet" && ext != ".duckdb" {
			continue
		}
		if err := os.Rename(filepath.Join(landing, name), filepath.Join(tier, name)); err != nil {
			t.Fatalf("promote %s: %v", name, err)
		}
	}
	if err := logmeta.Bump(dataDir, tenant); err != nil {
		t.Fatalf("bump log generation: %v", err)
	}
	if err := logmeta.SyncManifest(dataDir, tenant, artifact); err != nil {
		t.Fatalf("sync manifest: %v", err)
	}
}
