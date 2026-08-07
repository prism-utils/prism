package merge

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
)

func TestQuarantineCorruptLogLandingRemovesEmptyDuckDB(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-corrupt01-apps"
	artifact := "logs-summary"
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}

	empty := filepath.Join(landing, layout.SegmentNameFormat(time.Unix(0, 1786112191298973910), "duckdb"))
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(landing, layout.SegmentNameFormat(time.Unix(0, 1786112191298973911), "duckdb"))
	if err := os.WriteFile(good, []byte("not-empty"), 0o600); err != nil {
		t.Fatal(err)
	}

	removed, err := QuarantineCorruptLogLanding(dataDir, tenant, artifact)
	if err != nil {
		t.Fatalf("QuarantineCorruptLogLanding: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, err := os.Stat(empty); !os.IsNotExist(err) {
		t.Fatalf("empty file still present: %v", err)
	}
	if _, err := os.Stat(good); err != nil {
		t.Fatalf("good file missing: %v", err)
	}
}
