package lifecycle

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestTickMergeContinuesAfterArtifactError(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-isolate01-apps"
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	base := now.Add(-5 * time.Minute)

	rawLanding := layout.LogsLandingDir(dataDir, tenant, "logs-raw")
	if err := os.MkdirAll(rawLanding, 0o750); err != nil {
		t.Fatal(err)
	}
	poison := filepath.Join(rawLanding, layout.SegmentName(base))
	head := append([]byte("PAR1"), make([]byte, 200)...)
	head = append(head, []byte("PAR1")...)
	if err := os.WriteFile(poison, head, 0o600); err != nil {
		t.Fatal(err)
	}

	seedLandingWindows(t, dataDir, tenant, "logs-summary", base, 2)
	runner := refreshRunner(t, dataDir, func() time.Time { return now }, time.Minute, 8, 6)

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge must not fail the tenant tick: %v", err)
	}
	summaryL0 := layout.LogsTierDir(dataDir, tenant, "logs-summary", 0)
	got, err := countDirParquet(summaryL0)
	if err != nil {
		t.Fatal(err)
	}
	if got < 1 {
		t.Fatalf("logs-summary must still refresh after logs-raw error, L0=%d", got)
	}
}
