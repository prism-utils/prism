package lifecycle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func TestTickMergeSkipMarkedSourcesDoNotBlockOtherTenant(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	skipped := "user-skip00001-apps"
	healthy := "user-ok0000002-apps"

	skipDir := layout.TierDir(dataDir, skipped, 0)
	if err := os.MkdirAll(skipDir, 0o750); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		path := filepath.Join(skipDir, string(rune('a'+i))+".parquet")
		testparquet.WriteSegmentWithTs(t, path, now.Add(time.Duration(i)*time.Minute), "up", float64(i))
		if err := os.WriteFile(layout.MergeSkipMarker(path), []byte("attempts=5\nreason=too-large\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	okDir := layout.TierDir(dataDir, healthy, 0)
	if err := os.MkdirAll(okDir, 0o750); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		path := filepath.Join(okDir, string(rune('a'+i))+".parquet")
		testparquet.WriteSegmentWithTs(t, path, now.Add(time.Duration(i)*time.Minute), "up", float64(i))
	}

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:         dataDir,
		SegmentsPerTier: 6,
		MaxSegmentBytes: 1 << 30,
		MaxTier:         8,
	}, eng, func() time.Time { return now })

	if err := runner.TickMerge(); err != nil {
		t.Fatalf("TickMerge: %v", err)
	}

	l1, err := os.ReadDir(layout.TierDir(dataDir, healthy, 1))
	if err != nil {
		t.Fatalf("healthy L1: %v", err)
	}
	var dest int
	for _, e := range l1 {
		if strings.HasSuffix(e.Name(), ".parquet") && !strings.Contains(e.Name(), ".tmp") {
			dest++
		}
	}
	if dest == 0 {
		t.Fatal("healthy tenant should have produced an L1 dest")
	}
	for i := 0; i < 6; i++ {
		path := filepath.Join(skipDir, string(rune('a'+i))+".parquet")
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("skipped source %s should stay: %v", path, err)
		}
	}
}
