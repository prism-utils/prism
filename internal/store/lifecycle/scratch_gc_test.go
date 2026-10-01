package lifecycle

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
)

func TestGCContinuesAfterTenantError(t *testing.T) {
	dataDir := t.TempDir()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	grace := 120 * time.Second

	bad := "user-gcerr01-apps"
	good := "user-gcerr02-apps"
	badHot := filepath.Join(dataDir, bad, "hot")
	if err := os.MkdirAll(filepath.Dir(badHot), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(badHot, []byte("not-a-dir"), 0o640); err != nil {
		t.Fatal(err)
	}

	goodTmp := filepath.Join(dataDir, good, "hot", "current.duckdb.deadbeef.tmp")
	goodLive := filepath.Join(dataDir, good, "hot", "current.duckdb")
	if err := os.MkdirAll(filepath.Dir(goodTmp), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(goodTmp, []byte("scratch"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(goodLive, []byte("live"), 0o600); err != nil {
		t.Fatal(err)
	}
	stale := now.Add(-grace - time.Second)
	if err := os.Chtimes(goodTmp, stale, stale); err != nil {
		t.Fatal(err)
	}

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })
	runner := NewRunner(&Config{
		DataDir:       dataDir,
		RetentionDays: 15,
		MaxTier:       8,
		DeleteGrace:   grace,
	}, eng, func() time.Time { return now })

	if err := runner.TickRetention(); err != nil {
		t.Fatalf("TickRetention must continue after bad tenant: %v", err)
	}
	if _, err := os.Stat(goodTmp); !os.IsNotExist(err) {
		t.Fatalf("good tenant scratch still present, stat err = %v", err)
	}
	if _, err := os.Stat(goodLive); err != nil {
		t.Fatalf("good tenant live snapshot missing: %v", err)
	}
	if _, err := os.Stat(badHot); err != nil {
		t.Fatalf("bad tenant path missing: %v", err)
	}
}
