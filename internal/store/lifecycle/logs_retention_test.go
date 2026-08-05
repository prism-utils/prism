package lifecycle

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const logsRetentionTenant = "user-logret01-apps"

func TestLogsRetentionEnforcesFileCapAndMaxAge(t *testing.T) {
	dataDir := t.TempDir()
	tenant := logsRetentionTenant
	artifact := "logs-raw"
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		t.Fatal(err)
	}

	const maxFiles = 5
	const n = maxFiles + 5
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		at := now.Add(-time.Duration(n-i) * 24 * time.Hour)
		name := layout.SegmentName(at)
		testparquet.WriteLogsRawFile(t, filepath.Join(landing, name), []testparquet.LogRow{
			{Message: "x", Format: "none"},
		})
	}

	// Metrics tier file must survive log retention.
	metricsL0 := layout.TierDir(dataDir, tenant, 0)
	if err := os.MkdirAll(metricsL0, 0o750); err != nil {
		t.Fatal(err)
	}
	metricsPath := filepath.Join(metricsL0, "metrics-keep.parquet")
	testparquet.WriteSegmentWithTs(t, metricsPath, now.Add(-1*24*time.Hour), "up", 1)

	eng := engine.New(engine.Config{DataDir: dataDir}, func() time.Time { return now })
	t.Cleanup(func() { _ = eng.Close() })

	runner := NewRunner(&Config{
		DataDir:       dataDir,
		RetentionDays: 15,
		MaxLogFiles:   maxFiles,
		MaxTier:       8,
	}, eng, func() time.Time { return now })

	if err := runner.TickRetention(); err != nil {
		t.Fatalf("TickRetention: %v", err)
	}

	logCount, err := countLogParquet(dataDir, tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	if logCount > maxFiles {
		t.Fatalf("log file count = %d, want <= %d", logCount, maxFiles)
	}

	cutoff := now.Add(-15 * 24 * time.Hour)
	segs, err := listLogSegments(dataDir, tenant, artifact)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range segs {
		if s.MaxTs.Before(cutoff) {
			t.Fatalf("file older than retention still present: %s maxTs=%v", s.Path, s.MaxTs)
		}
	}

	if _, err := os.Stat(metricsPath); err != nil {
		t.Fatalf("metrics tier file must be untouched: %v", err)
	}
}

func countLogParquet(dataDir, tenant, artifact string) (int, error) {
	n := 0
	landing := layout.LogsLandingDir(dataDir, tenant, artifact)
	c, err := countDirParquet(landing)
	if err != nil {
		return 0, err
	}
	n += c
	for tier := 0; tier <= 8; tier++ {
		c, err := countDirParquet(layout.LogsTierDir(dataDir, tenant, artifact, tier))
		if err != nil {
			return 0, err
		}
		n += c
	}
	return n, nil
}

func countDirParquet(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".parquet" && e.Name()[0] != '.' {
			n++
		}
	}
	return n, nil
}

type logSeg struct {
	Path  string
	MaxTs time.Time
}

func listLogSegments(dataDir, tenant, artifact string) ([]logSeg, error) {
	var out []logSeg
	dirs := []string{layout.LogsLandingDir(dataDir, tenant, artifact)}
	for tier := 0; tier <= 8; tier++ {
		dirs = append(dirs, layout.LogsTierDir(dataDir, tenant, artifact, tier))
	}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() || filepath.Ext(e.Name()) != ".parquet" || e.Name()[0] == '.' {
				continue
			}
			path := filepath.Join(dir, e.Name())
			if ns, ok := layout.WindowIDNanos(path); ok {
				ts := time.Unix(0, ns).UTC()
				out = append(out, logSeg{Path: path, MaxTs: ts})
			}
		}
	}
	return out, nil
}
