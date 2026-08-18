//go:build e2e

package e2e_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/testparquet"
	"github.com/stretchr/testify/require"
)

const (
	compactComposeFile    = "../../deploy/docker-compose.compact-lifecycle.yml"
	compactComposeProject = "prism-compact-lifecycle"
	compactTenant         = "compacte2e"
)

func TestCompactCatchupAgedL0ToL1(t *testing.T) {
	requireDocker(t)
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o777))
	compactComposeUp(t, dataDir, "true", "", "")
	t.Cleanup(func() {
		if t.Failed() {
			dumpCompactComposeLogs(t)
		}
		compactComposeDown(t)
	})

	aged := time.Now().UTC().Add(-20 * time.Minute)
	l0 := filepath.Join(dataDir, compactTenant, "tiers", "L0")
	for i := 0; i < 6; i++ {
		p := filepath.Join(l0, fmt.Sprintf("a%d.parquet", i))
		testparquet.WriteSegmentWithTs(t, p, aged.Add(time.Duration(i)*time.Minute), "up", float64(i+1))
	}
	require.NoError(t, chmodTree(dataDir, 0o777))

	require.Eventually(t, func() bool {
		return countParquet(t, filepath.Join(dataDir, compactTenant, "tiers", "L1")) >= 1 &&
			(countSidecars(t, l0) >= 2 || countLiveParquet(t, l0) < 6)
	}, 60*time.Second, 2*time.Second, "catch-up never packed aged L0s into L1")
}

func TestCompactBucketDayPacksOldestDayFirst(t *testing.T) {
	requireDocker(t)
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o777))
	policy := filepath.Join(t.TempDir(), "compact.yaml")
	body := []byte(`
compact:
  policies:
    - name: daily
      tier: 0
      bucket: day
      olderThan: 15m
      maxSources: 64
      maxBytes: 512Mi
      every: 1s
`)
	require.NoError(t, os.WriteFile(policy, body, 0o644))
	compactComposeUp(t, dataDir, "false", "/etc/prism/compact.yaml", policy)
	t.Cleanup(func() {
		if t.Failed() {
			dumpCompactComposeLogs(t)
		}
		compactComposeDown(t)
	})

	now := time.Now().UTC()
	day0 := now.Add(-48 * time.Hour).Truncate(24 * time.Hour).Add(12 * time.Hour)
	day1 := now.Add(-24 * time.Hour).Truncate(24 * time.Hour).Add(12 * time.Hour)
	l0 := filepath.Join(dataDir, compactTenant, "tiers", "L0")
	for i := 0; i < 3; i++ {
		testparquet.WriteSegmentWithTs(t, filepath.Join(l0, fmt.Sprintf("d0-%d.parquet", i)),
			day0.Add(time.Duration(i)*time.Minute), "up", float64(i+1))
		testparquet.WriteSegmentWithTs(t, filepath.Join(l0, fmt.Sprintf("d1-%d.parquet", i)),
			day1.Add(time.Duration(i)*time.Minute), "up", float64(i+10))
	}
	require.NoError(t, chmodTree(dataDir, 0o777))

	require.Eventually(t, func() bool {
		if countParquet(t, filepath.Join(dataDir, compactTenant, "tiers", "L1")) < 1 {
			return false
		}
		d0Compacted := 0
		d1Live := 0
		entries, err := os.ReadDir(l0)
		if err != nil {
			return false
		}
		for _, e := range entries {
			name := e.Name()
			if !strings.HasSuffix(name, ".parquet") {
				continue
			}
			if _, err := os.Stat(filepath.Join(l0, name+".compacted")); err == nil {
				if strings.HasPrefix(name, "d0-") {
					d0Compacted++
				}
				continue
			}
			if strings.HasPrefix(name, "d1-") {
				d1Live++
			}
		}
		return d0Compacted >= 2 && d1Live >= 2
	}, 60*time.Second, 2*time.Second, "bucket=day must pack the oldest UTC day first")
}

func TestCompactSkipsOversizedMiddleL0(t *testing.T) {
	requireDocker(t)
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o777))
	policy := filepath.Join(t.TempDir(), "compact.yaml")
	body := []byte(`
compact:
  policies:
    - name: skippy
      tier: 0
      olderThan: 15m
      maxSources: 32
      maxBytes: 16KiB
      every: 1s
      bucket: none
`)
	require.NoError(t, os.WriteFile(policy, body, 0o644))
	compactComposeUp(t, dataDir, "false", "/etc/prism/compact.yaml", policy)
	t.Cleanup(func() {
		if t.Failed() {
			dumpCompactComposeLogs(t)
		}
		compactComposeDown(t)
	})

	aged := time.Now().UTC().Add(-20 * time.Minute)
	l0 := filepath.Join(dataDir, compactTenant, "tiers", "L0")
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0, "small-a.parquet"), aged, "up", 1)
	writeWideAgedParquet(t, filepath.Join(l0, "large.parquet"), aged.Add(time.Minute), 8000)
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0, "small-b.parquet"), aged.Add(2*time.Minute), "up", 2)
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0, "small-c.parquet"), aged.Add(3*time.Minute), "up", 3)
	require.NoError(t, chmodTree(dataDir, 0o777))

	largeInfo, err := os.Stat(filepath.Join(l0, "large.parquet"))
	require.NoError(t, err)
	require.Greater(t, largeInfo.Size(), int64(16<<10), "fixture large.parquet must exceed policy maxBytes")

	require.Eventually(t, func() bool {
		if countParquet(t, filepath.Join(dataDir, compactTenant, "tiers", "L1")) < 1 {
			return false
		}
		if _, err := os.Stat(filepath.Join(l0, "large.parquet.compacted")); err == nil {
			return false
		}
		if _, err := os.Stat(filepath.Join(l0, "large.parquet")); err != nil {
			return false
		}
		return countSidecars(t, l0) >= 2
	}, 60*time.Second, 2*time.Second, "small L0s must compact; oversized middle file must stay live")
}

func compactComposeUp(t *testing.T, dataDir, catchup, compactFile, policyHost string) {
	t.Helper()
	compactComposeDown(t)
	if policyHost == "" {
		policyHost = filepath.Join("..", "..", "deploy", "compact-e2e-empty.yaml")
	}
	cmd := exec.Command("docker", "compose", "-p", compactComposeProject, "-f", compactComposeFile, "up", "-d", "--build", "--wait")
	cmd.Env = append(os.Environ(), compactComposeEnv(dataDir, catchup, compactFile, policyHost)...)
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		dumpCompactComposeLogs(t)
		compactComposeDown(t)
		t.Fatalf("compose up: %v", err)
	}
}

func compactComposeEnv(dataDir, catchup, compactFile, policyHost string) []string {
	if policyHost == "" {
		policyHost = filepath.Join("..", "..", "deploy", "compact-e2e-empty.yaml")
	}
	if dataDir == "" {
		dataDir = os.TempDir()
	}
	return []string{
		"COMPACT_DATA_DIR=" + dataDir,
		"COMPACT_AGE_CATCHUP=" + catchup,
		"COMPACT_FILE=" + compactFile,
		"COMPACT_POLICY_FILE=" + policyHost,
		"COMPACT_UID=" + strconv.Itoa(os.Getuid()),
		"COMPACT_GID=" + strconv.Itoa(os.Getgid()),
	}
}

func compactComposeDown(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", compactComposeProject, "-f", compactComposeFile, "down", "-v", "--remove-orphans")
	cmd.Env = append(os.Environ(), compactComposeEnv(os.TempDir(), "true", "", "")...)
	_ = cmd.Run()
}

func dumpCompactComposeLogs(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "logs", "--tail", "200", "prism-compact-lifecycle-prism-store-1")
	out, _ := cmd.CombinedOutput()
	t.Logf("store logs:\n%s", out)
}

func countParquet(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".parquet") && !strings.Contains(e.Name(), ".tmp") {
			n++
		}
	}
	return n
}

func countLiveParquet(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".parquet") {
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, name+".compacted")); err == nil {
			continue
		}
		n++
	}
	return n
}

func countSidecars(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".compacted") {
			n++
		}
	}
	return n
}

func chmodTree(root string, mode os.FileMode) error {
	return filepath.WalkDir(root, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chmod(path, mode)
	})
}

func writeWideAgedParquet(t *testing.T, path string, ts time.Time, rows int) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o777))
	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	tsStr := ts.UTC().Format("2006-01-02 15:04:05.999999")
	tmp := path + ".tmp"
	q := fmt.Sprintf(`
		COPY (
			SELECT 'up' AS "__name__", '{}' AS labels, 1.0 AS value, 0 AS timestamp_ms,
			       CAST('%s' AS TIMESTAMP) AS ts
			FROM range(%d)
		) TO '%s' (FORMAT parquet)
	`, tsStr, rows, filepath.ToSlash(tmp))
	_, err = db.ExecContext(context.Background(), q)
	require.NoError(t, err)
	require.NoError(t, os.Rename(tmp, path))
}
