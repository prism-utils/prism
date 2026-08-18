//go:build e2e

package e2e_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func compactComposeUp(t *testing.T, dataDir, catchup, compactFile, policyHost string) {
	t.Helper()
	compactComposeDown(t)
	if policyHost == "" {
		policyHost = filepath.Join("..", "..", "deploy", "compact-e2e-empty.yaml")
	}
	cmd := exec.Command("docker", "compose", "-p", compactComposeProject, "-f", compactComposeFile, "up", "-d", "--build", "--wait")
	cmd.Env = append(os.Environ(),
		"COMPACT_DATA_DIR="+dataDir,
		"COMPACT_AGE_CATCHUP="+catchup,
		"COMPACT_FILE="+compactFile,
		"COMPACT_POLICY_FILE="+policyHost,
	)
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		dumpCompactComposeLogs(t)
		compactComposeDown(t)
		t.Fatalf("compose up: %v", err)
	}
}

func compactComposeDown(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", compactComposeProject, "-f", compactComposeFile, "down", "-v", "--remove-orphans")
	cmd.Env = append(os.Environ(), "COMPACT_DATA_DIR=/tmp", "COMPACT_POLICY_FILE="+filepath.Join("..", "..", "deploy", "compact-e2e-empty.yaml"))
	_ = cmd.Run()
}

func dumpCompactComposeLogs(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", compactComposeProject, "-f", compactComposeFile, "logs", "--no-color")
	out, _ := cmd.CombinedOutput()
	t.Logf("compose logs:\n%s", out)
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
