//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/testparquet"
	"github.com/stretchr/testify/require"
)

const (
	catalogComposeFile    = "../../deploy/docker-compose.merge-catalog.yml"
	catalogComposeProject = "prism-merge-catalog"
	catalogStoreBase      = "http://127.0.0.1:19095"
	catalogTenant         = "cataloge2e"
)

func TestMergeCatalogIdleHitsPackAndCorruptRebuild(t *testing.T) {
	requireDocker(t)
	dataDir := t.TempDir()
	coldDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o777))
	require.NoError(t, os.Chmod(coldDir, 0o777))
	catalogComposeUp(t, dataDir, coldDir)
	t.Cleanup(func() {
		if t.Failed() {
			dumpCatalogComposeLogs(t)
		}
		catalogComposeDown(t)
	})

	now := time.Now().UTC()
	l0 := filepath.Join(dataDir, catalogTenant, "tiers", "L0")
	testparquet.WriteSegmentWithTs(t, filepath.Join(l0, "seed-0.parquet"), now.Add(-2*time.Minute), "up", 1)
	require.NoError(t, chmodTree(dataDir, 0o777))

	var hitsAfterFill float64
	require.Eventually(t, func() bool {
		body, ok := scrapeCatalogMetrics(t)
		if !ok {
			return false
		}
		hits := metricValueOrZero(body, `prism_store_catalog_lookup_total{plane="metrics",result="hit"}`)
		misses := metricValueOrZero(body, `prism_store_catalog_lookup_total{plane="metrics",result="miss"}`)
		if misses < 1 && hits < 1 {
			return false
		}
		if catalogRebuildTotal(body) != 0 {
			return false
		}
		if hits > 0 {
			hitsAfterFill = hits
			return true
		}
		return false
	}, 45*time.Second, 500*time.Millisecond, "catalog never filled from the seeded L0")

	fileCount := countLiveParquet(t, l0)
	require.Greater(t, fileCount, 0)

	var idleHits, idleMisses, idleRebuild, idleStatOpens float64
	require.Eventually(t, func() bool {
		body, ok := scrapeCatalogMetrics(t)
		if !ok {
			return false
		}
		hits := metricValueOrZero(body, `prism_store_catalog_lookup_total{plane="metrics",result="hit"}`)
		if hits <= hitsAfterFill {
			return false
		}
		idleHits = hits
		idleMisses = metricValueOrZero(body, `prism_store_catalog_lookup_total{plane="metrics",result="miss"}`)
		idleRebuild = catalogRebuildTotal(body)
		idleStatOpens = metricValueOrZero(body, `prism_store_duckdb_opens_total{role="stat"}`)
		return true
	}, 20*time.Second, 500*time.Millisecond, "idle ticks never added catalog hits")
	require.Equal(t, float64(0), idleRebuild, "idle rebuild_total=%v", idleRebuild)

	body, ok := scrapeCatalogMetrics(t)
	require.True(t, ok)
	hits2 := metricValueOrZero(body, `prism_store_catalog_lookup_total{plane="metrics",result="hit"}`)
	misses2 := metricValueOrZero(body, `prism_store_catalog_lookup_total{plane="metrics",result="miss"}`)
	stat2 := metricValueOrZero(body, `prism_store_duckdb_opens_total{role="stat"}`)
	require.GreaterOrEqual(t, hits2, idleHits)
	require.Less(t, misses2-idleMisses, float64(fileCount), "idle misses grew ~1:1 with file count")
	if stat2 > 0 && idleStatOpens > 0 {
		require.Less(t, stat2-idleStatOpens, float64(fileCount), "stat-role duckdb opens grew ~1:1 with file count")
	}

	for i := 1; i <= 2; i++ {
		testparquet.WriteSegmentWithTs(t, filepath.Join(l0, fmt.Sprintf("seed-%d.parquet", i)), now.Add(time.Duration(i)*time.Minute), "up", float64(i+1))
	}
	require.NoError(t, chmodTree(dataDir, 0o777))

	l1 := filepath.Join(dataDir, catalogTenant, "tiers", "L1")
	require.Eventually(t, func() bool {
		return countParquet(t, l1) >= 1
	}, 60*time.Second, 500*time.Millisecond, "pack never created L1")

	require.Eventually(t, func() bool {
		man := filepath.Join(dataDir, catalogTenant, "tiers", "_manifest.json")
		b, err := os.ReadFile(man)
		if err != nil {
			return false
		}
		s := string(b)
		if !strings.Contains(s, "tiers/L1/") {
			return false
		}
		dropped := 0
		for i := 0; i <= 2; i++ {
			if !strings.Contains(s, fmt.Sprintf("seed-%d.parquet", i)) {
				dropped++
			}
		}
		// SEGMENTS_PER_TIER=2 packs two of three L0s; at least those sources leave.
		return dropped >= 2
	}, 30*time.Second, 500*time.Millisecond, "catalog did not keep dest and drop packed sources")

	rebuildBefore := catalogRebuildTotal(mustScrapeCatalog(t))
	man := filepath.Join(dataDir, catalogTenant, "tiers", "_manifest.json")
	require.NoError(t, os.WriteFile(man, []byte("{corrupt"), 0o666))

	require.Eventually(t, func() bool {
		body, ok := scrapeCatalogMetrics(t)
		if !ok {
			return false
		}
		if catalogRebuildTotal(body) < rebuildBefore+1 {
			return false
		}
		logs := catalogComposeLogs(t)
		return strings.Contains(logs, "catalog full rebuild")
	}, 30*time.Second, 500*time.Millisecond, "corrupt JSON never triggered one rebuild")

	afterCorrupt := catalogRebuildTotal(mustScrapeCatalog(t))
	require.Eventually(t, func() bool {
		body, ok := scrapeCatalogMetrics(t)
		if !ok {
			return false
		}
		hits := metricValueOrZero(body, `prism_store_catalog_lookup_total{plane="metrics",result="hit"}`)
		return hits > idleHits
	}, 20*time.Second, 500*time.Millisecond, "post-rebuild ticks never hit")
	require.Equal(t, afterCorrupt, catalogRebuildTotal(mustScrapeCatalog(t)), "later idle ticks incremented rebuild_total")
}

func catalogComposeUp(t *testing.T, dataDir, coldDir string) {
	t.Helper()
	catalogComposeDown(t)
	cmd := exec.Command("docker", "compose", "-p", catalogComposeProject, "-f", catalogComposeFile, "up", "-d", "--build", "--wait")
	cmd.Env = append(os.Environ(), catalogComposeEnv(dataDir, coldDir)...)
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		dumpCatalogComposeLogs(t)
		catalogComposeDown(t)
		t.Fatalf("compose up: %v", err)
	}
}

func catalogComposeDown(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", catalogComposeProject, "-f", catalogComposeFile, "down", "-v", "--remove-orphans")
	cmd.Env = append(os.Environ(), catalogComposeEnv(os.TempDir(), os.TempDir())...)
	_ = cmd.Run()
}

func catalogComposeEnv(dataDir, coldDir string) []string {
	if dataDir == "" {
		dataDir = os.TempDir()
	}
	if coldDir == "" {
		coldDir = os.TempDir()
	}
	return []string{
		"CATALOG_DATA_DIR=" + dataDir,
		"CATALOG_COLD_DIR=" + coldDir,
		"CATALOG_UID=" + strconv.Itoa(os.Getuid()),
		"CATALOG_GID=" + strconv.Itoa(os.Getgid()),
	}
}

func dumpCatalogComposeLogs(t *testing.T) {
	t.Helper()
	t.Logf("store logs:\n%s", catalogComposeLogs(t))
}

func catalogComposeLogs(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", catalogComposeProject, "-f", catalogComposeFile, "logs", "--no-color", "--tail", "200")
	cmd.Env = append(os.Environ(), catalogComposeEnv(os.TempDir(), os.TempDir())...)
	out, _ := cmd.CombinedOutput()
	return string(out)
}

func scrapeCatalogMetrics(t *testing.T) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, catalogStoreBase+"/metrics", nil)
	if err != nil {
		return "", false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return "", false
	}
	return string(b), true
}

func mustScrapeCatalog(t *testing.T) string {
	t.Helper()
	body, ok := scrapeCatalogMetrics(t)
	require.True(t, ok, "scrape /metrics")
	return body
}

func catalogRebuildTotal(body string) float64 {
	return metricValueOrZero(body, `prism_store_catalog_rebuild_total{plane="metrics",reason="corrupt"}`)
}

func metricValueOrZero(body, series string) float64 {
	for _, line := range strings.Split(body, "\n") {
		rest, ok := strings.CutPrefix(line, series+" ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			return 0
		}
		return v
	}
	return 0
}
