//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/testparquet"
)

var (
	storeBinOnce sync.Once
	storeBinPath string
	storeBinErr  error
)

func TestMemoryObserveOnExportsSeriesAfterIngest(t *testing.T) {
	bin := prismStoreBinary(t)
	base := startPrismStore(t, bin, map[string]string{
		"MEMORY_OBSERVE":  "true",
		"METRICS_ENABLED": "true",
		"RUN_JOBS":        "false",
	})
	ingestObserveMetricsWindow(t, base, "observee2e")
	body := scrapeStoreMetrics(t, base)
	if !strings.Contains(body, "prism_store_memory_observe 1") {
		t.Fatalf("observe-on scrape missing memory_observe:\n%s", body)
	}
	for _, name := range []string{
		"prism_store_gomemlimit_bytes",
		"prism_store_duckdb_memory_limit_bytes",
		"prism_store_duckdb_open",
	} {
		if !strings.Contains(body, name) {
			t.Fatalf("observe-on scrape missing %s:\n%s", name, body)
		}
	}
	engineOpen := metricGauge(t, body, `prism_store_duckdb_open{role="engine"}`)
	if engineOpen < 1 {
		t.Fatalf("duckdb_open engine = %v, want >= 1 after ingest", engineOpen)
	}
}

func TestMemoryObserveOffOmitsNewFamilies(t *testing.T) {
	bin := prismStoreBinary(t)
	base := startPrismStore(t, bin, map[string]string{
		"METRICS_ENABLED": "true",
		"RUN_JOBS":        "false",
	})
	ingestObserveMetricsWindow(t, base, "observeoff")
	body := scrapeStoreMetrics(t, base)
	for _, name := range []string{
		"prism_store_memory_observe",
		"prism_store_cgroup_memory_bytes",
		"prism_store_gomemlimit_bytes",
		"prism_store_duckdb_memory_limit_bytes",
		"prism_store_duckdb_open",
		"prism_store_job_rss_bytes",
		"prism_store_job_cgroup_current_bytes",
		"prism_store_job_heap_alloc_bytes",
	} {
		if strings.Contains(body, name) {
			t.Fatalf("observe-off scrape contains %s:\n%s", name, body)
		}
	}
	if !strings.Contains(body, "process_resident_memory_bytes") {
		t.Fatal("observe-off scrape dropped the process collector")
	}
}

func prismStoreBinary(t *testing.T) string {
	t.Helper()
	storeBinOnce.Do(func() {
		root, err := filepath.Abs("../..")
		if err != nil {
			storeBinErr = err
			return
		}
		out := filepath.Join(os.TempDir(), "prism-store-memory-observe-e2e")
		cmd := exec.Command("go", "build", "-tags", "duckdb_arrow", "-o", out, "./cmd/prism-store")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
		var buf strings.Builder
		cmd.Stdout = &buf
		cmd.Stderr = &buf
		storeBinErr = cmd.Run()
		if storeBinErr != nil {
			storeBinErr = fmt.Errorf("build prism-store: %w\n%s", storeBinErr, buf.String())
			return
		}
		storeBinPath = out
	})
	if storeBinErr != nil {
		t.Fatal(storeBinErr)
	}
	return storeBinPath
}

func startPrismStore(t *testing.T, bin string, extraEnv map[string]string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"DATA_DIR="+dataDir,
		"LISTEN_ADDR="+addr,
		"AUTH_MODE=none",
		"ALLOWED_ARTIFACTS=metrics-raw",
	)
	for k, v := range extraEnv {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var logs strings.Builder
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start prism-store: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if t.Failed() {
			t.Logf("prism-store logs:\n%s", logs.String())
		}
	})

	base := "http://" + addr
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/healthz", nil)
		resp, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return base
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("prism-store never became healthy at %s\n%s", base, logs.String())
	return ""
}

func ingestObserveMetricsWindow(t *testing.T, base, tenant string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "window.parquet")
	testparquet.WriteFile(t, path, []testparquet.Row{{
		Name:        "up",
		Labels:      `{"job":"e2e"}`,
		Value:       1,
		TimestampMs: time.Now().UnixMilli(),
	}})
	body, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/"+tenant+"/ingest/metrics-raw", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest status = %d body %s", resp.StatusCode, b)
	}
}

func scrapeStoreMetrics(t *testing.T, base string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scrape status = %d body %s", resp.StatusCode, b)
	}
	return string(b)
}

func metricGauge(t *testing.T, body, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		rest, ok := strings.CutPrefix(line, series+" ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return v
	}
	t.Fatalf("series %q not found", series)
	return 0
}
