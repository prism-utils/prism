package metrics_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prism-utils/prism/internal/store/metrics"
)

func observeConfig() metrics.Config {
	return metrics.Config{
		Enabled:   true,
		Path:      metrics.DefaultPath,
		PerTenant: true,
		Observe:   true,
	}
}

func observeNames() []string {
	return []string{
		"prism_store_memory_observe",
		"prism_store_cgroup_memory_bytes",
		"prism_store_gomemlimit_bytes",
		"prism_store_duckdb_memory_limit_bytes",
		"prism_store_duckdb_open",
		"prism_store_job_rss_bytes",
		"prism_store_job_cgroup_current_bytes",
		"prism_store_job_heap_alloc_bytes",
	}
}

func TestObserveSeriesAbsentWhenFlagOff(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	body := scrape(t, metrics.New(enabledConfig()))
	assertAbsent(t, body, observeNames()...)
}

func TestObserveInertWhenMetricsDisabled(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	reg := metrics.New(metrics.Config{Enabled: false, Observe: true, Path: metrics.DefaultPath})
	metrics.DuckDBOpen(metrics.RoleEngine)
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("scrape status = %d, want 404 when metrics are off", rec.Code)
	}
	if rec.Body.Len() > 0 && strings.Contains(rec.Body.String(), "prism_store_memory_observe") {
		t.Fatalf("disabled scrape leaked observe series: %s", rec.Body.String())
	}
}

func TestObserveSeriesPresentWhenFlagOn(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	cfg := observeConfig()
	cfg.GoMemLimitBytes = 1638_000_000
	cfg.DuckDBMemoryLimitBytes = 1433 << 20
	dir := t.TempDir()
	writeCgroupFiles(t, dir, map[string]string{
		"memory.current": "1000",
		"memory.peak":    "2000",
		"memory.max":     "3000",
	})
	cfg.CgroupRoot = dir
	reg := metrics.New(cfg)
	reg.ObserveTickStart("flush")
	reg.ObserveTick("flush", 0, nil)

	body := scrape(t, reg)
	assertContains(t, body,
		"prism_store_memory_observe 1",
		`prism_store_cgroup_memory_bytes{kind="current"} 1000`,
		`prism_store_cgroup_memory_bytes{kind="peak"} 2000`,
		`prism_store_cgroup_memory_bytes{kind="max"} 3000`,
		`prism_store_duckdb_open{role="engine"}`,
		`prism_store_job_rss_bytes{job="flush",phase="start"}`,
		`prism_store_job_rss_bytes{job="flush",phase="end"}`,
		`prism_store_job_heap_alloc_bytes{job="flush",phase="start"}`,
		`prism_store_job_heap_alloc_bytes{job="flush",phase="end"}`,
		`prism_store_job_cgroup_current_bytes{job="flush",phase="start"} 1000`,
		`prism_store_job_cgroup_current_bytes{job="flush",phase="end"} 1000`,
	)
	if v := gaugeValue(t, body, "prism_store_gomemlimit_bytes"); v != 1638_000_000 {
		t.Fatalf("gomemlimit = %v, want 1638000000", v)
	}
	if v := gaugeValue(t, body, "prism_store_duckdb_memory_limit_bytes"); v != float64(1433<<20) {
		t.Fatalf("duckdb memory limit = %v, want %v", v, 1433<<20)
	}
}

func TestCgroupKindsOmittedWhenFilesMissing(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	cfg := observeConfig()
	cfg.CgroupRoot = t.TempDir()
	body := scrape(t, metrics.New(cfg))
	assertContains(t, body, "prism_store_memory_observe 1")
	assertAbsent(t, body, "prism_store_cgroup_memory_bytes")
}

func TestCgroupMaxUnlimitedIsOmitted(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	cfg := observeConfig()
	dir := t.TempDir()
	writeCgroupFiles(t, dir, map[string]string{
		"memory.current": "11",
		"memory.max":     "max",
	})
	cfg.CgroupRoot = dir
	body := scrape(t, metrics.New(cfg))
	assertContains(t, body, `prism_store_cgroup_memory_bytes{kind="current"} 11`)
	if strings.Contains(body, `kind="max"`) {
		t.Fatal("unlimited memory.max must not export kind=max")
	}
}

func TestDuckDBOpenGaugeMovesAndUnknownRoleIsIgnored(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	reg := metrics.New(observeConfig())

	metrics.DuckDBOpen(metrics.RoleEngine)
	metrics.DuckDBOpen(metrics.RoleEngine)
	metrics.DuckDBOpen(metrics.RoleMerge)
	metrics.DuckDBOpen("not-a-role")

	body := scrape(t, reg)
	if v := gaugeValue(t, body, `prism_store_duckdb_open{role="engine"}`); v != 2 {
		t.Fatalf("engine open = %v, want 2", v)
	}
	if v := gaugeValue(t, body, `prism_store_duckdb_open{role="merge"}`); v != 1 {
		t.Fatalf("merge open = %v, want 1", v)
	}
	if strings.Contains(body, `role="not-a-role"`) {
		t.Fatal("unknown DuckDB role created a series")
	}

	metrics.DuckDBClose(metrics.RoleEngine)
	if v := gaugeValue(t, scrape(t, reg), `prism_store_duckdb_open{role="engine"}`); v != 1 {
		t.Fatalf("engine open after close = %v, want 1", v)
	}
}

func TestDuckDBOpenIsNoopWhenObserveOff(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	reg := metrics.New(enabledConfig())
	metrics.DuckDBOpen(metrics.RoleEngine)
	assertAbsent(t, scrape(t, reg), "prism_store_duckdb_open")
}

func TestUnsetMemoryLimitsExportZero(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	body := scrape(t, metrics.New(observeConfig()))
	assertContains(t, body,
		"prism_store_gomemlimit_bytes 0",
		"prism_store_duckdb_memory_limit_bytes 0",
	)
}

func TestJobMemorySamplesUpdateAfterTick(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	reg := metrics.New(observeConfig())
	reg.ObserveTickStart("merge")
	reg.ObserveTick("merge", 0, nil)
	body := scrape(t, reg)
	startRSS := gaugeValue(t, body, `prism_store_job_rss_bytes{job="merge",phase="start"}`)
	endRSS := gaugeValue(t, body, `prism_store_job_rss_bytes{job="merge",phase="end"}`)
	startHeap := gaugeValue(t, body, `prism_store_job_heap_alloc_bytes{job="merge",phase="start"}`)
	endHeap := gaugeValue(t, body, `prism_store_job_heap_alloc_bytes{job="merge",phase="end"}`)
	if startRSS <= 0 || endRSS <= 0 {
		t.Fatalf("job RSS start=%v end=%v, want > 0", startRSS, endRSS)
	}
	if startHeap <= 0 || endHeap <= 0 {
		t.Fatalf("job heap start=%v end=%v, want > 0", startHeap, endHeap)
	}
}

func TestObserveJobLogOnlyWhenEnabled(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)

	var onBuf bytes.Buffer
	on := metrics.New(observeConfig())
	on.SetLogger(slog.New(slog.NewTextHandler(&onBuf, nil)))
	on.ObserveTickStart("flush")
	on.ObserveTick("flush", 0, errors.New("scan failed"))
	onText := onBuf.String()
	if !strings.Contains(onText, "memory observe job") {
		t.Fatalf("observe-on log missing job line: %q", onText)
	}
	if !strings.Contains(onText, "job=flush") {
		t.Fatalf("observe-on log missing job attr: %q", onText)
	}
	if !strings.Contains(onText, "scan failed") {
		t.Fatalf("observe-on log missing tick error: %q", onText)
	}

	var offBuf bytes.Buffer
	off := metrics.New(enabledConfig())
	off.SetLogger(slog.New(slog.NewTextHandler(&offBuf, nil)))
	off.ObserveTickStart("flush")
	off.ObserveTick("flush", 0, errors.New("scan failed"))
	if strings.Contains(offBuf.String(), "memory observe job") {
		t.Fatalf("observe-off emitted job slog: %q", offBuf.String())
	}
}

func TestObserveJobLogOmitsCgroupAttrWhenUnavailable(t *testing.T) {
	t.Cleanup(metrics.ResetObserveForTest)
	var buf bytes.Buffer
	cfg := observeConfig()
	cfg.CgroupRoot = t.TempDir()
	reg := metrics.New(cfg)
	reg.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	reg.ObserveTickStart("retention")
	reg.ObserveTick("retention", 0, nil)
	text := buf.String()
	if !strings.Contains(text, "memory observe job") {
		t.Fatalf("missing job line: %q", text)
	}
	if strings.Contains(text, "cgroup_current_bytes") {
		t.Fatalf("cgroup attr present without cgroup files: %q", text)
	}
}

func writeCgroupFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
