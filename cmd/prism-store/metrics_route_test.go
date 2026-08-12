package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/metrics"
	"github.com/prism-utils/prism/internal/store/queue"
)

func metricsFixture(t *testing.T, cfgMetrics metrics.Config) (*serverConfig, *engine.Engine, *slog.Logger) {
	t.Helper()
	dir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	cfg := &serverConfig{
		dataDir:          dir,
		allowedArtifacts: []string{"metrics-raw"},
		authMode:         "none",
		metrics:          cfgMetrics,
		metricsReg:       metrics.New(cfgMetrics),
	}
	return cfg, eng, slog.New(slog.NewTextHandler(io.Discard, nil))
}

func getBody(t *testing.T, mux *http.ServeMux, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func defaultMetricsConfig() metrics.Config {
	return metrics.Config{Enabled: true, Path: metrics.DefaultPath, PerTenant: true}
}

func TestMetricsRouteServedOnBothPlanes(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, defaultMetricsConfig())
	cfg.adminListenAddr = ":9090"

	for name, plane := range map[string]servePlane{"public": planePublic, "admin": planeAdmin} {
		mux := newServeMux(cfg, eng, logger, plane, nil, nil, nil)
		code, body := getBody(t, mux, "/metrics")
		if code != http.StatusOK {
			t.Fatalf("%s plane /metrics = %d, want 200", name, code)
		}
		if !strings.Contains(body, "go_goroutines") || !strings.Contains(body, "process_resident_memory_bytes") {
			t.Fatalf("%s plane /metrics missing runtime collectors", name)
		}
	}
}

func TestMetricsRouteNeedsNoCredential(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, defaultMetricsConfig())
	cfg.adminToken = "admin-tok"

	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, nil)
	if code, _ := getBody(t, mux, "/metrics"); code != http.StatusOK {
		t.Fatalf("/metrics with ADMIN_TOKEN set = %d, want 200 (scrape is unauthenticated)", code)
	}
	if code, _ := getBody(t, mux, "/admin/queue"); code != http.StatusUnauthorized {
		t.Fatalf("/admin/queue = %d, want 401 — the admin plane must stay gated", code)
	}
}

func TestMetricsRouteAbsentWhenDisabled(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, metrics.Config{Enabled: false})

	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, nil)
	if code, _ := getBody(t, mux, "/metrics"); code != http.StatusNotFound {
		t.Fatalf("/metrics with METRICS_ENABLED=false = %d, want 404", code)
	}
	if code, _ := getBody(t, mux, "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200 with the exporter off", code)
	}
}

func TestMetricsRouteHonorsCustomPath(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, metrics.Config{Enabled: true, Path: "/internal/metrics", PerTenant: true})

	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, nil)
	if code, _ := getBody(t, mux, "/internal/metrics"); code != http.StatusOK {
		t.Fatalf("/internal/metrics = %d, want 200", code)
	}
	if code, _ := getBody(t, mux, "/metrics"); code != http.StatusNotFound {
		t.Fatalf("/metrics = %d, want 404 when METRICS_PATH moved the endpoint", code)
	}
}

func TestServedRoutesCarryTheirRouteLabel(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, defaultMetricsConfig())
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 2, MaxQueue: 8, Wait: time.Minute})
	cfg.sqlAPIEnabled = true

	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, lim)
	for _, path := range []string{"/healthz", "/readyz", "/admin/queue"} {
		if code, _ := getBody(t, mux, path); code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", path, code)
		}
	}

	_, body := getBody(t, mux, "/metrics")
	for _, want := range []string{
		`prism_store_http_requests_total{code="200",method="GET",route="healthz"} 1`,
		`prism_store_http_requests_total{code="200",method="GET",route="readyz"} 1`,
		`prism_store_http_requests_total{code="200",method="GET",route="admin_queue"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("exposition missing %q\n%s", want, body)
		}
	}
}

func TestQueryRoutesRecordTenantSeriesWithoutTenantRouteLabels(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, defaultMetricsConfig())
	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, nil)

	const tenant = "user-6f3a9c2b-apps"
	if code, _ := getBody(t, mux, "/"+tenant+"/query?start=0&end=1&step=1s"); code == http.StatusNotFound {
		t.Fatal("combined mux must serve /{ns}/query")
	}

	_, body := getBody(t, mux, "/metrics")
	if want := `prism_store_queries_total{route="query",tenant="` + tenant + `"} 1`; !strings.Contains(body, want) {
		t.Fatalf("exposition missing %q\n%s", want, body)
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "prism_store_http_requests_total") && strings.Contains(line, tenant) {
			t.Fatalf("HTTP route label leaked the tenant id: %s", line)
		}
	}
}

func TestQueueMetricsWiredToTheServedLimiter(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, defaultMetricsConfig())
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 3, MaxQueue: 64, Wait: time.Minute})
	if err := cfg.metricsReg.SetQueueSource(lim); err != nil {
		t.Fatalf("SetQueueSource: %v", err)
	}
	if err := cfg.metricsReg.SetEngineSource(eng); err != nil {
		t.Fatalf("SetEngineSource: %v", err)
	}

	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, lim)
	_, body := getBody(t, mux, "/metrics")
	for _, want := range []string{
		"prism_store_queue_max_in_flight 3",
		"prism_store_queue_max_queue 64",
		"prism_store_engine_max_open_tenants",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("exposition missing %q", want)
		}
	}
}

func TestMetricsConfigDefaultsOn(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	cfg := loadConfig()

	if !cfg.metrics.Enabled {
		t.Fatal("METRICS_ENABLED default = false, want true")
	}
	if !cfg.metrics.PerTenant {
		t.Fatal("METRICS_PER_TENANT default = false, want true")
	}
	if cfg.metrics.Path != metrics.DefaultPath {
		t.Fatalf("METRICS_PATH default = %q, want %q", cfg.metrics.Path, metrics.DefaultPath)
	}
}

func TestMetricsConfigReadsEnvOverrides(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("METRICS_ENABLED", "false")
	t.Setenv("METRICS_PER_TENANT", "false")
	t.Setenv("METRICS_PATH", "/internal/metrics")
	cfg := loadConfig()

	if cfg.metrics.Enabled {
		t.Fatal("METRICS_ENABLED=false was not honored")
	}
	if cfg.metrics.PerTenant {
		t.Fatal("METRICS_PER_TENANT=false was not honored")
	}
	if cfg.metrics.Path != "/internal/metrics" {
		t.Fatalf("METRICS_PATH = %q, want /internal/metrics", cfg.metrics.Path)
	}
}

// TestQueryREDMetricsCountAuthzRejects proves InstrumentQuery sits outermost on
// the SQL/PromQL/Loki chains: a bearer reject never reaches the handler yet
// still increments the query RED counter under the matching api label.
func TestQueryREDMetricsCountAuthzRejects(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, defaultMetricsConfig())
	cfg.adminToken = "admin-tok"
	cfg.sqlAPIEnabled = true
	cfg.promqlAPIEnabled = true
	cfg.lokiAPIEnabled = true
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 2, MaxQueue: 8, Wait: time.Minute})

	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, lim)
	const tenant = "user-6f3a9c2b-apps"
	for _, path := range []string{
		"/" + tenant + "/sql",
		"/" + tenant + "/api/v1/query?query=up",
		"/" + tenant + "/loki/api/v1/labels",
	} {
		method := http.MethodGet
		if strings.HasSuffix(path, "/sql") {
			method = http.MethodPost
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), method, path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s = %d, want 401", path, rec.Code)
		}
	}

	_, body := getBody(t, mux, "/metrics")
	for _, want := range []string{
		`prism_store_query_requests_total{api="sql",code="401",tenant="` + tenant + `"} 1`,
		`prism_store_query_requests_total{api="promql",code="401",tenant="` + tenant + `"} 1`,
		`prism_store_query_requests_total{api="loki",code="401",tenant="` + tenant + `"} 1`,
		`prism_store_query_duration_seconds_count{api="sql",tenant="` + tenant + `"} 1`,
		`prism_store_query_duration_seconds_count{api="promql",tenant="` + tenant + `"} 1`,
		`prism_store_query_duration_seconds_count{api="loki",tenant="` + tenant + `"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("exposition missing %q\n%s", want, body)
		}
	}
}

// TestQueryREDMetricsAbsentWhenDisabled pins that turning the exporter off
// leaves query RED series out of the scrape surface entirely.
func TestQueryREDMetricsAbsentWhenDisabled(t *testing.T) {
	cfg, eng, logger := metricsFixture(t, metrics.Config{Enabled: false})
	cfg.sqlAPIEnabled = true
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: false})
	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, lim)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/user-6f3a9c2b-apps/sql", nil))
	if rec.Code == http.StatusNotFound {
		t.Fatal("SQL route missing on combined mux")
	}

	code, body := getBody(t, mux, "/metrics")
	if code != http.StatusNotFound {
		t.Fatalf("/metrics = %d, want 404", code)
	}
	if strings.Contains(body, "prism_store_query_requests_total") {
		t.Fatalf("disabled scrape leaked query RED series: %s", body)
	}
}
