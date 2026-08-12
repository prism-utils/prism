package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/prism-utils/prism/internal/store/metrics"
)

func enabledConfig() metrics.Config {
	return metrics.Config{Enabled: true, Path: metrics.DefaultPath, PerTenant: true}
}

// scrape renders the exporter the way a Prometheus server would and returns the
// text exposition body.
func scrape(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

func assertContains(t *testing.T, body string, names ...string) {
	t.Helper()
	for _, name := range names {
		if !strings.Contains(body, name) {
			t.Fatalf("exposition missing %q", name)
		}
	}
}

func assertAbsent(t *testing.T, body string, names ...string) {
	t.Helper()
	for _, name := range names {
		if strings.Contains(body, name) {
			t.Fatalf("exposition unexpectedly contains %q", name)
		}
	}
}

// gaugeValue pulls the sample value of one fully-qualified series out of a text
// exposition body.
func gaugeValue(t *testing.T, body, series string) float64 {
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
	t.Fatalf("series %q not found in exposition", series)
	return 0
}

func TestScrapeExposesGoAndProcessCollectors(t *testing.T) {
	body := scrape(t, metrics.New(enabledConfig()))
	assertContains(t, body,
		"go_goroutines",
		"go_memstats_alloc_bytes",
		"process_resident_memory_bytes",
		"process_open_fds",
		"process_cpu_seconds_total",
	)
}

func TestScrapeUsesPrivateRegistryNotTheGlobalDefault(t *testing.T) {
	// A collector on the global default registry must never appear on the
	// store's scrape output; leaking it would make test runs order-dependent.
	body := scrape(t, metrics.New(enabledConfig()))
	if strings.Contains(body, "promhttp_metric_handler_requests_total") {
		t.Fatal("exposition includes the default registry's handler metrics")
	}
}

func TestDisabledRegistryServesNoExposition(t *testing.T) {
	reg := metrics.New(metrics.Config{Enabled: false})
	if reg.Enabled() {
		t.Fatal("Enabled() = true for a disabled exporter")
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("disabled handler status = %d, want 404", rec.Code)
	}
}

func TestPathDefaultsWhenNotAbsolute(t *testing.T) {
	for _, given := range []string{"", "metrics", "  "} {
		reg := metrics.New(metrics.Config{Enabled: true, Path: given})
		if got := reg.Path(); got != metrics.DefaultPath {
			t.Fatalf("Path() for %q = %q, want %q", given, got, metrics.DefaultPath)
		}
	}
}

func TestPathHonorsAbsoluteOverride(t *testing.T) {
	reg := metrics.New(metrics.Config{Enabled: true, Path: "/internal/metrics"})
	if got := reg.Path(); got != "/internal/metrics" {
		t.Fatalf("Path() = %q, want /internal/metrics", got)
	}
}

func TestNilRegistryIsInert(t *testing.T) {
	var reg *metrics.Registry
	if reg.Enabled() {
		t.Fatal("nil registry reports enabled")
	}
	called := false
	h := reg.Instrument(metrics.RouteHealthz, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil))
	if !called {
		t.Fatal("nil registry dropped the wrapped handler")
	}
	reg.ObserveTick("flush", 0, nil)
	reg.SetLogLandingLimit(4)
}
