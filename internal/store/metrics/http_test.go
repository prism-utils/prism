package metrics_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/store/metrics"
)

func statusHandler(code int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(code)
	})
}

// serveNS drives one request whose {ns} wildcard is populated the way the store
// mux populates it, so the middleware sees a tenant exactly as it would in
// production.
func serveNS(h http.Handler, method, pattern, path string) {
	mux := http.NewServeMux()
	mux.Handle(pattern, h)
	req := httptest.NewRequestWithContext(context.Background(), method, path, nil)
	mux.ServeHTTP(httptest.NewRecorder(), req)
}

func TestInstrumentCountsRequestsUnderRouteLabel(t *testing.T) {
	reg := metrics.New(enabledConfig())
	h := reg.Instrument(metrics.RouteHealthz, statusHandler(http.StatusOK))
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil))
	}

	body := scrape(t, reg)
	want := `prism_store_http_requests_total{code="200",method="GET",route="healthz"} 3`
	if !strings.Contains(body, want) {
		t.Fatalf("exposition missing %q\n%s", want, body)
	}
	assertContains(t, body, `prism_store_http_request_duration_seconds_count{route="healthz"} 3`)
}

func TestInstrumentRecordsHandlerStatusIncluding429And5xx(t *testing.T) {
	reg := metrics.New(enabledConfig())
	reg.Instrument(metrics.RouteSQL, statusHandler(http.StatusTooManyRequests)).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	reg.Instrument(metrics.RouteSQL, statusHandler(http.StatusInternalServerError)).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_http_requests_total{code="429",method="POST",route="sql"} 1`,
		`prism_store_http_requests_total{code="500",method="POST",route="sql"} 1`,
	)
}

func TestInstrumentDefaultsToStatus200WhenHandlerOnlyWrites(t *testing.T) {
	reg := metrics.New(enabledConfig())
	h := reg.Instrument(metrics.RouteQuery, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("body"))
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/q", nil))

	assertContains(t, scrape(t, reg), `prism_store_http_requests_total{code="200",method="GET",route="query"} 1`)
}

func TestInstrumentKeepsResponseStreamable(t *testing.T) {
	reg := metrics.New(enabledConfig())
	flushed := false
	h := reg.Instrument(metrics.RouteSQL, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			return
		}
		_, _ = w.Write([]byte("chunk"))
		f.Flush()
		flushed = true
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	if !flushed {
		t.Fatal("wrapped ResponseWriter is not an http.Flusher; Arrow streaming would break")
	}
}

func TestInstrumentNeverLabelsRoutesWithTheRequestPath(t *testing.T) {
	reg := metrics.New(enabledConfig())
	h := reg.Instrument(metrics.RouteSQL, statusHandler(http.StatusOK))
	serveNS(h, http.MethodPost, "POST /{ns}/sql", "/user-6f3a9c2b-apps/sql")

	body := scrape(t, reg)
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "prism_store_http_requests_total") {
			continue
		}
		if strings.Contains(line, "user-6f3a9c2b-apps") {
			t.Fatalf("HTTP route series carries a raw tenant id: %s", line)
		}
	}
}

func TestPerTenantSeriesRecordedWhenEnabled(t *testing.T) {
	reg := metrics.New(enabledConfig())
	serveNS(reg.Instrument(metrics.RouteSQL, statusHandler(http.StatusOK)), http.MethodPost, "POST /{ns}/sql", "/user-6f3a9c2b-apps/sql")
	serveNS(reg.Instrument(metrics.RouteSQL, statusHandler(http.StatusInternalServerError)), http.MethodPost, "POST /{ns}/sql", "/user-6f3a9c2b-apps/sql")

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_queries_total{route="sql",tenant="user-6f3a9c2b-apps"} 2`,
		`prism_store_query_errors_total{code_class="5xx",route="sql",tenant="user-6f3a9c2b-apps"} 1`,
	)
}

func TestPerTenantSeriesAbsentWhenDisabled(t *testing.T) {
	cfg := enabledConfig()
	cfg.PerTenant = false
	reg := metrics.New(cfg)
	serveNS(reg.Instrument(metrics.RouteSQL, statusHandler(http.StatusInternalServerError)), http.MethodPost, "POST /{ns}/sql", "/user-6f3a9c2b-apps/sql")

	body := scrape(t, reg)
	assertAbsent(t, body, "prism_store_queries_total", "prism_store_query_errors_total")
	// The bounded HTTP series stay on: only the tenant dimension is opt-in.
	assertContains(t, body, `prism_store_http_requests_total{code="500",method="POST",route="sql"} 1`)
}

func TestPerTenantSeriesSkipMalformedNamespaces(t *testing.T) {
	reg := metrics.New(enabledConfig())
	serveNS(reg.Instrument(metrics.RouteSQL, statusHandler(http.StatusNotFound)), http.MethodPost, "POST /{ns}/sql", "/NOT-a-tenant!/sql")

	assertAbsent(t, scrape(t, reg), "prism_store_queries_total")
}

func TestPerTenantSeriesFoldOverflowIntoOneLabel(t *testing.T) {
	reg := metrics.New(enabledConfig())
	h := reg.Instrument(metrics.RouteQuery, statusHandler(http.StatusOK))
	for i := 0; i < metrics.MaxTenantLabelValues+8; i++ {
		ns := fmt.Sprintf("tenant-%04d", i)
		serveNS(h, http.MethodGet, "GET /{ns}/query", "/"+ns+"/query")
	}

	body := scrape(t, reg)
	var series int
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "prism_store_queries_total{") {
			series++
		}
	}
	if series > metrics.MaxTenantLabelValues+1 {
		t.Fatalf("tenant series = %d, want at most %d (cap + overflow)", series, metrics.MaxTenantLabelValues+1)
	}
	assertContains(t, body, metrics.OverflowTenantLabel)
}

func TestCodeClassBucketsByFamily(t *testing.T) {
	reg := metrics.New(enabledConfig())
	for _, code := range []int{http.StatusBadRequest, http.StatusTooManyRequests, http.StatusBadGateway} {
		serveNS(reg.Instrument(metrics.RouteLoki, statusHandler(code)), http.MethodGet, "GET /{ns}/loki", "/user-6f3a9c2b-apps/loki")
	}

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_query_errors_total{code_class="4xx",route="loki",tenant="user-6f3a9c2b-apps"} 2`,
		`prism_store_query_errors_total{code_class="5xx",route="loki",tenant="user-6f3a9c2b-apps"} 1`,
	)
}

func TestSuccessfulRequestRecordsNoError(t *testing.T) {
	reg := metrics.New(enabledConfig())
	serveNS(reg.Instrument(metrics.RouteQuery, statusHandler(http.StatusOK)), http.MethodGet, "GET /{ns}/query", "/user-6f3a9c2b-apps/query")

	assertAbsent(t, scrape(t, reg), "prism_store_query_errors_total")
}
