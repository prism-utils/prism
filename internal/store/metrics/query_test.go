package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/metrics"
)

func TestInstrumentQueryCountsSuccessAsCode200(t *testing.T) {
	reg := metrics.New(enabledConfig())
	serveNS(reg.InstrumentQuery(metrics.APIPromQL, statusHandler(http.StatusOK)),
		http.MethodGet, "GET /{ns}/api/v1/query", "/user-6f3a9c2b-apps/api/v1/query")

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_query_requests_total{api="promql",code="200",tenant="user-6f3a9c2b-apps"} 1`,
		`prism_store_query_duration_seconds_count{api="promql",tenant="user-6f3a9c2b-apps"} 1`,
	)
}

func TestInstrumentQueryCounts4xxAnd5xx(t *testing.T) {
	reg := metrics.New(enabledConfig())
	serveNS(reg.InstrumentQuery(metrics.APILoki, statusHandler(http.StatusForbidden)),
		http.MethodGet, "GET /{ns}/loki/api/v1/query_range", "/user-6f3a9c2b-apps/loki/api/v1/query_range")
	serveNS(reg.InstrumentQuery(metrics.APISQL, statusHandler(http.StatusTooManyRequests)),
		http.MethodPost, "POST /{ns}/sql", "/user-6f3a9c2b-apps/sql")
	serveNS(reg.InstrumentQuery(metrics.APISQL, statusHandler(http.StatusBadGateway)),
		http.MethodPost, "POST /{ns}/sql", "/user-6f3a9c2b-apps/sql")

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_query_requests_total{api="loki",code="403",tenant="user-6f3a9c2b-apps"} 1`,
		`prism_store_query_requests_total{api="sql",code="429",tenant="user-6f3a9c2b-apps"} 1`,
		`prism_store_query_requests_total{api="sql",code="502",tenant="user-6f3a9c2b-apps"} 1`,
	)
}

func TestInstrumentQueryOmitsTenantLabelWhenPerTenantOff(t *testing.T) {
	cfg := enabledConfig()
	cfg.PerTenant = false
	reg := metrics.New(cfg)
	serveNS(reg.InstrumentQuery(metrics.APISQL, statusHandler(http.StatusOK)),
		http.MethodPost, "POST /{ns}/sql", "/user-6f3a9c2b-apps/sql")
	serveNS(reg.InstrumentQuery(metrics.APISQL, statusHandler(http.StatusInternalServerError)),
		http.MethodPost, "POST /{ns}/sql", "/user-6f3a9c2b-apps/sql")

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_query_requests_total{api="sql",code="200"} 1`,
		`prism_store_query_requests_total{api="sql",code="500"} 1`,
		`prism_store_query_duration_seconds_count{api="sql"} 2`,
	)
	if queryREDLineHasTenant(body, "prism_store_query_requests_total") {
		t.Fatalf("query_requests_total carried a tenant label with METRICS_PER_TENANT=false\n%s", body)
	}
	if queryREDLineHasTenant(body, "prism_store_query_duration_seconds") {
		t.Fatalf("query_duration_seconds carried a tenant label with METRICS_PER_TENANT=false\n%s", body)
	}
}

func TestInstrumentQueryInflightRisesDuringHeldRequest(t *testing.T) {
	reg := metrics.New(enabledConfig())
	started := make(chan struct{})
	release := make(chan struct{})
	h := reg.InstrumentQuery(metrics.APIPromQL, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		serveNS(h, http.MethodGet, "GET /{ns}/api/v1/query", "/user-6f3a9c2b-apps/api/v1/query")
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	mid := scrape(t, reg)
	assertContains(t, mid, `prism_store_query_inflight{api="promql"} 1`)
	if queryREDLineHasTenant(mid, "prism_store_query_inflight") {
		t.Fatalf("query_inflight must not carry a tenant label\n%s", mid)
	}

	close(release)
	wg.Wait()

	after := scrape(t, reg)
	assertContains(t, after, `prism_store_query_inflight{api="promql"} 0`)
	assertContains(t, after,
		`prism_store_query_requests_total{api="promql",code="200",tenant="user-6f3a9c2b-apps"} 1`,
	)
}

func TestInstrumentQueryAbsentWhenMetricsDisabled(t *testing.T) {
	reg := metrics.New(metrics.Config{Enabled: false})
	called := false
	h := reg.InstrumentQuery(metrics.APISQL, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	if !called {
		t.Fatal("disabled registry dropped the wrapped handler")
	}

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("disabled scrape status = %d, want 404", rec.Code)
	}
}

func TestNilRegistryInstrumentQueryIsInert(t *testing.T) {
	var reg *metrics.Registry
	called := false
	h := reg.InstrumentQuery(metrics.APILoki, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/loki", nil))
	if !called {
		t.Fatal("nil registry dropped the wrapped handler")
	}
}

func queryREDLineHasTenant(body, metricPrefix string) bool {
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, metricPrefix) {
			continue
		}
		if strings.Contains(line, `tenant="`) {
			return true
		}
	}
	return false
}
