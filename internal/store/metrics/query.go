package metrics

import (
	"net/http"
	"strconv"
	"time"

	storetenant "github.com/prism-utils/prism/internal/store/tenant"
	"github.com/prometheus/client_golang/prometheus"
)

// Query API labels are a closed set chosen by the wiring. They name the read
// surface (never a request path), so cardinality stays at three values.
const (
	APIPromQL = "promql"
	APILoki   = "loki"
	APISQL    = "sql"
)

func (r *Registry) buildQuery() {
	requestLabels := []string{"api", "code"}
	durationLabels := []string{"api"}
	if r.cfg.PerTenant {
		requestLabels = append(requestLabels, "tenant")
		durationLabels = append(durationLabels, "tenant")
	}

	r.queryRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "query_requests_total",
		Help:      "Query-plane requests by API and response status code.",
	}, requestLabels)

	r.queryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "query_duration_seconds",
		Help:      "End-to-end query-plane handler latency by API.",
		Buckets:   []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1, 2.5, 5, 15, 60, 300},
	}, durationLabels)

	r.queryInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "query_inflight",
		Help:      "Query-plane requests currently inside the handler, including queue wait.",
	}, []string{"api"})
}

// InstrumentQuery wraps next with query-plane RED accounting under a fixed api
// label. It belongs outermost in a handler chain so authz, tenant-guard, and
// queue rejects are counted, and so inflight covers the whole wait.
func (r *Registry) InstrumentQuery(api string, next http.Handler) http.Handler {
	if r.off() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.queryInflight.WithLabelValues(api).Inc()
		defer r.queryInflight.WithLabelValues(api).Dec()

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, req)
		r.observeQuery(api, req, rec.status, time.Since(start).Seconds())
	})
}

func (r *Registry) observeQuery(api string, req *http.Request, status int, seconds float64) {
	code := strconv.Itoa(status)
	if !r.cfg.PerTenant {
		r.queryRequests.WithLabelValues(api, code).Inc()
		r.queryDuration.WithLabelValues(api).Observe(seconds)
		return
	}
	tenant := r.queryTenantLabel(req)
	r.queryRequests.WithLabelValues(api, code, tenant).Inc()
	r.queryDuration.WithLabelValues(api, tenant).Observe(seconds)
}

// queryTenantLabel maps the route namespace onto the bounded tenant label set.
// Namespaces that cannot name a tenant fold into the overflow value so a crafted
// path cannot mint unbounded series.
func (r *Registry) queryTenantLabel(req *http.Request) string {
	ns := req.PathValue("ns")
	if ns == "" || !storetenant.TenantAllowed(ns) {
		return OverflowTenantLabel
	}
	return r.tenants.label(ns)
}
