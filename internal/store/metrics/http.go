package metrics

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	storetenant "github.com/prism-utils/prism/internal/store/tenant"
	"github.com/prometheus/client_golang/prometheus"
)

// Route labels are a closed set chosen by the wiring, never derived from the
// request path: a path-derived label would mint one series per tenant, per
// artifact, and per crafted URL.
const (
	RouteHealthz      = "healthz"
	RouteReadyz       = "readyz"
	RouteMetrics      = "metrics"
	RouteIngest       = "ingest"
	RouteQuery        = "query"
	RouteSQL          = "sql"
	RoutePromQL       = "promql"
	RouteLoki         = "loki"
	RouteStats        = "stats"
	RouteAdminQueue   = "admin_queue"
	RouteAdminEnsure  = "admin_ensure"
	RouteAdminCompact = "admin_compact"
)

// MaxTenantLabelValues caps how many distinct tenants may appear in tenant
// series. The budget is per label value across the tenant-labelled families, so
// this bounds the worst case an operator has to size Prometheus for.
const MaxTenantLabelValues = 256

// OverflowTenantLabel absorbs every tenant past the cap. It starts with an
// underscore, which tenant namespaces may not, so it can never shadow a real
// namespace's series.
const OverflowTenantLabel = "__over_limit__"

func (r *Registry) buildHTTP() {
	r.httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "http_requests_total",
		Help:      "HTTP requests served, by low-cardinality route, method, and response status code.",
	}, []string{"route", "method", "code"})

	r.httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request latency by low-cardinality route.",
		// Heavy reads queue for seconds while probes answer in microseconds, so
		// the buckets span both ends rather than the default web-request range.
		Buckets: []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1, 2.5, 5, 15, 60, 300},
	}, []string{"route"})

	r.queries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "queries_total",
		Help:      "Read requests served per tenant and route; aggregate with sum without (tenant).",
	}, []string{"tenant", "route"})

	r.queryErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "query_errors_total",
		Help:      "Failed read requests per tenant and route, bucketed by response status class.",
	}, []string{"tenant", "route", "code_class"})
}

// Instrument wraps next with request accounting under a fixed route label. It
// belongs outermost in a handler chain so shed and unauthorized responses are
// counted, not just the ones that reach the handler.
func (r *Registry) Instrument(route string, next http.Handler) http.Handler {
	if r.off() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, req)

		r.httpRequests.WithLabelValues(route, req.Method, strconv.Itoa(rec.status)).Inc()
		r.httpDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
		r.observeTenantRequest(route, req, rec.status)
	})
}

// observeTenantRequest records the tenant-dimensioned view of one request. The
// namespace is taken from the matched route's wildcard and must pass tenant
// validation, so a path segment that could never name a tenant creates no
// series at all.
func (r *Registry) observeTenantRequest(route string, req *http.Request, status int) {
	if !r.cfg.PerTenant {
		return
	}
	ns := req.PathValue("ns")
	if ns == "" || !storetenant.TenantAllowed(ns) {
		return
	}
	label := r.tenants.label(ns)
	r.queries.WithLabelValues(label, route).Inc()
	if status >= http.StatusBadRequest {
		r.queryErrors.WithLabelValues(label, route, codeClass(status)).Inc()
	}
}

// codeClass folds a status into its family so error series stay at a handful of
// values instead of one per status code.
func codeClass(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "other"
	}
}

// statusRecorder remembers the status line so the request counter can label it.
// Unwrap and Flush keep streaming responses working: a handler that writes an
// Arrow stream must still be able to flush through this wrapper.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.written {
		s.status = code
		s.written = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.written = true
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

func (s *statusRecorder) Flush() {
	_ = http.NewResponseController(s.ResponseWriter).Flush()
}

// tenantLabeller bounds the number of distinct tenant label values the exporter
// will mint. Past the cap every further namespace folds into one overflow
// value, so a flood of unknown tenants costs a constant number of series
// instead of an unbounded one.
type tenantLabeller struct {
	max  int
	mu   sync.RWMutex
	seen map[string]struct{}
}

func newTenantLabeller(maxValues int) *tenantLabeller {
	return &tenantLabeller{max: maxValues, seen: make(map[string]struct{})}
}

func (t *tenantLabeller) label(ns string) string {
	t.mu.RLock()
	_, known := t.seen[ns]
	t.mu.RUnlock()
	if known {
		return ns
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if _, known := t.seen[ns]; known {
		return ns
	}
	if len(t.seen) >= t.max {
		return OverflowTenantLabel
	}
	t.seen[ns] = struct{}{}
	return ns
}
