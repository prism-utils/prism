package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry is the exporter: one private Prometheus registry plus the store
// collectors registered on it. A nil Registry, and one built from a disabled
// Config, are both inert — every method is safe to call and does nothing — so
// call sites need no conditionals.
type Registry struct {
	cfg Config
	reg *prometheus.Registry

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	queries      *prometheus.CounterVec
	queryErrors  *prometheus.CounterVec

	queryRequests *prometheus.CounterVec
	queryDuration *prometheus.HistogramVec
	queryInflight *prometheus.GaugeVec

	queueRejected *prometheus.CounterVec
	queueWait     prometheus.Histogram

	ticks        *prometheus.CounterVec
	tickErrors   *prometheus.CounterVec
	tickDuration *prometheus.HistogramVec
	tickSuccess  *prometheus.GaugeVec

	tierSegments  *prometheus.GaugeVec
	landingFiles  *prometheus.GaugeVec
	landingLimit  prometheus.Gauge
	compactionCPU *prometheus.CounterVec

	tenants *tenantLabeller
}

// New builds an exporter. A disabled Config yields an inert Registry that
// registers nothing, so the runtime cost of turning metrics off is zero.
func New(cfg Config) *Registry {
	cfg = cfg.normalized()
	if !cfg.Enabled {
		return &Registry{cfg: cfg}
	}

	r := &Registry{
		cfg:     cfg,
		reg:     prometheus.NewRegistry(),
		tenants: newTenantLabeller(MaxTenantLabelValues),
	}
	r.buildHTTP()
	r.buildQuery()
	r.buildQueue()
	r.buildLifecycle()

	// Registration can only fail on a duplicate or inconsistent descriptor,
	// which is a coding error in this file and is caught by this package's own
	// tests on the first construction.
	r.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.httpRequests, r.httpDuration, r.queries, r.queryErrors,
		r.queryRequests, r.queryDuration, r.queryInflight,
		r.queueRejected, r.queueWait,
		r.ticks, r.tickErrors, r.tickDuration, r.tickSuccess,
		r.tierSegments, r.landingFiles, r.landingLimit, r.compactionCPU,
	)
	return r
}

// off reports an exporter that must ignore every observation.
func (r *Registry) off() bool {
	return r == nil || r.reg == nil
}

// Enabled reports whether the scrape endpoint should be mounted.
func (r *Registry) Enabled() bool {
	return !r.off()
}

// Path is the scrape path the endpoint must be mounted on.
func (r *Registry) Path() string {
	if r == nil {
		return DefaultPath
	}
	return r.cfg.Path
}

// Gatherer exposes the private registry for callers that render the exposition
// themselves.
func (r *Registry) Gatherer() prometheus.Gatherer {
	if r.off() {
		return prometheus.Gatherers{}
	}
	return r.reg
}

// Handler serves the text exposition for this registry alone. A partly failed
// gather still returns the metrics that did collect, because a scrape that
// drops everything over one unreadable source hides the incident.
func (r *Registry) Handler() http.Handler {
	if r.off() {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}
