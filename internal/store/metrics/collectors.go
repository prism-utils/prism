package metrics

import (
	"time"

	"github.com/elk-utilities/prism/internal/store/queue"
	"github.com/prometheus/client_golang/prometheus"
)

// QueueStats reports the read limiter's caps and live occupancy.
type QueueStats interface {
	Snapshot() queue.Snapshot
}

// EngineStats reports resident tenant-handle occupancy against its ceiling.
type EngineStats interface {
	OpenTenants() int
	MaxOpenTenants() int
	EvictedTenantsTotal() int64
}

var (
	queueEnabledDesc = prometheus.NewDesc(
		namespace+"_queue_enabled",
		"1 when the heavy-read in-flight limiter is gating requests, 0 when reads run unbounded.",
		nil, nil)
	queueInFlightDesc = prometheus.NewDesc(
		namespace+"_queue_in_flight",
		"Heavy reads executing right now.",
		nil, nil)
	queueWaitingDesc = prometheus.NewDesc(
		namespace+"_queue_waiting",
		"Heavy reads queued for an execution slot.",
		nil, nil)
	queueMaxInFlightDesc = prometheus.NewDesc(
		namespace+"_queue_max_in_flight",
		"Configured ceiling on concurrently executing heavy reads.",
		nil, nil)
	queueMaxQueueDesc = prometheus.NewDesc(
		namespace+"_queue_max_queue",
		"Configured ceiling on heavy reads allowed to wait for a slot.",
		nil, nil)
	queueWaitTimeoutDesc = prometheus.NewDesc(
		namespace+"_queue_wait_timeout_seconds",
		"How long a queued heavy read waits for a slot before it is shed.",
		nil, nil)

	engineOpenTenantsDesc = prometheus.NewDesc(
		namespace+"_engine_open_tenants",
		"Per-tenant databases held open by the engine LRU.",
		nil, nil)
	engineMaxOpenTenantsDesc = prometheus.NewDesc(
		namespace+"_engine_max_open_tenants",
		"Ceiling on per-tenant databases held open at once.",
		nil, nil)
	engineEvictionsDesc = prometheus.NewDesc(
		namespace+"_engine_tenant_evictions_total",
		"Tenant databases closed to stay under the open-handle ceiling since process start.",
		nil, nil)
)

func (r *Registry) buildQueue() {
	r.queueRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "queue_rejected_total",
		Help:      "Heavy reads shed by the in-flight limiter, by reason.",
	}, []string{"reason"})

	r.queueWait = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "queue_wait_seconds",
		Help:      "Time an admitted heavy read spent waiting for an execution slot.",
		// The wait budget runs to two minutes by default, so the buckets have to
		// reach that far to distinguish a slow queue from a saturated one.
		Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 15, 30, 60, 120},
	})
}

// SetQueueSource attaches the read limiter the gauges are read from. Reading at
// scrape time keeps them fresh with no sampling goroutine, because a snapshot
// is atomic loads only.
func (r *Registry) SetQueueSource(q QueueStats) error {
	if r.off() || q == nil {
		return nil
	}
	return r.reg.Register(&queueCollector{src: q})
}

// SetEngineSource attaches the engine whose open-handle occupancy is reported.
func (r *Registry) SetEngineSource(e EngineStats) error {
	if r.off() || e == nil {
		return nil
	}
	return r.reg.Register(&engineCollector{src: e})
}

// ObserveAdmitted records the queueing delay of one admitted heavy read.
func (r *Registry) ObserveAdmitted(wait time.Duration) {
	if r.off() {
		return
	}
	r.queueWait.Observe(wait.Seconds())
}

// ObserveRejected records one shed heavy read against the reason it was shed.
func (r *Registry) ObserveRejected(reason queue.RejectReason, _ time.Duration) {
	if r.off() {
		return
	}
	r.queueRejected.WithLabelValues(string(reason)).Inc()
}

type queueCollector struct {
	src QueueStats
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueEnabledDesc
	ch <- queueInFlightDesc
	ch <- queueWaitingDesc
	ch <- queueMaxInFlightDesc
	ch <- queueMaxQueueDesc
	ch <- queueWaitTimeoutDesc
}

// Collect publishes one coherent view: every gauge comes from a single snapshot
// so in-flight and waiting cannot be read from different instants.
func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.src.Snapshot()
	ch <- prometheus.MustNewConstMetric(queueEnabledDesc, prometheus.GaugeValue, boolValue(s.Enabled))
	ch <- prometheus.MustNewConstMetric(queueInFlightDesc, prometheus.GaugeValue, float64(s.InFlight))
	ch <- prometheus.MustNewConstMetric(queueWaitingDesc, prometheus.GaugeValue, float64(s.Waiting))
	ch <- prometheus.MustNewConstMetric(queueMaxInFlightDesc, prometheus.GaugeValue, float64(s.MaxInFlight))
	ch <- prometheus.MustNewConstMetric(queueMaxQueueDesc, prometheus.GaugeValue, float64(s.MaxQueue))
	ch <- prometheus.MustNewConstMetric(queueWaitTimeoutDesc, prometheus.GaugeValue, float64(s.TimeoutMs)/1000)
}

type engineCollector struct {
	src EngineStats
}

func (c *engineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- engineOpenTenantsDesc
	ch <- engineMaxOpenTenantsDesc
	ch <- engineEvictionsDesc
}

func (c *engineCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(engineOpenTenantsDesc, prometheus.GaugeValue, float64(c.src.OpenTenants()))
	ch <- prometheus.MustNewConstMetric(engineMaxOpenTenantsDesc, prometheus.GaugeValue, float64(c.src.MaxOpenTenants()))
	ch <- prometheus.MustNewConstMetric(engineEvictionsDesc, prometheus.CounterValue, float64(c.src.EvictedTenantsTotal()))
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
