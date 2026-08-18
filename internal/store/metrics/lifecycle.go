package metrics

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func (r *Registry) buildLifecycle() {
	r.ticks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "lifecycle_ticks_total",
		Help:      "Background maintenance passes run, by job.",
	}, []string{"job"})

	r.tickErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "lifecycle_tick_errors_total",
		Help:      "Background maintenance passes that returned an error, by job.",
	}, []string{"job"})

	r.tickDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "lifecycle_tick_duration_seconds",
		Help:      "Wall time of one background maintenance pass, by job.",
		// A merge pass can run for minutes on a large tier, well past the
		// default bucket range.
		Buckets: []float64{0.01, 0.1, 1, 5, 15, 60, 300, 900},
	}, []string{"job"})

	r.tickSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "lifecycle_last_success_timestamp_seconds",
		Help:      "Unix time of the last maintenance pass that completed without error, by job.",
	}, []string{"job"})

	r.tierSegments = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "tier_segment_files",
		Help:      "Metrics tier segment files seen by the most recent maintenance pass, per tenant.",
	}, []string{"tenant"})

	r.landingFiles = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "log_landing_files",
		Help:      "Log windows in the landing zone after the most recent maintenance pass, per tenant and artifact.",
	}, []string{"tenant", "artifact"})

	r.landingLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "log_landing_files_limit",
		Help:      "Configured per-artifact landing-zone file cap; 0 means the cap is off.",
	})

	r.compactionCPU = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "compaction_cpu_seconds_total",
		Help:      "Wall seconds spent compacting segments, per tenant.",
	}, []string{"tenant"})

	r.promoteAttempts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "promote_attempts_total",
		Help:      "Compacted segments a promote pass tried to copy to the cold root.",
	})
	r.promoteSuccesses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "promote_successes_total",
		Help:      "Compacted segments verified on the cold root.",
	})
	r.promoteRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "promote_retries_total",
		Help:      "Promote copies that replaced a broken cold dest or will be retried.",
	})
	r.promoteBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "promote_bytes_total",
		Help:      "Bytes copied to the cold root after dest verification.",
	})
	r.promoteTmp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "promote_tmp_files",
		Help:      "Unfinished promote temps seen at the start of the latest pass.",
	})
}

// ObserveTickStart snapshots RSS, heap, and cgroup current before a pass.
func (r *Registry) ObserveTickStart(job string) {
	if r.off() || !r.observe {
		return
	}
	r.recordJobSample(job, "start")
}

// ObserveTick records one completed maintenance pass. The success timestamp
// only advances on a clean pass, so a stalled job shows as a growing age even
// while it keeps erroring on schedule.
func (r *Registry) ObserveTick(job string, d time.Duration, err error) {
	if r.off() {
		return
	}
	if r.observe {
		s := r.recordJobSample(job, "end")
		r.logObserveJob(job, d, err, s)
	}
	r.ticks.WithLabelValues(job).Inc()
	r.tickDuration.WithLabelValues(job).Observe(d.Seconds())
	if err != nil {
		r.tickErrors.WithLabelValues(job).Inc()
		return
	}
	r.tickSuccess.WithLabelValues(job).SetToCurrentTime()
}

func (r *Registry) recordJobSample(job, phase string) memSample {
	s := sampleMem(r.cgroupRoot)
	if s.rssOK {
		r.jobRSS.WithLabelValues(job, phase).Set(s.rss)
	}
	r.jobHeap.WithLabelValues(job, phase).Set(s.heap)
	if s.cgOK {
		r.jobCgroup.WithLabelValues(job, phase).Set(s.cg)
	}
	return s
}

func (r *Registry) logObserveJob(job string, d time.Duration, err error, s memSample) {
	log := r.log
	if log == nil {
		log = slog.Default()
	}
	attrs := []any{
		"job", job,
		"duration_ms", d.Milliseconds(),
		"rss_bytes", int64(s.rss),
		"heap_alloc_bytes", int64(s.heap),
	}
	if s.cgOK {
		attrs = append(attrs, "cgroup_current_bytes", int64(s.cg))
	}
	if err != nil {
		attrs = append(attrs, "err", err)
	}
	log.Info("memory observe job", attrs...)
}

// ObserveTierSegments records the live metrics segment count a pass just
// counted for one tenant.
func (r *Registry) ObserveTierSegments(tenant string, files int) {
	if r.off() || !r.cfg.PerTenant {
		return
	}
	r.tierSegments.WithLabelValues(r.tenants.label(tenant)).Set(float64(files))
}

// ObserveLogLandingFiles records how deep one artifact's landing zone is after
// a pass, which is the number an operator compares against the configured cap.
func (r *Registry) ObserveLogLandingFiles(tenant, artifact string, files int) {
	if r.off() || !r.cfg.PerTenant {
		return
	}
	r.landingFiles.WithLabelValues(r.tenants.label(tenant), artifact).Set(float64(files))
}

// ObserveCompactionSeconds adds the wall time one compaction spent working.
func (r *Registry) ObserveCompactionSeconds(tenant string, seconds float64) {
	if r.off() || !r.cfg.PerTenant || seconds <= 0 {
		return
	}
	r.compactionCPU.WithLabelValues(r.tenants.label(tenant)).Add(seconds)
}

// ObservePromote records one promote pass: files attempted, verified, retried,
// bytes landed, and leftover temps seen before crash GC.
func (r *Registry) ObservePromote(attempts, successes, retries int, bytes int64, tmpFiles int) {
	if r.off() {
		return
	}
	if attempts > 0 {
		r.promoteAttempts.Add(float64(attempts))
	}
	if successes > 0 {
		r.promoteSuccesses.Add(float64(successes))
	}
	if retries > 0 {
		r.promoteRetries.Add(float64(retries))
	}
	if bytes > 0 {
		r.promoteBytes.Add(float64(bytes))
	}
	r.promoteTmp.Set(float64(tmpFiles))
}

// SetLogLandingLimit publishes the configured landing-zone cap so the depth
// gauge can be read as a saturation ratio without hardcoding the limit in a
// dashboard.
func (r *Registry) SetLogLandingLimit(files int) {
	if r.off() {
		return
	}
	r.landingLimit.Set(float64(files))
}
