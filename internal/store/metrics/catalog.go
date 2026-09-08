package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// PlaneMetrics is the catalog_lookup / catalog_rebuild plane label for
	// metrics segments.
	PlaneMetrics = "metrics"
	// PlaneLogs is the catalog_lookup / catalog_rebuild plane label for logs.
	PlaneLogs = "logs"
	// CatalogHit is a lookup that reused stored bounds.
	CatalogHit = "hit"
	// CatalogMiss is a lookup that must stat the file.
	CatalogMiss = "miss"
	// RebuildCorrupt is the only production rebuild reason.
	RebuildCorrupt = "corrupt"
)

var bound atomic.Pointer[Registry]

var lookupTotals [4]atomic.Int64

func lookupIndex(plane, result string) (int, bool) {
	switch {
	case plane == PlaneMetrics && result == CatalogHit:
		return 0, true
	case plane == PlaneMetrics && result == CatalogMiss:
		return 1, true
	case plane == PlaneLogs && result == CatalogHit:
		return 2, true
	case plane == PlaneLogs && result == CatalogMiss:
		return 3, true
	default:
		return 0, false
	}
}

func (r *Registry) buildCatalog() {
	r.catalogLookup = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "catalog_lookup_total",
		Help:      "Merge-scan catalog lookups by plane and hit/miss.",
	}, []string{"plane", "result"})

	r.catalogRebuild = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "catalog_rebuild_total",
		Help:      "Full catalog rebuilds by plane and reason.",
	}, []string{"plane", "reason"})

	r.duckdbOpens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "duckdb_opens_total",
		Help:      "DuckDB instances opened, by role.",
	}, []string{"role"})
}

// Bind installs the process exporter that package-level catalog helpers write
// to. A nil registry clears the binding.
func Bind(r *Registry) {
	bound.Store(r)
}

// Bound returns the exporter last passed to Bind, or nil.
func Bound() *Registry {
	return bound.Load()
}

// CatalogLookup records one merge-scan lookup against the bound exporter.
func CatalogLookup(plane, result string) {
	if i, ok := lookupIndex(plane, result); ok {
		lookupTotals[i].Add(1)
	}
	if r := bound.Load(); r != nil {
		r.ObserveCatalogLookup(plane, result)
	}
}

// CatalogRebuild records one full rebuild against the bound exporter.
func CatalogRebuild(plane, reason string) {
	if r := bound.Load(); r != nil {
		r.ObserveCatalogRebuild(plane, reason)
	}
}

// LookupTotal is the process count of CatalogLookup calls for tests.
func LookupTotal(plane, result string) int64 {
	i, ok := lookupIndex(plane, result)
	if !ok {
		return 0
	}
	return lookupTotals[i].Load()
}

// ObserveCatalogLookup increments the lookup counter.
func (r *Registry) ObserveCatalogLookup(plane, result string) {
	if r.off() {
		return
	}
	r.catalogLookup.WithLabelValues(plane, result).Inc()
}

// ObserveCatalogRebuild increments the rebuild counter.
func (r *Registry) ObserveCatalogRebuild(plane, reason string) {
	if r.off() {
		return
	}
	r.catalogRebuild.WithLabelValues(plane, reason).Inc()
}

// ObserveDuckDBOpen increments the per-role open counter.
func (r *Registry) ObserveDuckDBOpen(role string) {
	if r.off() {
		return
	}
	if _, ok := roleIndex(role); !ok {
		return
	}
	r.duckdbOpens.WithLabelValues(role).Inc()
}
