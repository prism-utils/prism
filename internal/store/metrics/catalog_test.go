package metrics_test

import (
	"testing"

	"github.com/prism-utils/prism/internal/store/metrics"
)

func TestCatalogLookupAndRebuildCounters(t *testing.T) {
	reg := metrics.New(enabledConfig())
	reg.ObserveCatalogLookup(metrics.PlaneMetrics, metrics.CatalogHit)
	reg.ObserveCatalogLookup(metrics.PlaneMetrics, metrics.CatalogHit)
	reg.ObserveCatalogLookup(metrics.PlaneMetrics, metrics.CatalogMiss)
	reg.ObserveCatalogLookup(metrics.PlaneLogs, metrics.CatalogHit)
	reg.ObserveCatalogRebuild(metrics.PlaneMetrics, metrics.RebuildCorrupt)
	reg.ObserveCatalogRebuild(metrics.PlaneLogs, metrics.RebuildCorrupt)

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_catalog_lookup_total{plane="metrics",result="hit"} 2`,
		`prism_store_catalog_lookup_total{plane="metrics",result="miss"} 1`,
		`prism_store_catalog_lookup_total{plane="logs",result="hit"} 1`,
		`prism_store_catalog_rebuild_total{plane="metrics",reason="corrupt"} 1`,
		`prism_store_catalog_rebuild_total{plane="logs",reason="corrupt"} 1`,
	)
}

func TestCatalogCountersNoopWhenDisabled(t *testing.T) {
	reg := metrics.New(metrics.Config{Enabled: false})
	reg.ObserveCatalogLookup(metrics.PlaneMetrics, metrics.CatalogHit)
	reg.ObserveCatalogRebuild(metrics.PlaneMetrics, metrics.RebuildCorrupt)
}

func TestBindForwardsProcessCatalogObservations(t *testing.T) {
	reg := metrics.New(enabledConfig())
	metrics.Bind(reg)
	t.Cleanup(func() { metrics.Bind(nil) })

	metrics.CatalogLookup(metrics.PlaneMetrics, metrics.CatalogMiss)
	metrics.CatalogRebuild(metrics.PlaneLogs, metrics.RebuildCorrupt)

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_catalog_lookup_total{plane="metrics",result="miss"} 1`,
		`prism_store_catalog_rebuild_total{plane="logs",reason="corrupt"} 1`,
	)
}
