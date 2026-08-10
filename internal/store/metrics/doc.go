// Package metrics is the prism-store Prometheus exporter.
//
// It owns a registry that is private to the process rather than the global
// default one, so a test that builds an exporter cannot collide with another
// test or leak collectors into unrelated code. On top of the standard Go
// runtime and process collectors it publishes a USE-shaped view of the store:
// utilization and saturation as gauges (queue occupancy, resident tenant
// handles, landing-file depth), errors and work as counters, and latency as
// histograms — plus a query-plane RED family (requests, duration, inflight)
// under a closed api label set (promql | loki | sql).
//
// Two rules govern every label produced here. HTTP route labels and query api
// labels come from a closed set supplied by the caller and are never derived
// from a request path, and tenant labels are opt-in and capped, so neither an
// unusual URL nor a burst of unknown namespaces can grow the series count
// without bound.
//
// Nothing in this package reads the data directory. File counts arrive from
// lifecycle ticks that already scanned those directories for their own work,
// which keeps a scrape free of disk I/O.
package metrics
