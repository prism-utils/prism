package metrics

import "strings"

// DefaultPath is the scrape path used unless an absolute override is given.
const DefaultPath = "/metrics"

// namespace prefixes every store-owned metric. Runtime collectors keep their
// conventional go_ / process_ names so the standard dashboards resolve.
const namespace = "prism_store"

// Config selects the exporter surface. The zero value is a disabled exporter.
type Config struct {
	// Enabled registers the collectors and serves the scrape endpoint.
	Enabled bool
	// Path is the scrape path. Anything that is not an absolute path falls back
	// to the default, because a relative pattern cannot be mounted on a mux.
	Path string
	// PerTenant adds the tenant dimension to query, error, and lifecycle file
	// series. It multiplies those families by the number of active tenants, so
	// it is a deliberate cardinality choice rather than a free switch.
	PerTenant bool
}

func (c Config) normalized() Config {
	c.Path = strings.TrimSpace(c.Path)
	if !strings.HasPrefix(c.Path, "/") {
		c.Path = DefaultPath
	}
	return c
}
