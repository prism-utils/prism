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
	// PerTenant adds the tenant dimension to query, error, lifecycle file, and
	// query-plane RED series. It multiplies those families by the number of
	// active tenants, so it is a deliberate cardinality choice rather than a
	// free switch. When false, query RED series stay registered but drop the
	// tenant label rather than emitting an empty one.
	PerTenant bool
	// Observe registers extra memory-debug series and job slog. Default off.
	Observe bool
	// GoMemLimitBytes is the parsed GOMEMLIMIT (0 if unset or unparseable).
	GoMemLimitBytes int64
	// DuckDBMemoryLimitBytes is the parsed DUCKDB_MEMORY_LIMIT (0 if unset).
	DuckDBMemoryLimitBytes int64
	// CgroupRoot is the directory holding cgroup v2 memory files. Empty uses
	// the default unified hierarchy mount.
	CgroupRoot string
}

func (c Config) normalized() Config {
	c.Path = strings.TrimSpace(c.Path)
	if !strings.HasPrefix(c.Path, "/") {
		c.Path = DefaultPath
	}
	return c
}
