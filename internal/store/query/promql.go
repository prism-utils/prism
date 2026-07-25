package query

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// PromQL API defaults. MaxSamples mirrors Prometheus' --query.max-samples so a
// single query cannot load more samples into memory than an operator expects;
// MaxPoints caps a range query's resolution the way Prometheus bounds
// (end-start)/step to protect the server from pathological step values.
const (
	defaultPromQLMaxSamples    = 50_000_000
	defaultPromQLTimeout       = 30 * time.Second
	defaultPromQLLookbackDelta = 5 * time.Minute
	defaultPromQLMaxPoints     = 11_000
)

// PromQLConfig holds settings for the Prometheus-compatible read API.
type PromQLConfig struct {
	DataDir     string
	RoutePrefix string
	// MaxSamples bounds the samples a single query may load into memory.
	MaxSamples int
	// Timeout bounds a single query's wall-clock execution.
	Timeout time.Duration
	// LookbackDelta is how far back an instant vector selector looks for a sample.
	LookbackDelta time.Duration
	// MaxPoints caps the number of resolution steps in a range query.
	MaxPoints int
	// MemoryLimit / Threads apply the shared DuckDB governance to the sandbox.
	MemoryLimit string
	Threads     int
	// HotOnly restricts the sandbox metrics view to the hot snapshot.
	HotOnly bool
	// RunJobs mirrors the process-wide flag: a writer flushes a fresh hot
	// snapshot before serving; a read-only replica serves the writer's snapshot
	// as-is and never writes to the tenant data dir.
	RunJobs bool
}

// Validate reports configuration errors with the offending field named.
func (c *PromQLConfig) Validate() error {
	if c.MaxSamples <= 0 {
		return fmt.Errorf("promql.max_samples: must be > 0")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("promql.timeout: must be > 0")
	}
	if c.LookbackDelta <= 0 {
		return fmt.Errorf("promql.lookback_delta: must be > 0")
	}
	if c.MaxPoints <= 0 {
		return fmt.Errorf("promql.max_points: must be > 0")
	}
	return nil
}

func (c *PromQLConfig) withDefaults() {
	if c.MaxSamples <= 0 {
		c.MaxSamples = defaultPromQLMaxSamples
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultPromQLTimeout
	}
	if c.LookbackDelta <= 0 {
		c.LookbackDelta = defaultPromQLLookbackDelta
	}
	if c.MaxPoints <= 0 {
		c.MaxPoints = defaultPromQLMaxPoints
	}
}

// PromQLConfigFromEnv reads PromQL API limits from the process environment,
// falling back to the Prometheus-compatible defaults.
func PromQLConfigFromEnv(dataDir, routePrefix, memoryLimit string, threads int) PromQLConfig {
	cfg := PromQLConfig{
		DataDir:       dataDir,
		RoutePrefix:   routePrefix,
		MaxSamples:    defaultPromQLMaxSamples,
		Timeout:       defaultPromQLTimeout,
		LookbackDelta: defaultPromQLLookbackDelta,
		MaxPoints:     defaultPromQLMaxPoints,
		MemoryLimit:   memoryLimit,
		Threads:       threads,
	}
	if v := os.Getenv("PROMQL_MAX_SAMPLES"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.MaxSamples = n
		}
	}
	if v := os.Getenv("PROMQL_TIMEOUT_SECONDS"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.Timeout = time.Duration(n) * time.Second
		}
	}
	if v := os.Getenv("PROMQL_LOOKBACK_DELTA_SECONDS"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.LookbackDelta = time.Duration(n) * time.Second
		}
	}
	if v := os.Getenv("PROMQL_MAX_POINTS"); v != "" {
		if n, err := parsePositiveInt(v); err == nil {
			cfg.MaxPoints = n
		}
	}
	return cfg
}

// PromQLAPIEnabledFromEnv reports whether the Prometheus API routes should be
// registered. Default is true; the API is metrics-only (it queries the tenant
// metrics view), so logs-only deployments simply never receive PromQL traffic.
func PromQLAPIEnabledFromEnv() bool {
	v := strings.TrimSpace(os.Getenv("PROMQL_API_ENABLED"))
	if v == "" {
		return true
	}
	b, err := parseBool(v)
	if err != nil {
		return true
	}
	return b
}

// PromQLRoutePatterns returns every ServeMux pattern the Prometheus API serves,
// so data nodes and the cluster coordinator mount an identical route set.
func PromQLRoutePatterns(prefix string) []string {
	prefix = strings.TrimSuffix(prefix, "/")
	base := prefix + "/{ns}/api/v1"
	endpoints := []struct {
		methods []string
		path    string
	}{
		{[]string{"GET", "POST"}, "/query"},
		{[]string{"GET", "POST"}, "/query_range"},
		{[]string{"GET", "POST"}, "/series"},
		{[]string{"GET", "POST"}, "/labels"},
		{[]string{"GET"}, "/label/{name}/values"},
	}
	var out []string
	for _, e := range endpoints {
		for _, m := range e.methods {
			out = append(out, m+" "+base+e.path)
		}
	}
	return out
}
