package query

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Loki API defaults. MaxEntries mirrors Loki's max_entries_limit_per_query so one
// request cannot stream an unbounded number of log lines; DefaultLimit is Loki's
// own default page size for a query with no explicit limit.
const (
	defaultLokiMaxEntries = 5_000
	defaultLokiTimeout    = 30 * time.Second
	defaultLokiLimit      = 100
	// defaultLokiRange is the window used when a request omits start/end, the
	// same "last hour" default Grafana falls back to.
	defaultLokiRange = time.Hour
)

// LokiConfig holds settings for the Loki-compatible logs read API.
type LokiConfig struct {
	DataDir     string
	ColdDir     string
	RoutePrefix string
	// MaxEntries caps the log entries a single query may return.
	MaxEntries int
	// Timeout bounds a single query's wall-clock execution.
	Timeout time.Duration
	// MemoryLimit / Threads apply the shared DuckDB governance to the sandbox.
	MemoryLimit string
	Threads     int
	// RecentLookback, when >0, bounds label/browse open sets that omit an
	// explicit time range to files within now-lookback (cold history still
	// reachable when start/end cover it).
	RecentLookback time.Duration
}

// Validate reports configuration errors with the offending field named.
func (c *LokiConfig) Validate() error {
	if c.MaxEntries <= 0 {
		return fmt.Errorf("loki.max_entries: must be > 0")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("loki.timeout: must be > 0")
	}
	return nil
}

func (c *LokiConfig) withDefaults() {
	if c.MaxEntries <= 0 {
		c.MaxEntries = defaultLokiMaxEntries
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultLokiTimeout
	}
}

// LokiAPIEnabledFromEnv reports whether the Loki API routes should be registered.
// Default is true; the API is logs-only (it queries the tenant logs relation), so
// metrics-only deployments simply never receive Loki traffic.
func LokiAPIEnabledFromEnv() bool {
	v := strings.TrimSpace(os.Getenv("LOKI_API_ENABLED"))
	if v == "" {
		return true
	}
	b, err := parseBool(v)
	if err != nil {
		return true
	}
	return b
}

// LokiRoutePatterns returns every ServeMux pattern the Loki API serves, so data
// nodes and the cluster coordinator mount an identical route set.
func LokiRoutePatterns(prefix string) []string {
	prefix = strings.TrimSuffix(prefix, "/")
	base := prefix + "/{ns}/loki/api/v1"
	paths := []string{"/query_range", "/labels", "/label/{name}/values"}
	out := make([]string, 0, len(paths)*2)
	for _, p := range paths {
		out = append(out, "GET "+base+p, "POST "+base+p)
	}
	return out
}

// parseLokiTimeNanos parses a Loki time parameter into Unix nanoseconds. Loki
// accepts a nanosecond Unix epoch, a (fractional) second epoch, or an RFC3339
// string; an empty value yields def.
func parseLokiTimeNanos(s string, def int64) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return def, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	if dot := strings.IndexByte(s, '.'); dot > 0 {
		if ns, ok := fractionalSecondsToNanos(s[:dot], s[dot+1:]); ok {
			return ns, nil
		}
	}
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("invalid time %q", s)
	}
	return ts.UnixNano(), nil
}

// fractionalSecondsToNanos converts a "<seconds>.<fraction>" epoch into
// nanoseconds with exact integer arithmetic, so a sub-second bound is not
// perturbed by float rounding.
func fractionalSecondsToNanos(secPart, fracPart string) (int64, bool) {
	sec, err := strconv.ParseInt(secPart, 10, 64)
	if err != nil {
		return 0, false
	}
	const nanoDigits = 9
	if len(fracPart) > nanoDigits {
		fracPart = fracPart[:nanoDigits]
	}
	frac := int64(0)
	for i := 0; i < nanoDigits; i++ {
		digit := int64(0)
		if i < len(fracPart) {
			if fracPart[i] < '0' || fracPart[i] > '9' {
				return 0, false
			}
			digit = int64(fracPart[i] - '0')
		}
		frac = frac*10 + digit
	}
	if sec < 0 {
		return sec*int64(time.Second) - frac, true
	}
	return sec*int64(time.Second) + frac, true
}
