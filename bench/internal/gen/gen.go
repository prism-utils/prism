// Package gen produces deterministic seeded metrics and logs datasets for the
// store vs ClickHouse benchmark harness.
package gen

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	// DefaultSeed is the fixed RNG seed for reproducible benchmark runs.
	DefaultSeed = 42
	// DefaultMetricsRows is the default metrics row count (~half of 2M profile).
	DefaultMetricsRows = 1_000_000
	// DefaultLogsRows is the default logs row count (~half of 2M profile).
	DefaultLogsRows = 1_000_000
	// MetricCardinality is the number of distinct metric series names.
	MetricCardinality = 200
	// DeadlineSubstring is embedded in log messages at a fixed frequency.
	DeadlineSubstring = "deadline exceeded"
	// DeadlineEveryN marks every Nth log row with DeadlineSubstring.
	DeadlineEveryN = 100
	// DefaultSpan is the synthetic time range width for generated timestamps.
	DefaultSpan = 7 * 24 * time.Hour
)

// Config controls deterministic dataset generation.
type Config struct {
	Seed        int64
	MetricsRows int64
	LogsRows    int64
	Start       time.Time
	Span        time.Duration
}

// MetricRow is one metrics-raw sample (contract v1 columns without ingest ts).
type MetricRow struct {
	Name        string
	Labels      string
	Value       float64
	TimestampMs int64
	Ts          time.Time
}

// LogRow is one synthetic log record for the logs LIKE workload.
type LogRow struct {
	Ts      time.Time
	Level   string
	Service string
	Message string
}

// Dataset holds generated rows shared by both benchmark backends.
type Dataset struct {
	Metrics []MetricRow
	Logs    []LogRow
	metrics map[string]struct{}
}

// DefaultConfig returns the small laptop/CI profile.
func DefaultConfig() Config {
	return Config{
		Seed:        DefaultSeed,
		MetricsRows: DefaultMetricsRows,
		LogsRows:    DefaultLogsRows,
		Start:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Span:        DefaultSpan,
	}
}

// ScaleConfig multiplies row counts by scale while keeping seed and span fixed.
func ScaleConfig(scale int) Config {
	if scale < 1 {
		scale = 1
	}
	cfg := DefaultConfig()
	cfg.MetricsRows = DefaultMetricsRows * int64(scale)
	cfg.LogsRows = DefaultLogsRows * int64(scale)
	return cfg
}

// ExpectedDeadlineCount returns the deterministic LIKE match count for n logs.
func ExpectedDeadlineCount(logs int64) int64 {
	if logs <= 0 {
		return 0
	}
	return logs / DeadlineEveryN
}

// Generate builds metrics and logs rows from cfg using a seeded RNG.
func Generate(cfg Config) (*Dataset, error) {
	if cfg.MetricsRows < 0 || cfg.LogsRows < 0 {
		return nil, fmt.Errorf("gen: negative row count")
	}
	if cfg.Span <= 0 {
		cfg.Span = DefaultSpan
	}
	if cfg.Start.IsZero() {
		cfg.Start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	rng := rand.New(rand.NewSource(cfg.Seed)) //nolint:gosec // deterministic bench fixture

	levels := []string{"debug", "info", "warn", "error"}
	services := []string{"api", "worker", "gateway", "scheduler", "store"}
	templates := []string{
		"request completed status=%d latency=%dms",
		"connection reset by peer host=%s",
		"retry attempt %d for operation %s",
		"cache miss key=%s",
		"handler error: %s",
	}

	ds := &Dataset{
		Metrics: make([]MetricRow, 0, cfg.MetricsRows),
		Logs:    make([]LogRow, 0, cfg.LogsRows),
		metrics: make(map[string]struct{}, MetricCardinality),
	}

	for i := int64(0); i < cfg.MetricsRows; i++ {
		nameIdx := int(i % MetricCardinality)
		name := fmt.Sprintf("bench_metric_%03d", nameIdx)
		ds.metrics[name] = struct{}{}
		ts := cfg.Start.Add(spanOffset(cfg.Span, cfg.MetricsRows, i))
		ds.Metrics = append(ds.Metrics, MetricRow{
			Name:        name,
			Labels:      fmt.Sprintf(`{"pod":"p%d"}`, nameIdx%20),
			Value:       rng.Float64() * 1000,
			TimestampMs: ts.UnixMilli(),
			Ts:          ts.UTC(),
		})
	}

	for i := int64(0); i < cfg.LogsRows; i++ {
		ts := cfg.Start.Add(spanOffset(cfg.Span, cfg.LogsRows, i))
		msg := fmt.Sprintf(templates[rng.Intn(len(templates))], rng.Intn(500), rng.Intn(1000))
		if (i+1)%DeadlineEveryN == 0 {
			msg = fmt.Sprintf("rpc error: %s upstream=%s", DeadlineSubstring, services[rng.Intn(len(services))])
		}
		ds.Logs = append(ds.Logs, LogRow{
			Ts:      ts.UTC(),
			Level:   levels[rng.Intn(len(levels))],
			Service: services[rng.Intn(len(services))],
			Message: msg,
		})
	}
	return ds, nil
}

// DeadlineCount returns how many log messages contain DeadlineSubstring.
func (d *Dataset) DeadlineCount() int64 {
	var n int64
	for _, row := range d.Logs {
		if containsDeadline(row.Message) {
			n++
		}
	}
	return n
}

func containsDeadline(msg string) bool {
	return len(msg) >= len(DeadlineSubstring) && stringContains(msg, DeadlineSubstring)
}

func stringContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// MetricNames returns distinct metric names present in the dataset.
func (d *Dataset) MetricNames() []string {
	out := make([]string, 0, len(d.metrics))
	for name := range d.metrics {
		out = append(out, name)
	}
	return out
}

// QueryRange returns inclusive start and exclusive end for ClickHouse metrics queries.
func (d *Dataset) QueryRange() (time.Time, time.Time) {
	switch {
	case len(d.Metrics) > 0:
		return d.Metrics[0].Ts, d.Metrics[len(d.Metrics)-1].Ts.Add(time.Second)
	case len(d.Logs) > 0:
		return d.LogsQueryRange()
	default:
		return time.Time{}, time.Time{}
	}
}

// LogsQueryRange returns inclusive start and exclusive end covering all log rows.
func (d *Dataset) LogsQueryRange() (time.Time, time.Time) {
	if len(d.Logs) == 0 {
		return time.Time{}, time.Time{}
	}
	return d.Logs[0].Ts, d.Logs[len(d.Logs)-1].Ts.Add(time.Second)
}

func spanOffset(span time.Duration, rows, i int64) time.Duration {
	if rows <= 0 {
		return 0
	}
	step := time.Duration(int64(span) / rows)
	if step <= 0 {
		step = 1
	}
	return step * time.Duration(i)
}
