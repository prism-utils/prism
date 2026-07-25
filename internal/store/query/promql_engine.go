package query

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// newPromQLEngine builds a Prometheus PromQL engine bounded by the config. The
// engine is safe for concurrent reuse across requests; each request supplies its
// own storage.Queryable (a per-request DuckDB sandbox), so no state leaks
// between tenants.
func newPromQLEngine(cfg *PromQLConfig, logger *slog.Logger) *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Logger:               logger,
		MaxSamples:           cfg.MaxSamples,
		Timeout:              cfg.Timeout,
		LookbackDelta:        cfg.LookbackDelta,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})
}

// execInstant runs an instant query at ts against the sandbox-backed storage.
func execInstant(ctx context.Context, eng *promql.Engine, q storage.Queryable, expr string, ts time.Time) (*promql.Result, error) {
	query, err := eng.NewInstantQuery(ctx, q, nil, expr, ts)
	if err != nil {
		return nil, err
	}
	defer query.Close()
	res := query.Exec(ctx)
	return res, nil
}

// execRange runs a range query over [start,end] at the given step.
func execRange(ctx context.Context, eng *promql.Engine, q storage.Queryable, expr string, start, end time.Time, step time.Duration) (*promql.Result, error) {
	query, err := eng.NewRangeQuery(ctx, q, nil, expr, start, end, step)
	if err != nil {
		return nil, err
	}
	defer query.Close()
	res := query.Exec(ctx)
	return res, nil
}
