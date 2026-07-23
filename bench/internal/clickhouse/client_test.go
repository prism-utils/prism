package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/elk-utilities/prism/bench/internal/clickhouse"
	"github.com/elk-utilities/prism/bench/internal/gen"
	"github.com/stretchr/testify/require"
)

func TestCountLogsLike_matchesGeneratedData(t *testing.T) {
	if testing.Short() {
		t.Skip("requires local ClickHouse on :9000")
	}
	ctx := context.Background()
	ch, err := clickhouse.Open(clickhouse.Config{Addr: "127.0.0.1:9000"})
	if err != nil {
		t.Skip("clickhouse not available:", err)
	}
	t.Cleanup(func() { _ = ch.Close() })

	require.NoError(t, ch.InitSchema(ctx))
	require.NoError(t, ch.Truncate(ctx))

	cfg := gen.Config{Seed: 42, MetricsRows: 0, LogsRows: 10_000}
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)
	require.NoError(t, ch.IngestLogs(ctx, ds.Logs))

	start := ds.Logs[0].Ts
	end := ds.Logs[len(ds.Logs)-1].Ts.Add(time.Second)

	got, err := ch.CountLogsLike(ctx, start, end)
	require.NoError(t, err)
	require.Equal(t, gen.ExpectedDeadlineCount(cfg.LogsRows), got)
}

func TestCountLogsLike_withMetricsDatasetQueryRange(t *testing.T) {
	if testing.Short() {
		t.Skip("requires local ClickHouse on :9000")
	}
	ctx := context.Background()
	ch, err := clickhouse.Open(clickhouse.Config{Addr: "127.0.0.1:9000"})
	if err != nil {
		t.Skip("clickhouse not available:", err)
	}
	t.Cleanup(func() { _ = ch.Close() })

	require.NoError(t, ch.InitSchema(ctx))
	require.NoError(t, ch.Truncate(ctx))

	cfg := gen.Config{Seed: 42, MetricsRows: 1000, LogsRows: 1000}
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)
	require.NoError(t, ch.IngestLogs(ctx, ds.Logs))

	start, end := ds.QueryRange()
	got, err := ch.CountLogsLike(ctx, start, end)
	require.NoError(t, err)
	require.Equal(t, gen.ExpectedDeadlineCount(cfg.LogsRows), got)
}
