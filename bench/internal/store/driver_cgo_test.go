//go:build cgo

package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/elk-utilities/prism/bench/internal/gen"
	benchstore "github.com/elk-utilities/prism/bench/internal/store"
	"github.com/stretchr/testify/require"
)

func TestCountLogsLike_engineLevelParquet(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	tenant := "bench-tenant"

	cfg := gen.Config{Seed: 42, MetricsRows: 0, LogsRows: 10_000}
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)

	path := filepath.Join(dataDir, tenant, "bench-logs", "segment.parquet")
	require.NoError(t, gen.WriteLogsTier(path, ds.Logs))

	sd, err := benchstore.New(benchstore.Config{DataDir: dataDir, Tenant: tenant})
	require.NoError(t, err)

	logStart, logEnd := ds.LogsQueryRange()
	got, err := sd.CountLogsLike(ctx, path, logStart, logEnd)
	require.NoError(t, err)
	require.Equal(t, gen.ExpectedDeadlineCount(cfg.LogsRows), got)
}

func TestCountLogsLike_fullScaleQueryRange(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	ctx := context.Background()
	dataDir := t.TempDir()
	tenant := "bench-tenant"

	cfg := gen.ScaleConfig(1)
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)

	path := filepath.Join(dataDir, tenant, "bench-logs", "segment.parquet")
	require.NoError(t, gen.WriteLogsTier(path, ds.Logs))

	sd, err := benchstore.New(benchstore.Config{DataDir: dataDir, Tenant: tenant})
	require.NoError(t, err)

	logStart, logEnd := ds.LogsQueryRange()
	got, err := sd.CountLogsLike(ctx, path, logStart, logEnd)
	require.NoError(t, err)
	require.Equal(t, gen.ExpectedDeadlineCount(cfg.LogsRows), got)

	start, end := ds.QueryRange()
	got2, err := sd.CountLogsLike(ctx, path, start, end)
	require.NoError(t, err)
	require.Equal(t, gen.ExpectedDeadlineCount(cfg.LogsRows), got2)
}
