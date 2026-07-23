//go:build cgo

package store_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/bench/internal/authgen"
	"github.com/elk-utilities/prism/bench/internal/gen"
	benchstore "github.com/elk-utilities/prism/bench/internal/store"
	"github.com/stretchr/testify/require"
)

func TestDriverArrowAPIRoundTrip(t *testing.T) {
	if raceBuild() {
		t.Skip("arrow parquet writer reports races under -race; run without race detector")
	}

	storeBin, err := locateStoreBinary()
	if err != nil {
		t.Skip(err)
	}

	ctx := context.Background()
	dataDir := t.TempDir()
	authDir := t.TempDir()
	tenant := "bench-tenant"

	cfg := gen.Config{Seed: 42, MetricsRows: 500, LogsRows: 0}
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)

	metricsDir := filepath.Join(dataDir, "metrics-windows")
	windows, err := gen.WriteMetricsWindows(metricsDir, ds.Metrics, 500)
	require.NoError(t, err)

	env, err := authgen.New(authDir, tenant)
	require.NoError(t, err)

	policy := authgen.PolicyYAML([]authgen.Binding{
		{Subject: authgen.AdminSubject, Role: "admin", Tenants: []string{tenant}},
	})
	require.NoError(t, env.WritePolicy(policy))

	adminTok, err := env.Token()
	require.NoError(t, err)

	sd, err := benchstore.New(benchstore.Config{
		DataDir:  dataDir,
		Tenant:   tenant,
		StoreBin: storeBin,
		RBAC: &benchstore.RBACConfig{
			PolicyFile: env.PolicyPath(),
			Issuer:     env.Issuer(),
			JWKSFile:   env.JWKSPath(),
			Audience:   env.Audience(),
		},
		Token: adminTok,
	})
	require.NoError(t, err)

	require.NoError(t, sd.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = sd.Stop(stopCtx)
	})

	require.NoError(t, sd.IngestMetricsHTTP(ctx, windows))
	require.NoError(t, sd.Compact(ctx))

	stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	require.NoError(t, sd.StopServer(stopCtx))
	cancel()

	require.NoError(t, sd.StartServer(ctx))

	count, err := sd.CountMetricsArrowAPI(ctx)
	require.NoError(t, err)
	require.Equal(t, cfg.MetricsRows, count)

	require.NoError(t, sd.AggregateMetricsArrowAPI(ctx))

	scanRows, err := sd.ScanMetricsArrowAPI(ctx, `SELECT "__name__", value FROM metrics LIMIT 100`)
	require.NoError(t, err)
	require.Equal(t, int64(100), scanRows)
}
