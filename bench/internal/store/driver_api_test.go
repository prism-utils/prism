//go:build cgo

package store_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/bench/internal/authgen"
	"github.com/elk-utilities/prism/bench/internal/gen"
	benchstore "github.com/elk-utilities/prism/bench/internal/store"
	"github.com/stretchr/testify/require"
)

func TestDriverAPIRBACIntegration(t *testing.T) {
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

	otherTenant := "other-tenant-abc"
	policy := authgen.PolicyYAML([]authgen.Binding{
		{Subject: authgen.AdminSubject, Role: "admin", Tenants: []string{tenant}},
		{Subject: "reader-a", Role: "reader", Tenants: []string{tenant}},
		{Subject: "writer-a", Role: "writer", Tenants: []string{tenant}},
		{Subject: "reader-b", Role: "reader", Tenants: []string{otherTenant}},
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

	count, err := sd.CountMetricsAPI(ctx)
	require.NoError(t, err)
	require.Equal(t, cfg.MetricsRows, count)

	require.NoError(t, sd.AggregateMetricsAPI(ctx))

	base := sd.BaseURL()
	sqlURL := base + "/" + tenant + "/sql"
	countSQL := `{"sql":"SELECT COUNT(*) AS n FROM metrics"}`

	readerTok, err := env.TokenFor("reader-a", time.Hour)
	require.NoError(t, err)
	resp := postSQL(ctx, t, sqlURL, countSQL, readerTok)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = resp.Body.Close()

	writerTok, err := env.TokenFor("writer-a", time.Hour)
	require.NoError(t, err)
	resp = postSQL(ctx, t, sqlURL, countSQL, writerTok)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	_ = resp.Body.Close()

	readerOtherTok, err := env.TokenFor("reader-b", time.Hour)
	require.NoError(t, err)
	resp = postSQL(ctx, t, sqlURL, countSQL, readerOtherTok)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	_ = resp.Body.Close()

	resp = postSQL(ctx, t, sqlURL, countSQL, "")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	_ = resp.Body.Close()
}

func postSQL(ctx context.Context, t *testing.T, url, body, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func locateStoreBinary() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	bin := filepath.Join(root, "bin", "prism-store")
	if _, err := os.Stat(bin); err == nil {
		return bin, nil
	}
	build := exec.CommandContext(context.Background(), "go", "build", "-tags", "duckdb_arrow", "-o", bin, "./cmd/prism-store")
	build.Dir = root
	build.Env = append(os.Environ(), "CGO_ENABLED=1")
	out, err := build.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("build prism-store: %w: %s", err, out)
	}
	return bin, nil
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = parent
	}
}

func raceBuild() bool {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return false
	}
	for _, s := range info.Settings {
		if s.Key == "-race" && s.Value == "true" {
			return true
		}
	}
	return false
}
