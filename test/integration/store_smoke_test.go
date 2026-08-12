//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/prism-utils/prism/internal/store/admin"
	"github.com/prism-utils/prism/internal/store/engine"
	storeingest "github.com/prism-utils/prism/internal/store/ingest"
	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const smokeTenant = "user-6f3a9c2b-apps"

func TestStoreIngestFlushQueryStatsSmoke(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	hotWindow := time.Minute
	now := start
	clock := func() time.Time { return now }

	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: hotWindow}, clock)
	t.Cleanup(func() { require.NoError(t, eng.Close()) })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingestCfg := storeingest.Config{
		AllowedArtifacts: []string{"metrics-raw"},
		MaxBodyBytes:     1 << 20,
		AuthMode:         storeingest.AuthNone,
	}
	adminCfg := &admin.Config{
		DataDir:          dataDir,
		AllowedArtifacts: []string{"metrics-raw"},
	}
	queryCfg := &query.Config{DataDir: dataDir}

	mux := http.NewServeMux()
	mux.Handle(storeingest.IngestRoutePattern(""), storeingest.Handler(&ingestCfg, eng, logger))
	mux.Handle(admin.StatsRoutePattern(), admin.StatsHandler(adminCfg, eng))
	mux.Handle(query.QueryRoutePattern(""), query.Handler(queryCfg, eng, logger))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	parquetDir := t.TempDir()
	path := testparquet.WriteWindow(t, parquetDir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 42, TimestampMs: 0},
	})
	body, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = body.Close() })

	ingestURL := srv.URL + "/" + smokeTenant + "/ingest/metrics-raw"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ingestURL, body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	hotCount, err := eng.HotRowCount(smokeTenant)
	require.NoError(t, err)
	require.Equal(t, int64(1), hotCount)

	now = start.Add(hotWindow)
	require.NoError(t, eng.FlushDue())

	startS := start.Format(time.RFC3339)
	endS := start.Add(2 * hotWindow).Format(time.RFC3339)
	queryURL := srv.URL + "/" + smokeTenant + "/query?start=" + startS + "&end=" + endS
	qResp, err := http.Get(queryURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = qResp.Body.Close() })
	require.Equal(t, http.StatusOK, qResp.StatusCode)

	qBody, err := io.ReadAll(qResp.Body)
	require.NoError(t, err)
	require.Contains(t, string(qBody), `"rows"`)
	require.Contains(t, string(qBody), `"up"`)

	statsResp, err := http.Get(srv.URL + "/stats?ns=" + smokeTenant)
	require.NoError(t, err)
	t.Cleanup(func() { _ = statsResp.Body.Close() })
	require.Equal(t, http.StatusOK, statsResp.StatusCode)

	var stats admin.StatsResponse
	require.NoError(t, json.NewDecoder(statsResp.Body).Decode(&stats))
	require.GreaterOrEqual(t, stats.TotalWindows, 1)
	require.GreaterOrEqual(t, stats.Artifacts["metrics-raw"].Windows, 1)
}

func TestStoreHealthEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	dataDir := t.TempDir()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if err := os.MkdirAll(dataDir, 0o750); err != nil {
			http.Error(w, "data dir not writable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		require.NoError(t, err)
		body, readErr := io.ReadAll(resp.Body)
		require.NoError(t, readErr)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.True(t, strings.HasSuffix(string(body), "\n"))
	}
}
