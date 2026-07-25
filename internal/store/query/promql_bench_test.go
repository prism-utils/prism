package query_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/query"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

// benchSeedPromMetrics writes series×points samples so PromQL benchmarks reflect
// realistic fan-out (many series, a full range each).
func benchSeedPromMetrics(b *testing.B, dataDir string, series, points int) {
	b.Helper()
	rows := make([]testparquet.SegRow, 0, series*points)
	for s := 0; s < series; s++ {
		lbl := fmt.Sprintf(`job="api",instance="host-%d"`, s)
		for p := 0; p < points; p++ {
			ts := promBase.Add(time.Duration(p) * 15 * time.Second)
			rows = append(rows, testparquet.SegRow{
				Name:   "http_requests_total",
				Labels: lbl,
				Value:  float64(p * 10),
				Ts:     ts,
			})
		}
	}
	path := filepath.Join(dataDir, promTenant, "tiers", "L0", "seg.parquet")
	testparquet.WriteSegmentRows(b, path, rows)
}

func benchPromServer(b *testing.B, dataDir string) *httptest.Server {
	b.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &query.PromQLConfig{
		DataDir:       dataDir,
		MaxSamples:    50_000_000,
		Timeout:       30 * time.Second,
		LookbackDelta: 5 * time.Minute,
		MaxPoints:     11_000,
		RunJobs:       false,
	}
	h := query.PromQLHandler(cfg, nil, logger)
	mux := http.NewServeMux()
	for _, p := range query.PromQLRoutePatterns("") {
		mux.Handle(p, h)
	}
	srv := httptest.NewServer(mux)
	b.Cleanup(srv.Close)
	return srv
}

func benchGet(b *testing.B, url string) {
	b.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		b.Fatalf("do: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("status = %d", resp.StatusCode)
	}
}

func BenchmarkPromQLRangeQuery(b *testing.B) {
	dataDir := b.TempDir()
	benchSeedPromMetrics(b, dataDir, 50, 60)
	srv := benchPromServer(b, dataDir)
	start := unixStr(promBase)
	end := unixStr(promBase.Add(59 * 15 * time.Second))
	url := srv.URL + "/" + promTenant + "/api/v1/query_range?query=" +
		urlq("sum(rate(http_requests_total[1m]))") + "&start=" + start + "&end=" + end + "&step=15s"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchGet(b, url)
	}
}

func BenchmarkPromQLInstantQuery(b *testing.B) {
	dataDir := b.TempDir()
	benchSeedPromMetrics(b, dataDir, 50, 60)
	srv := benchPromServer(b, dataDir)
	ts := unixStr(promBase.Add(59 * 15 * time.Second))
	url := srv.URL + "/" + promTenant + "/api/v1/query?query=" +
		urlq("sum(rate(http_requests_total[5m]))") + "&time=" + ts

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchGet(b, url)
	}
}
