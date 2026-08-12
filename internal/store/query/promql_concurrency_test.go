package query_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/segformat"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

// TestPromQLConcurrentQueriesOneTenantDuckDBHot covers a dashboard refresh: many
// panels query one tenant at the same instant, and a writer store exports the
// tenant's hot snapshot per request. With the DuckDB hot format those exports
// must not overlap on the tenant database, or DuckDB rejects the second attach
// and the panel gets a 500.
func TestPromQLConcurrentQueriesOneTenantDuckDBHot(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{
		DataDir:              dataDir,
		HotWindow:            time.Hour,
		HotSegmentFormat:     segformat.DuckDB,
		DuckDBStorageVersion: segformat.DefaultStorageVersion,
	}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	window := testparquet.WriteWindow(t, t.TempDir(), "w.parquet", []testparquet.Row{
		{Name: "up", Labels: `job="api"`, Value: 1, TimestampMs: start.UnixMilli()},
	})
	f, err := os.Open(window)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := eng.Ingest(promTenant, f); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	cfg := promConfig(dataDir, func(c *query.PromQLConfig) { c.RunJobs = true })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	h := query.PromQLHandler(cfg, eng, logger)
	for _, p := range query.PromQLRoutePatterns("") {
		mux.Handle(p, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	const panels = 12
	target := srv.URL + "/" + promTenant + "/api/v1/query?query=" + urlq("up") +
		"&time=" + unixStr(start)
	var wg sync.WaitGroup
	results := make(chan string, panels)
	for i := 0; i < panels; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- queryOutcome(target)
		}()
	}
	wg.Wait()
	close(results)
	for outcome := range results {
		if outcome != "" {
			t.Fatalf("concurrent promql query: %s", outcome)
		}
	}

	leftovers, _ := filepath.Glob(filepath.Join(dataDir, promTenant, "hot", "*.tmp"))
	if len(leftovers) != 0 {
		t.Fatalf("temp snapshot files should not remain: %v", leftovers)
	}
}

// queryOutcome issues one request and returns "" on success or a description of
// the failure, so callers can assert from the test goroutine.
func queryOutcome(target string) string {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
	if err != nil {
		return err.Error()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err.Error()
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Sprintf("status=%d body=%s", resp.StatusCode, body)
	}
	return ""
}
