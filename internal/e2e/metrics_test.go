// Package e2e holds black-box end-to-end tests that drive whole pipelines
// through the real Default() component set, exactly as `prism run` would.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/components"
	"github.com/elk-utilities/prism/internal/config"
	"github.com/elk-utilities/prism/internal/obs"
	"github.com/elk-utilities/prism/internal/pipeline"
)

const metricsExposition = `# HELP http_requests_total total requests
# TYPE http_requests_total counter
http_requests_total{method="get"} 5
http_requests_total{method="post"} 7
`

const metricsConfig = `
pipelines:
  - name: metrics
    input:
      type: prometheus
      options: { targets: ["${PRISM_METRICS_URL}"], interval: "40ms" }
    parser: { type: prometheus }
    buffer: { max_rows: 1 }
    branches:
      - name: data
        encoder: { type: parquet }
        output: { type: dir, options: { dir: "${PRISM_OUT}/data", prefix: "m-" } }
      - name: summary
        processors:
          - type: summary
            options: { group_by: ["__name__"], aggregates: ["count", "sum:value", "avg:value"] }
        encoder: { type: json }
        output: { type: dir, options: { dir: "${PRISM_OUT}/summary", prefix: "s-" } }
`

// Metrics path: prometheus scrape → prometheus parse → window buffer →
// {parquet → dir, summary → json → dir}. Asserts both sinks materialize with
// the expected data.
func TestE2E_MetricsToParquetAndSummary(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(metricsExposition))
	}))
	defer srv.Close()

	out := t.TempDir()
	t.Setenv("PRISM_METRICS_URL", srv.URL)
	t.Setenv("PRISM_OUT", out)

	cfg, err := config.LoadConfig(strings.NewReader(metricsConfig))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	reg, err := components.Default()
	if err != nil {
		t.Fatalf("Default: %v", err)
	}
	logger := obs.NewLogger(os.Stderr, 0)
	set, err := pipeline.Build(cfg, reg, component.Settings{Logger: logger})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- set.Run(ctx, obs.NewHost(logger)) }()

	dataDir := filepath.Join(out, "data")
	sumDir := filepath.Join(out, "summary")
	waitForFiles(t, dataDir, ".parquet")
	waitForFiles(t, sumDir, ".json")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	assertParquetRows(t, newestFile(t, dataDir, ".parquet"), 2)
	assertSummary(t, newestFile(t, sumDir, ".json"))
}

func waitForFiles(t *testing.T, dir, ext string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if entries, _ := os.ReadDir(dir); hasExt(entries, ext) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no %s file appeared in %s within timeout", ext, dir)
}

func hasExt(entries []os.DirEntry, ext string) bool {
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ext {
			return true
		}
	}
	return false
}

func newestFile(t *testing.T, dir, ext string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	var newest string
	var mod time.Time
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ext {
			continue
		}
		info, _ := e.Info()
		if info.ModTime().After(mod) {
			mod = info.ModTime()
			newest = filepath.Join(dir, e.Name())
		}
	}
	if newest == "" {
		t.Fatalf("no %s file in %s", ext, dir)
	}
	return newest
}

func assertParquetRows(t *testing.T, path string, wantRows int) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	rdr, err := file.NewParquetReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("open parquet %s: %v", path, err)
	}
	defer func() { _ = rdr.Close() }()
	pr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("arrow reader: %v", err)
	}
	tbl, err := pr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	defer tbl.Release()
	if int(tbl.NumRows()) != wantRows {
		t.Fatalf("parquet rows = %d, want %d", tbl.NumRows(), wantRows)
	}
	if tbl.Schema().Field(0).Name != "__name__" {
		t.Fatalf("first parquet column = %q, want __name__", tbl.Schema().Field(0).Name)
	}
}

func assertSummary(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(b, &rows); err != nil {
		t.Fatalf("summary not valid JSON: %v\n%s", err, b)
	}
	if len(rows) != 1 {
		t.Fatalf("summary groups = %d, want 1 (single __name__)", len(rows))
	}
	r := rows[0]
	if r["__name__"] != "http_requests_total" {
		t.Fatalf("group name = %v, want http_requests_total", r["__name__"])
	}
	if r["count"].(float64) != 2 || r["sum_value"].(float64) != 12 || r["avg_value"].(float64) != 6 {
		t.Fatalf("aggregates wrong: %+v", r)
	}
}
