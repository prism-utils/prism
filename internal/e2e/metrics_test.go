// Package e2e holds black-box end-to-end tests that drive whole pipelines
// through the real Default() component set, exactly as `prism run` would.
package e2e

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/components"
	"github.com/prism-utils/prism/internal/config"
	"github.com/prism-utils/prism/internal/obs"
	"github.com/prism-utils/prism/internal/pipeline"
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
      - name: raw
        encoder: { type: parquet }
        output: { type: dir, options: { dir: "${PRISM_OUT}/metrics/raw" } }
`

// Metrics path: prometheus scrape → prometheus parse → window buffer → parquet
// → dir. No summary branch (server-side analytics aggregate the columnar parquet
// directly). Asserts the raw sink materializes with the expected data and a
// time-range-encoded file name.
func TestE2E_MetricsToParquet(t *testing.T) {
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

	dataDir := filepath.Join(out, "metrics", "raw")
	waitForFiles(t, dataDir, ".parquet")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	newest := newestFile(t, dataDir, ".parquet")
	assertParquetRows(t, newest, 3) // exposition samples + synthetic scrape `up`
	assertRangeName(t, newest, "metrics", "raw")
}

// assertRangeName checks the file follows <pipeline>-<phase>-<start>-<end>-<seq>
// with sortable UTC window bounds embedded in the name.
func assertRangeName(t *testing.T, path, pipeline, phase string) {
	t.Helper()
	name := filepath.Base(path)
	prefix := pipeline + "-" + phase + "-"
	if !strings.HasPrefix(name, prefix) {
		t.Fatalf("name %q does not start with %q", name, prefix)
	}
	base := strings.TrimSuffix(name, filepath.Ext(name))
	parts := strings.Split(base, "-")
	if len(parts) < 5 {
		t.Fatalf("name %q lacks the range components", name)
	}
	start, end := parts[len(parts)-3], parts[len(parts)-2]
	if start > end {
		t.Fatalf("window start %q is after end %q in %q", start, end, name)
	}
	if _, err := time.Parse("20060102T150405.000000000Z", start); err != nil {
		t.Fatalf("start %q is not a compact UTC timestamp: %v", start, err)
	}
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

// readParquetRows reads a Parquet file into row maps (string/int64/float64
// values), for asserting small summary/aggregate outputs.
func readParquetRows(t *testing.T, path string) []map[string]any {
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

	rows := make([]map[string]any, tbl.NumRows())
	for i := range rows {
		rows[i] = map[string]any{}
	}
	for c := 0; c < int(tbl.NumCols()); c++ {
		name := tbl.Schema().Field(c).Name
		col := tbl.Column(c)
		r := 0
		for _, chunk := range col.Data().Chunks() {
			for j := 0; j < chunk.Len(); j++ {
				switch a := chunk.(type) {
				case *array.String:
					rows[r][name] = a.Value(j)
				case *array.Int64:
					rows[r][name] = a.Value(j)
				case *array.Float64:
					rows[r][name] = a.Value(j)
				}
				r++
			}
		}
	}
	return rows
}
