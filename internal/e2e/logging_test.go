package e2e

import (
	"bytes"
	"context"
	"encoding/json"
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

const loggingConfig = `
pipelines:
  - name: logs
    input:
      type: file
      options: { path: "${PRISM_LOG}", mode: tail, batch_size: 100 }
    parser: { type: logfmt }
    processors:
      - type: template
        options: { source: msg, target: template }
    buffer:
      max_age: "200ms"
      max_rows: 100
    branches:
      - name: data
        encoder: { type: parquet }
        output: { type: dir, options: { dir: "${PRISM_OUT}/logs/data", prefix: "l-" } }
      - name: summary
        processors:
          - type: summary
            options: { group_by: ["level"], aggregates: ["count"] }
        encoder: { type: json }
        output: { type: dir, options: { dir: "${PRISM_OUT}/logs/summary", prefix: "s-" } }
`

// Logging path: file(tail) → logfmt → template → window buffer →
// {parquet → dir, summary(count by level) → json → dir}.
func TestE2E_LoggingToParquetAndSummary(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	lines := []string{
		`level=info msg="user 1 logged in from 10.0.0.1" status=200`,
		`level=info msg="user 2 logged in from 10.0.0.2" status=200`,
		`level=error msg="user 3 request failed code 500" status=500`,
	}
	if err := os.WriteFile(logPath, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}

	out := filepath.Join(dir, "out")
	t.Setenv("PRISM_LOG", logPath)
	t.Setenv("PRISM_OUT", out)

	cfg, err := config.LoadConfig(strings.NewReader(loggingConfig))
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

	dataDir := filepath.Join(out, "logs", "data")
	sumDir := filepath.Join(out, "logs", "summary")
	waitForFiles(t, dataDir, ".parquet")
	waitForFiles(t, sumDir, ".json")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	assertHasTemplateColumn(t, newestFile(t, dataDir, ".parquet"))
	assertLevelCounts(t, sumDir)
}

func assertHasTemplateColumn(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}
	rdr, err := file.NewParquetReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
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
	found := false
	for i := 0; i < int(tbl.NumCols()); i++ {
		if tbl.Schema().Field(i).Name == "template" {
			found = true
		}
	}
	if !found {
		t.Fatalf("parquet has no template column; schema=%v", tbl.Schema())
	}
}

// assertLevelCounts sums per-level counts across every summary window file and
// checks the totals (2 info + 1 error), since tail may cut windows anywhere.
func assertLevelCounts(t *testing.T, sumDir string) {
	t.Helper()
	entries, err := os.ReadDir(sumDir)
	if err != nil {
		t.Fatalf("read summary dir: %v", err)
	}
	totals := map[string]int64{}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(sumDir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(b, &rows); err != nil {
			t.Fatalf("summary %s not valid JSON: %v", e.Name(), err)
		}
		for _, r := range rows {
			totals[r["level"].(string)] += int64(r["count"].(float64))
		}
	}
	if totals["info"] != 2 || totals["error"] != 1 {
		t.Fatalf("level counts = %v, want info:2 error:1", totals)
	}
}
