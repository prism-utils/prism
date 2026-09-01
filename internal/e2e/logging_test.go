package e2e

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/components"
	"github.com/prism-utils/prism/internal/config"
	"github.com/prism-utils/prism/internal/obs"
	"github.com/prism-utils/prism/internal/pipeline"
	"github.com/stretchr/testify/require"
)

const loggingConfig = `
pipelines:
  - name: logs
    input:
      type: file
      options: { path: "${PRISM_LOG}", mode: tail, batch_size: 100 }
    parser:
      type: logs
      options: { format: auto }
    buffer:
      max_age: "200ms"
      max_rows: 100
    branches:
      - name: raw
        encoder: { type: parquet }
        output: { type: dir, options: { dir: "${PRISM_OUT}/logs/raw" } }
      - name: template
        processors:
          - type: template
            options: { source: message, target: template }
        encoder: { type: parquet }
        output: { type: dir, options: { dir: "${PRISM_OUT}/logs/template" } }
      - name: summary
        processors:
          - type: template
            options: { source: message, target: template }
          - type: summary
            options: { group_by: ["template"], aggregates: ["count"] }
        encoder: { type: parquet }
        output: { type: dir, options: { dir: "${PRISM_OUT}/logs/summary" } }
`

// Logging path in three parquet phases: file(tail) → logs(auto) → window buffer
// → { raw → parquet, template → parquet, summary(count by template) → parquet }.
// The sample lines are not a known format, so `logs` keeps them raw (no field
// guessing) and the template is mined from the whole message.
func TestE2E_LoggingThreePhaseParquet(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	// Create the file before Start; ModeTail seeks to EOF so pre-existing
	// content is not re-shipped (append below after the pipeline is running).
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatalf("create log: %v", err)
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

	rawDir := filepath.Join(out, "logs", "raw")
	tmplDir := filepath.Join(out, "logs", "template")
	sumDir := filepath.Join(out, "logs", "summary")
	lines := []string{
		`user 1 logged in from 10.0.0.1`,
		`user 2 logged in from 10.0.0.2`,
		`user 3 request failed code 500`,
	}
	payload := strings.Join(lines, "\n") + "\n"
	// Wait until the tailer is following, then append once. Truncate+rewrite
	// races nxadm/tail under `go test ./...` load (watch reset: file vanished).
	start := time.Now()
	written := false
	require.Eventually(t, func() bool {
		if !written {
			if time.Since(start) < 2*time.Second {
				return false
			}
			f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // G304: test fixture path
			if err != nil {
				return false
			}
			_, werr := f.WriteString(payload)
			_ = f.Sync()
			_ = f.Close()
			if werr != nil {
				return false
			}
			written = true
			return false
		}
		entries, err := os.ReadDir(rawDir)
		return err == nil && hasExt(entries, ".parquet")
	}, 90*time.Second, 100*time.Millisecond)

	waitForFiles(t, tmplDir, ".parquet")
	waitForFiles(t, sumDir, ".parquet")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}

	// raw phase: parsed records, no template column; time-range name.
	rawFile := newestFile(t, rawDir, ".parquet")
	assertRangeName(t, rawFile, "logs", "raw")
	if hasColumn(t, rawFile, "template") {
		t.Fatal("raw phase must not carry a template column")
	}
	// template phase: template column present.
	assertRangeName(t, newestFile(t, tmplDir, ".parquet"), "logs", "template")
	assertHasTemplateColumn(t, newestFile(t, tmplDir, ".parquet"))
	// summary phase: count per template, as parquet.
	assertRangeName(t, newestFile(t, sumDir, ".parquet"), "logs", "summary")
	assertTemplateCounts(t, sumDir)
}

// assertTemplateCounts sums per-template counts across every summary window file
// (tail may cut windows anywhere) and checks the two mined shapes.
func assertTemplateCounts(t *testing.T, sumDir string) {
	t.Helper()
	entries, err := os.ReadDir(sumDir)
	if err != nil {
		t.Fatalf("read summary dir: %v", err)
	}
	totals := map[string]int64{}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".parquet" {
			continue
		}
		for _, r := range readParquetRows(t, filepath.Join(sumDir, e.Name())) {
			totals[r["template"].(string)] += r["count"].(int64)
		}
	}
	login := "user <*> logged in from <*>"
	failed := "user <*> request failed code <*>"
	if totals[login] != 2 || totals[failed] != 1 {
		t.Fatalf("template counts = %v, want %q:2 %q:1", totals, login, failed)
	}
}

// hasColumn reports whether a parquet file has a column of the given name.
func hasColumn(t *testing.T, path, name string) bool {
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
	for i := 0; i < int(tbl.NumCols()); i++ {
		if tbl.Schema().Field(i).Name == name {
			return true
		}
	}
	return false
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
