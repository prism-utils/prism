package pipeline_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/components"
	"github.com/elk-utilities/prism/internal/config"
	"github.com/elk-utilities/prism/internal/obs"
	"github.com/elk-utilities/prism/internal/pipeline"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// End-to-end walking-skeleton test: file input (batch) → raw parser → raw
// encoder → file output. Uses real built-in components and the real Default()
// assembler; no fakes. Proves the build/wire/run/drain path works and no
// goroutine leaks (goleak in TestMain).
func TestPipeline_FileToFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	inPath := filepath.Join(dir, "in.log")
	outPath := filepath.Join(dir, "out.raw")

	const content = "line-1\nline-2\nline-3\n"
	if err := os.WriteFile(inPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	cfg := &config.Pipeline{
		Input:   config.Stage{Type: "file", Options: mustJSON(t, map[string]any{"path": inPath, "mode": "batch", "batch_size": 2})},
		Parser:  config.Stage{Type: "raw"},
		Encoder: config.Stage{Type: "raw"},
		Output:  config.Stage{Type: "file", Options: mustJSON(t, map[string]any{"path": outPath})},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config invalid: %v", err)
	}

	reg, err := components.Default()
	if err != nil {
		t.Fatalf("Default registry: %v", err)
	}

	logger := obs.NewLogger(os.Stderr, 0)
	p, err := pipeline.Build(cfg, reg, component.Settings{Logger: logger})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if err := p.Run(context.Background(), obs.NewHost(logger)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	// raw parser is passthrough, raw encoder re-joins with newlines => identity.
	if string(got) != content {
		t.Fatalf("round-trip mismatch:\n got %q\nwant %q", string(got), content)
	}
}

func TestBuild_UnknownTypeErrors(t *testing.T) {
	reg, err := components.Default()
	if err != nil {
		t.Fatalf("Default registry: %v", err)
	}
	cfg := &config.Pipeline{
		Input:   config.Stage{Type: "does-not-exist"},
		Parser:  config.Stage{Type: "raw"},
		Encoder: config.Stage{Type: "raw"},
		Output:  config.Stage{Type: "stdout"},
	}
	if _, err := pipeline.Build(cfg, reg, component.Settings{}); err == nil {
		t.Fatal("Build: expected error for unknown input type, got nil")
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal options: %v", err)
	}
	return b
}
