package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/components"
	"github.com/elk-utilities/prism/internal/config"
	"github.com/elk-utilities/prism/internal/obs"
	"github.com/elk-utilities/prism/internal/pipeline"
)

// TestQuickPresetBuildsPipeline proves the logs preset resolves and wires
// through the real component registry + pipeline builder (not just validates).
func TestQuickPresetBuildsPipeline(t *testing.T) {
	cfg, err := runConfig(&runOptions{quickTemplate: "logs"})
	if err != nil {
		t.Fatalf("runConfig: %v", err)
	}
	reg, err := components.Default()
	if err != nil {
		t.Fatalf("components.Default: %v", err)
	}
	logger := obs.NewLogger(bytes.NewBuffer(nil), 0)
	if _, err := pipeline.Build(cfg, reg, component.Settings{Logger: logger}); err != nil {
		t.Fatalf("pipeline.Build on quick preset: %v", err)
	}
}

func TestQuickAndConfigMutuallyExclusive(t *testing.T) {
	_, err := runConfig(&runOptions{quickTemplate: "logs", configPath: "prism.yaml"})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("err = %v, want mutually exclusive", err)
	}
}

func TestRunRequiresConfigOrQuick(t *testing.T) {
	_, err := runConfig(&runOptions{})
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("err = %v, want required", err)
	}
}

func TestPrintEffectiveConfigRoundTrips(t *testing.T) {
	cfg, err := runConfig(&runOptions{quickTemplate: "logs", store: "http://store:8080", tenant: "team-a"})
	if err != nil {
		t.Fatalf("runConfig: %v", err)
	}
	var buf bytes.Buffer
	if err := printEffectiveConfig(&buf, cfg); err != nil {
		t.Fatalf("printEffectiveConfig: %v", err)
	}
	// The printed config must be valid JSON and reload back into a valid config
	// (durations/byte sizes serialize in a reloadable form).
	if !json.Valid(buf.Bytes()) {
		t.Fatalf("printed config is not valid JSON:\n%s", buf.String())
	}
	reloaded, err := config.LoadConfig(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("reload printed config: %v", err)
	}
	if len(reloaded.Pipelines) != 1 || len(reloaded.Pipelines[0].Branches) != 2 {
		t.Fatalf("reloaded config lost structure: %+v", reloaded.Pipelines)
	}
}
