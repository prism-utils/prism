package config_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/config"
)

const yamlDoc = `
pipelines:
  - name: metrics
    input:
      type: prometheus
      options: { targets: ["http://localhost:9100/metrics"], interval: 15s }
    parser:
      type: prometheus
    buffer:
      max_age: 30s
      max_bytes: 12MiB
    branches:
      - name: data
        encoder: { type: parquet }
        output:  { type: file, options: { dir: /var/lib/prism/metrics } }
      - name: summary
        processors:
          - type: summary
            options: { group_by: ["__name__"], aggregates: ["count"] }
        encoder: { type: json }
        output:  { type: file, options: { dir: /var/lib/prism/metrics-summary } }
`

const jsonDoc = `{
  "pipelines": [
    {
      "name": "metrics",
      "input":  {"type": "prometheus", "options": {"targets": ["http://localhost:9100/metrics"], "interval": "15s"}},
      "parser": {"type": "prometheus"},
      "buffer": {"max_age": "30s", "max_bytes": "12MiB"},
      "branches": [
        {"name": "data", "encoder": {"type": "parquet"}, "output": {"type": "file", "options": {"dir": "/var/lib/prism/metrics"}}},
        {"name": "summary",
         "processors": [{"type": "summary", "options": {"group_by": ["__name__"], "aggregates": ["count"]}}],
         "encoder": {"type": "json"},
         "output": {"type": "file", "options": {"dir": "/var/lib/prism/metrics-summary"}}}
      ]
    }
  ]
}`

func TestLoadConfig_YAMLEqualsJSON(t *testing.T) {
	t.Parallel()
	fromYAML, err := config.LoadConfig(strings.NewReader(yamlDoc))
	if err != nil {
		t.Fatalf("LoadConfig(yaml): %v", err)
	}
	fromJSON, err := config.LoadConfig(strings.NewReader(jsonDoc))
	if err != nil {
		t.Fatalf("LoadConfig(json): %v", err)
	}
	if !reflect.DeepEqual(fromYAML, fromJSON) {
		t.Fatalf("YAML and JSON configs differ:\n yaml=%+v\n json=%+v", fromYAML, fromJSON)
	}
	p := fromYAML.Pipelines[0]
	if p.Name != "metrics" || p.Input.Type != "prometheus" {
		t.Fatalf("unexpected pipeline: %+v", p)
	}
	if time.Duration(p.Buffer.MaxAge) != 30*time.Second {
		t.Fatalf("buffer max_age = %v, want 30s", time.Duration(p.Buffer.MaxAge))
	}
	if int64(p.Buffer.MaxBytes) != 12*1024*1024 {
		t.Fatalf("buffer max_bytes = %d, want 12MiB", int64(p.Buffer.MaxBytes))
	}
	if len(p.Branches) != 2 || p.Branches[1].Encoder.Type != "json" {
		t.Fatalf("branches not preserved: %+v", p.Branches)
	}
}

func TestLoadConfig_EnvInterpolation(t *testing.T) {
	t.Setenv("PRISM_DIR", "/data/out")
	const doc = `
pipelines:
  - name: p
    input: { type: stdin }
    parser: { type: raw }
    branches:
      - name: data
        encoder: { type: raw }
        output: { type: file, options: { dir: "${PRISM_DIR}/logs" } }
`
	cfg, err := config.LoadConfig(strings.NewReader(doc))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	got := string(cfg.Pipelines[0].Branches[0].Output.Options)
	if !strings.Contains(got, "/data/out/logs") {
		t.Fatalf("env not interpolated in options: %s", got)
	}
}

func TestLoadConfig_AppliesBufferDefaults(t *testing.T) {
	t.Parallel()
	const doc = `
pipelines:
  - name: p
    input: { type: stdin }
    parser: { type: raw }
    branches:
      - name: data
        encoder: { type: raw }
        output: { type: stdout }
`
	cfg, err := config.LoadConfig(strings.NewReader(doc))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	b := cfg.Pipelines[0].Buffer
	if time.Duration(b.MaxAge) != 30*time.Second {
		t.Fatalf("default max_age = %v, want 30s", time.Duration(b.MaxAge))
	}
	if int64(b.MaxBytes) != 12*1024*1024 {
		t.Fatalf("default max_bytes = %d, want 12MiB", int64(b.MaxBytes))
	}
}

func TestLoadConfig_ValidateErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		doc     string
		wantSub string
	}{
		{
			name:    "no pipelines",
			doc:     `{"pipelines": []}`,
			wantSub: "pipelines",
		},
		{
			name: "duplicate names",
			doc: `pipelines:
  - {name: dup, input: {type: stdin}, parser: {type: raw}, branches: [{name: b, encoder: {type: raw}, output: {type: stdout}}]}
  - {name: dup, input: {type: stdin}, parser: {type: raw}, branches: [{name: b, encoder: {type: raw}, output: {type: stdout}}]}`,
			wantSub: "pipelines[1].name",
		},
		{
			name: "missing input type",
			doc: `pipelines:
  - {name: p, input: {type: ""}, parser: {type: raw}, branches: [{name: b, encoder: {type: raw}, output: {type: stdout}}]}`,
			wantSub: "pipelines[0].input.type",
		},
		{
			name: "no branches",
			doc: `pipelines:
  - {name: p, input: {type: stdin}, parser: {type: raw}, branches: []}`,
			wantSub: "pipelines[0].branches",
		},
		{
			name: "branch missing encoder",
			doc: `pipelines:
  - {name: p, input: {type: stdin}, parser: {type: raw}, branches: [{name: b, encoder: {type: ""}, output: {type: stdout}}]}`,
			wantSub: "pipelines[0].branches[0].encoder.type",
		},
		{
			name: "pre-processor missing type",
			doc: `pipelines:
  - {name: p, input: {type: stdin}, parser: {type: raw}, processors: [{type: ""}], branches: [{name: b, encoder: {type: raw}, output: {type: stdout}}]}`,
			wantSub: "pipelines[0].processors[0].type",
		},
		{
			name:    "unknown field",
			doc:     `{"pipelines": [], "bogus": true}`,
			wantSub: "bogus",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.LoadConfig(strings.NewReader(c.doc))
			if err == nil {
				t.Fatalf("LoadConfig(%s): expected error", c.name)
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Fatalf("error %q should contain %q", err.Error(), c.wantSub)
			}
		})
	}
}
