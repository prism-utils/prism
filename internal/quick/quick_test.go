package quick_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/quick"
)

func TestBuildLogsLocalOnly(t *testing.T) {
	cfg, err := quick.Build("logs", quick.Options{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(cfg.Pipelines) != 1 {
		t.Fatalf("pipelines = %d, want 1", len(cfg.Pipelines))
	}
	p := cfg.Pipelines[0]
	if p.Input.Type != "stdin" {
		t.Errorf("input = %q, want stdin", p.Input.Type)
	}
	if p.Parser.Type != "logs" {
		t.Errorf("parser = %q, want logs", p.Parser.Type)
	}
	if p.OnError != "drop" {
		t.Errorf("on_error = %q, want drop", p.OnError)
	}
	if len(p.Branches) != 1 {
		t.Fatalf("branches = %d, want 1 (local only)", len(p.Branches))
	}
	b := p.Branches[0]
	if b.Encoder.Type != "json" || b.Output.Type != "stdout" {
		t.Errorf("local branch = %s/%s, want json/stdout", b.Encoder.Type, b.Output.Type)
	}
}

func TestBuildLogsWithStore(t *testing.T) {
	cfg, err := quick.Build("logs", quick.Options{Store: "https://store:8080/", Tenant: "team-a", Token: "sekret"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	p := cfg.Pipelines[0]
	if len(p.Branches) != 2 {
		t.Fatalf("branches = %d, want 2 (local + ship)", len(p.Branches))
	}
	// Local branch is unchanged when a store is added; both run.
	if p.Branches[0].Output.Type != "stdout" {
		t.Errorf("branch[0] output = %q, want stdout", p.Branches[0].Output.Type)
	}
	ship := p.Branches[1]
	if ship.Encoder.Type != "parquet" || ship.Output.Type != "http" {
		t.Fatalf("ship branch = %s/%s, want parquet/http", ship.Encoder.Type, ship.Output.Type)
	}
	opts := decodeOpts(t, ship.Output.Options)
	wantURL := "https://store:8080/team-a/ingest/logs-summary"
	if opts["url"] != wantURL {
		t.Errorf("url = %v, want %v", opts["url"], wantURL)
	}
	if opts["token"] != "sekret" {
		t.Errorf("token = %v, want sekret", opts["token"])
	}
}

func TestBuildLogsDefaultTenant(t *testing.T) {
	cfg, err := quick.Build("logs", quick.Options{Store: "http://s"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	opts := decodeOpts(t, cfg.Pipelines[0].Branches[1].Output.Options)
	url, _ := opts["url"].(string)
	if !strings.Contains(url, "/default/ingest/logs-summary") {
		t.Errorf("url = %v, want default tenant", url)
	}
	if _, hasToken := opts["token"]; hasToken {
		t.Errorf("token should be absent when unset, got %v", opts["token"])
	}
}

func TestBuildUnknownTemplate(t *testing.T) {
	_, err := quick.Build("bogus", quick.Options{})
	if err == nil || !strings.Contains(err.Error(), "unknown template") {
		t.Fatalf("err = %v, want unknown template error", err)
	}
}

func decodeOpts(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode options: %v", err)
	}
	return m
}
