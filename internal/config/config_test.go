package config_test

import (
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/config"
)

func TestLoad_Valid(t *testing.T) {
	const doc = `{
		"input":  {"type": "stdin"},
		"parser": {"type": "json"},
		"processors": [{"type": "summary"}, {"type": "ml"}],
		"encoder": {"type": "parquet"},
		"output":  {"type": "stdout"}
	}`
	p, err := config.Load(strings.NewReader(doc))
	if err != nil {
		t.Fatalf("Load: unexpected error: %v", err)
	}
	if p.Input.Type != "stdin" || p.Output.Type != "stdout" {
		t.Fatalf("Load: unexpected stages: in=%q out=%q", p.Input.Type, p.Output.Type)
	}
	if len(p.Processors) != 2 || p.Processors[1].Type != "ml" {
		t.Fatalf("Load: processors not preserved in order: %+v", p.Processors)
	}
}

func TestValidate_NamesOffendingPath(t *testing.T) {
	// A processor missing its type must fail with the exact indexed path so an
	// operator can find it immediately (docs/CONTRIBUTING.md §3.2).
	const doc = `{
		"input":  {"type": "stdin"},
		"parser": {"type": "json"},
		"processors": [{"type": "summary"}, {"type": ""}],
		"encoder": {"type": "parquet"},
		"output":  {"type": "stdout"}
	}`
	_, err := config.Load(strings.NewReader(doc))
	if err == nil {
		t.Fatal("Load: expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "processors[1].type") {
		t.Fatalf("error %q should name the path processors[1].type", err.Error())
	}
}

func TestLoad_RejectsUnknownFields(t *testing.T) {
	const doc = `{"input": {"type": "stdin"}, "bogus": true}`
	if _, err := config.Load(strings.NewReader(doc)); err == nil {
		t.Fatal("Load: expected error on unknown top-level field, got nil")
	}
}
