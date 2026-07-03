// Package config defines prism's typed configuration tree and its loader.
//
// A pipeline is declared as input → parser → processors → encoder → output
// (docs/DESIGN.md §7). One Go struct with json tags serves both YAML and JSON:
// this package decodes JSON today; Phase 1 of docs/PLAN.md adds the YAML→JSON
// shim and env interpolation via koanf without changing this struct.
//
// Validation is total and runs at load time so a malformed config never reaches
// the runtime. Every error names the offending path (e.g. "processors[2].type").
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ErrNotFound is returned by loaders when a config source is absent.
var ErrNotFound = errors.New("config: not found")

// Pipeline is the top-level configuration: a single linear pipeline.
type Pipeline struct {
	Input      Stage   `json:"input"`
	Parser     Stage   `json:"parser"`
	Processors []Stage `json:"processors"`
	Encoder    Stage   `json:"encoder"`
	Output     Stage   `json:"output"`
}

// Stage is one component's config: its "type" plus the raw per-type options.
// The typed per-type options are decoded by the component's Factory in later
// phases (the factory owns its schema); here we validate the shared invariants.
type Stage struct {
	// Type selects the component factory (e.g. "file", "parquet", "http").
	Type string `json:"type"`
	// Options holds the type-specific config block, decoded later by the
	// factory into its own typed struct. Kept raw here to keep this package
	// free of any component import (dependencies point inward).
	Options json.RawMessage `json:"options,omitempty"`
}

// Load decodes a Pipeline from JSON and validates it. It never returns an
// unvalidated config.
func Load(r io.Reader) (*Pipeline, error) {
	if r == nil {
		return nil, ErrNotFound
	}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var p Pipeline
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("config: decode: %w", err)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

// Validate checks the pipeline is structurally complete. It is total: it
// reports the first offending path with the violated constraint.
func (p *Pipeline) Validate() error {
	if err := p.Input.validate("input"); err != nil {
		return err
	}
	if err := p.Parser.validate("parser"); err != nil {
		return err
	}
	for i := range p.Processors {
		if err := p.Processors[i].validate(fmt.Sprintf("processors[%d]", i)); err != nil {
			return err
		}
	}
	if err := p.Encoder.validate("encoder"); err != nil {
		return err
	}
	if err := p.Output.validate("output"); err != nil {
		return err
	}
	return nil
}

func (s Stage) validate(path string) error {
	if s.Type == "" {
		return fmt.Errorf("config: %s.type: required, must not be empty", path)
	}
	return nil
}
