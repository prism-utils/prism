// Package config defines prism's typed configuration tree and its loader.
//
// A config declares a set of pipelines; each pipeline is
// input → parser → processors → buffer → fan-out branches (docs/DESIGN.md §7).
// One Go struct with json tags serves both YAML and JSON via LoadConfig's
// yaml→map→json shim, with ${VAR} env interpolation.
//
// Validation is total and runs at load time so a malformed config never reaches
// the runtime. Every error names the offending path
// (e.g. "pipelines[0].branches[1].encoder.type").
package config

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrNotFound is returned by loaders when a config source is absent.
var ErrNotFound = errors.New("config: not found")

// Stage is one component's config: its "type" plus the raw per-type options.
// The typed per-type options are decoded by the component's Factory (the
// factory owns its schema); here we validate the shared invariant that a type
// is present. Keeping options raw keeps this package free of any component
// import (dependencies point inward).
type Stage struct {
	// Type selects the component factory (e.g. "file", "parquet", "http").
	Type string `json:"type"`
	// Options holds the type-specific config block, decoded later by the
	// factory into its own typed struct.
	Options json.RawMessage `json:"options,omitempty"`
}

func (s Stage) validate(path string) error {
	if s.Type == "" {
		return fmt.Errorf("config: %s.type: required, must not be empty", path)
	}
	return nil
}
