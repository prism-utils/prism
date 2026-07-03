package pipeline

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/config"
)

// Pipeline is a built, ready-to-run chain of components. Construct it with
// Build; drive it with Run.
type Pipeline struct {
	input      component.Input
	parser     component.Parser
	processors []component.Processor
	encoder    component.Encoder
	output     component.Output
	log        *slog.Logger
}

// Build resolves a validated *config.Pipeline against a *component.Registry into
// concrete, wired components. It fails fast with a path-qualified error if any
// stage's type is unknown or its options are invalid.
func Build(cfg *config.Pipeline, reg *component.Registry, set component.Settings) (*Pipeline, error) {
	if cfg == nil {
		return nil, fmt.Errorf("pipeline: nil config")
	}
	if reg == nil {
		return nil, fmt.Errorf("pipeline: nil registry")
	}

	in, err := buildStage("input", cfg.Input, reg.Input, set)
	if err != nil {
		return nil, err
	}
	parser, err := buildStage("parser", cfg.Parser, reg.Parser, set)
	if err != nil {
		return nil, err
	}
	procs := make([]component.Processor, 0, len(cfg.Processors))
	for i := range cfg.Processors {
		p, perr := buildStage(fmt.Sprintf("processors[%d]", i), cfg.Processors[i], reg.Processor, set)
		if perr != nil {
			return nil, perr
		}
		procs = append(procs, p)
	}
	enc, err := buildStage("encoder", cfg.Encoder, reg.Encoder, set)
	if err != nil {
		return nil, err
	}
	out, err := buildStage("output", cfg.Output, reg.Output, set)
	if err != nil {
		return nil, err
	}

	log := set.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Pipeline{
		input:      in,
		parser:     parser,
		processors: procs,
		encoder:    enc,
		output:     out,
		log:        log,
	}, nil
}

// buildStage is the shared "config type + options -> component" flow used for
// every kind (docs/DESIGN.md §4): look up the factory, start from its defaults,
// overlay the user's options, validate, then create. Methods cannot be generic
// in Go, so the per-kind Registry lookups are passed in as a function value.
func buildStage[T any](
	path string,
	stage config.Stage,
	lookup func(string) (component.Factory[T], error),
	set component.Settings,
) (T, error) {
	var zero T
	f, err := lookup(stage.Type)
	if err != nil {
		return zero, fmt.Errorf("%s: %w", path, err)
	}
	cfg := f.DefaultConfig()
	if len(stage.Options) > 0 {
		if err := json.Unmarshal(stage.Options, cfg); err != nil {
			return zero, fmt.Errorf("%s %q: decode options: %w", path, stage.Type, err)
		}
	}
	if err := cfg.Validate(); err != nil {
		return zero, fmt.Errorf("%s %q: %w", path, stage.Type, err)
	}
	c, err := f.Create(cfg, set)
	if err != nil {
		return zero, fmt.Errorf("%s %q: create: %w", path, stage.Type, err)
	}
	return c, nil
}
