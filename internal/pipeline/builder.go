package pipeline

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/prism-utils/prism/internal/buffer"
	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/config"
)

// policy is how a pipeline reacts to a malformed-data error from a parser or
// processor.
type policy int

const (
	// policyBlock stops the pipeline on a processing error (the default).
	policyBlock policy = iota
	// policyDrop logs and skips the offending window, keeping the pipeline up.
	policyDrop
)

func parsePolicy(s string) policy {
	if s == "drop" {
		return policyDrop
	}
	return policyBlock
}

// branch is one fan-out tail: optional processors, then encode and output.
type branch struct {
	name    string
	procs   []component.Processor
	encoder component.Encoder
	output  component.Output
}

// builtPipeline is one input's wired, ready-to-run pipeline.
type builtPipeline struct {
	name     string
	input    component.Input
	parser   component.Parser
	pre      []component.Processor
	bufCfg   buffer.Config
	onError  policy
	branches []branch
}

// Set is a set of built pipelines. Run drives them concurrently — one worker
// per input — each isolated so one failing pipeline does not stop the others.
type Set struct {
	pipelines []builtPipeline
	log       *slog.Logger
}

// Build resolves a validated *config.Config against a *component.Registry into
// concrete, wired pipelines. It fails fast with a path-qualified error if any
// stage's type is unknown or its options are invalid.
func Build(cfg *config.Config, reg *component.Registry, set component.Settings) (*Set, error) {
	if cfg == nil {
		return nil, fmt.Errorf("pipeline: nil config")
	}
	if reg == nil {
		return nil, fmt.Errorf("pipeline: nil registry")
	}
	log := set.Logger
	if log == nil {
		log = slog.Default()
	}

	built := make([]builtPipeline, 0, len(cfg.Pipelines))
	for i := range cfg.Pipelines {
		pc := &cfg.Pipelines[i]
		path := fmt.Sprintf("pipelines[%d](%s)", i, pc.Name)

		in, err := buildStage(path+".input", pc.Input, reg.Input, set)
		if err != nil {
			return nil, err
		}
		parser, err := buildStage(path+".parser", pc.Parser, reg.Parser, set)
		if err != nil {
			return nil, err
		}
		pre, err := buildProcs(path, pc.Processors, reg, set)
		if err != nil {
			return nil, err
		}

		brs := make([]branch, 0, len(pc.Branches))
		for j := range pc.Branches {
			bc := &pc.Branches[j]
			bpath := fmt.Sprintf("%s.branches[%d](%s)", path, j, bc.Name)
			procs, perr := buildProcs(bpath, bc.Processors, reg, set)
			if perr != nil {
				return nil, perr
			}
			enc, eerr := buildStage(bpath+".encoder", bc.Encoder, reg.Encoder, set)
			if eerr != nil {
				return nil, eerr
			}
			out, oerr := buildStage(bpath+".output", bc.Output, reg.Output, set)
			if oerr != nil {
				return nil, oerr
			}
			brs = append(brs, branch{name: bc.Name, procs: procs, encoder: enc, output: out})
		}

		built = append(built, builtPipeline{
			name:   pc.Name,
			input:  in,
			parser: parser,
			pre:    pre,
			bufCfg: buffer.Config{
				MaxAge:   time.Duration(pc.Buffer.MaxAge),
				MaxRows:  pc.Buffer.MaxRows,
				MaxBytes: int64(pc.Buffer.MaxBytes),
			},
			onError:  parsePolicy(pc.OnError),
			branches: brs,
		})
	}
	return &Set{pipelines: built, log: log}, nil
}

func buildProcs(path string, stages []config.Stage, reg *component.Registry, set component.Settings) ([]component.Processor, error) {
	procs := make([]component.Processor, 0, len(stages))
	for i := range stages {
		p, err := buildStage(fmt.Sprintf("%s.processors[%d]", path, i), stages[i], reg.Processor, set)
		if err != nil {
			return nil, err
		}
		procs = append(procs, p)
	}
	return procs, nil
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
