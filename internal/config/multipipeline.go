package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	kyaml "github.com/knadh/koanf/parsers/yaml"
)

// Default buffer bounds applied at load when a pipeline declares none, per
// docs/DESIGN.md §6.1. Flat steady-state memory relies on at least one bound.
const (
	defaultBufferMaxAge   = 30 * time.Second
	defaultBufferMaxBytes = 12 * 1024 * 1024
)

// Config is the top-level configuration: a set of independent pipelines, each
// run in its own worker (docs/DESIGN.md §6).
type Config struct {
	// Include is a list of file globs (relative to the config file, or absolute)
	// whose pipelines are merged into this set at load — the Beats-style
	// "config.d/*.yaml" pattern for splitting per-source pipelines into their own
	// files. Only the top-level config may declare Include (no nested includes),
	// and it is honored only by LoadFile (a base directory is required to resolve
	// the globs).
	Include []string `json:"include,omitempty"`

	Pipelines []PipelineConfig `json:"pipelines"`
}

// PipelineConfig is one input's pipeline: input → parser → pre-buffer
// processors → buffer → fan-out branches. Each pipeline runs in isolation.
type PipelineConfig struct {
	Name       string   `json:"name"`
	Input      Stage    `json:"input"`
	Parser     Stage    `json:"parser"`
	Processors []Stage  `json:"processors,omitempty"`
	Buffer     Buffer   `json:"buffer"`
	Branches   []Branch `json:"branches"`
	// OnError selects how malformed data (a parser/processor error) is handled:
	// "drop" logs and skips the offending window and keeps the pipeline running;
	// "block" stops this pipeline on the error. Empty means "block".
	OnError string `json:"on_error,omitempty"`
}

// Buffer configures the windowing accumulator that flushes on the first of its
// bounds (docs/DESIGN.md §6.1). A zero value means "apply defaults at load".
type Buffer struct {
	MaxAge   Duration `json:"max_age"`
	MaxRows  int      `json:"max_rows"`
	MaxBytes ByteSize `json:"max_bytes"`
}

// Branch is one fan-out tail after the buffer: optional processors, then an
// encoder and an output. The data branch encodes parquet; the summary branch
// aggregates then encodes json.
type Branch struct {
	Name       string  `json:"name"`
	Processors []Stage `json:"processors,omitempty"`
	Encoder    Stage   `json:"encoder"`
	Output     Stage   `json:"output"`
}

// envVarRe matches ${VAR} references for env interpolation in the raw config.
var envVarRe = regexp.MustCompile(`\$\{[A-Za-z_][A-Za-z0-9_]*\}`)

// LoadConfig reads a YAML or JSON pipeline set from a reader, interpolates
// ${VAR} from the environment, applies buffer defaults, and validates it. It
// never returns an unvalidated config. Because a reader has no base directory,
// this path cannot resolve `include` globs — a config that declares include is
// rejected here; use LoadFile for split configs.
func LoadConfig(r io.Reader) (*Config, error) {
	if r == nil {
		return nil, ErrNotFound
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("config: read: %w", err)
	}
	cfg, err := decodeConfig(raw)
	if err != nil {
		return nil, err
	}
	if len(cfg.Include) > 0 {
		return nil, fmt.Errorf("config: include is only supported by file-based loading")
	}
	return finalize(cfg)
}

// LoadFile reads a config from a path and merges any `include` globs (resolved
// relative to the file's directory) into the pipeline set before validating —
// the Beats-style config.d pattern. Included files must not themselves declare
// include (one level only).
func LoadFile(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, path)
		}
		return nil, fmt.Errorf("config: read %q: %w", path, err)
	}
	cfg, err := decodeConfig(raw)
	if err != nil {
		return nil, err
	}
	baseDir := filepath.Dir(path)
	for _, glob := range cfg.Include {
		pattern := glob
		if !filepath.IsAbs(pattern) {
			pattern = filepath.Join(baseDir, pattern)
		}
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("config: include %q: %w", glob, err)
		}
		sort.Strings(matches) // deterministic merge order
		for _, m := range matches {
			sub, err := os.ReadFile(m)
			if err != nil {
				return nil, fmt.Errorf("config: include %q: read: %w", m, err)
			}
			subCfg, err := decodeConfig(sub)
			if err != nil {
				return nil, fmt.Errorf("config: include %q: %w", m, err)
			}
			if len(subCfg.Include) > 0 {
				return nil, fmt.Errorf("config: include %q: nested include is not allowed", m)
			}
			cfg.Pipelines = append(cfg.Pipelines, subCfg.Pipelines...)
		}
	}
	return finalize(cfg)
}

// decodeConfig interpolates ${VAR}, parses YAML/JSON into the typed tree with
// unknown keys rejected, and returns the config without defaults or validation.
// YAML and JSON share one path: JSON is a subset of YAML, so both parse to the
// same map, which is re-marshalled to JSON and decoded into the typed tree.
func decodeConfig(raw []byte) (*Config, error) {
	m, err := kyaml.Parser().Unmarshal(interpolateEnv(raw))
	if err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	normalized, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("config: normalize: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(normalized))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: decode: %w", err)
	}
	return &cfg, nil
}

// finalize applies defaults and runs total validation on an assembled config.
func finalize(cfg *Config) (*Config, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// interpolateEnv replaces ${VAR} with the environment value (empty if unset).
func interpolateEnv(b []byte) []byte {
	return envVarRe.ReplaceAllFunc(b, func(match []byte) []byte {
		name := string(match[2 : len(match)-1])
		return []byte(os.Getenv(name))
	})
}

// applyDefaults fills buffer bounds a pipeline left entirely unset.
func (c *Config) applyDefaults() {
	for i := range c.Pipelines {
		b := &c.Pipelines[i].Buffer
		if b.MaxAge == 0 && b.MaxRows == 0 && b.MaxBytes == 0 {
			b.MaxAge = Duration(defaultBufferMaxAge)
			b.MaxBytes = ByteSize(defaultBufferMaxBytes)
		}
	}
}

// Validate is total: it reports the first offending path with the violated
// constraint so an operator can find it immediately.
func (c *Config) Validate() error {
	if len(c.Pipelines) == 0 {
		return fmt.Errorf("config: pipelines: at least one required")
	}
	seen := make(map[string]struct{}, len(c.Pipelines))
	for i := range c.Pipelines {
		p := &c.Pipelines[i]
		path := fmt.Sprintf("pipelines[%d]", i)
		if p.Name == "" {
			return fmt.Errorf("config: %s.name: required, must not be empty", path)
		}
		if _, dup := seen[p.Name]; dup {
			return fmt.Errorf("config: %s.name: duplicate pipeline name %q", path, p.Name)
		}
		seen[p.Name] = struct{}{}
		if err := p.Input.validate(path + ".input"); err != nil {
			return err
		}
		if err := p.Parser.validate(path + ".parser"); err != nil {
			return err
		}
		for j := range p.Processors {
			if err := p.Processors[j].validate(fmt.Sprintf("%s.processors[%d]", path, j)); err != nil {
				return err
			}
		}
		if err := p.Buffer.validate(path + ".buffer"); err != nil {
			return err
		}
		switch p.OnError {
		case "", "drop", "block":
		default:
			return fmt.Errorf("config: %s.on_error: must be \"drop\" or \"block\", got %q", path, p.OnError)
		}
		if len(p.Branches) == 0 {
			return fmt.Errorf("config: %s.branches: at least one required", path)
		}
		for j := range p.Branches {
			bpath := fmt.Sprintf("%s.branches[%d]", path, j)
			br := &p.Branches[j]
			for k := range br.Processors {
				if err := br.Processors[k].validate(fmt.Sprintf("%s.processors[%d]", bpath, k)); err != nil {
					return err
				}
			}
			if err := br.Encoder.validate(bpath + ".encoder"); err != nil {
				return err
			}
			if err := br.Output.validate(bpath + ".output"); err != nil {
				return err
			}
		}
	}
	return nil
}

// validate checks a buffer's bounds are non-negative and at least one is active.
func (b Buffer) validate(path string) error {
	if b.MaxAge < 0 {
		return fmt.Errorf("config: %s.max_age: must be >= 0", path)
	}
	if b.MaxRows < 0 {
		return fmt.Errorf("config: %s.max_rows: must be >= 0", path)
	}
	if b.MaxBytes < 0 {
		return fmt.Errorf("config: %s.max_bytes: must be >= 0", path)
	}
	if b.MaxAge == 0 && b.MaxRows == 0 && b.MaxBytes == 0 {
		return fmt.Errorf("config: %s: at least one of max_age/max_rows/max_bytes must be set", path)
	}
	return nil
}
