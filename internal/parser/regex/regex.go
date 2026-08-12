// Package regex parses lines with a configured regular expression, mapping each
// named capture group to a column via the columnar builder. It suits
// semi-structured logs (e.g. access logs) that are neither JSON nor logfmt. A
// line that does not match yields an error the runtime routes per policy.
package regex

import (
	"context"
	"fmt"
	"regexp"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/columnar"
	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

// Type is the config identifier for this parser.
const Type = "regex"

// Config configures the regex parser.
type Config struct {
	// Pattern is an RE2 regex with at least one named capture group
	// (?P<name>…); each named group becomes a column.
	Pattern string `json:"pattern"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Pattern == "" {
		return fmt.Errorf("regex.pattern: required, must not be empty")
	}
	re, err := regexp.Compile(c.Pattern)
	if err != nil {
		return fmt.Errorf("regex.pattern: %w", err)
	}
	if named := namedGroups(re); len(named) == 0 {
		return fmt.Errorf("regex.pattern: must define at least one named group (?P<name>...)")
	}
	return nil
}

func namedGroups(re *regexp.Regexp) []string {
	var out []string
	for _, n := range re.SubexpNames() {
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

type factory struct{}

// NewFactory returns the regex parser factory.
func NewFactory() component.Factory[component.Parser] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Parser, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("parser/regex: unexpected config type %T", cfg)
	}
	re, err := regexp.Compile(c.Pattern)
	if err != nil {
		return nil, fmt.Errorf("parser/regex: %w", err)
	}
	return &parser{re: re, names: namedGroups(re)}, nil
}

type parser struct {
	re    *regexp.Regexp
	names []string
	mem   memory.Allocator
}

func (p *parser) Start(_ context.Context, host component.Host) error {
	if host != nil {
		p.mem = host.Allocator()
	}
	if p.mem == nil {
		p.mem = memory.DefaultAllocator
	}
	return nil
}
func (p *parser) Shutdown(context.Context) error { return nil }

func (p *parser) Parse(_ context.Context, in data.RawBatch) (data.RecordBatch, error) {
	rows := make([]map[string]any, 0, len(in.Records))
	for _, rec := range in.Records {
		m := p.re.FindStringSubmatch(string(rec))
		if m == nil {
			return data.RecordBatch{}, fmt.Errorf("parser/regex: line does not match pattern: %q", rec)
		}
		row := make(map[string]any, len(p.names))
		for i, name := range p.re.SubexpNames() {
			if name != "" {
				row[name] = m[i]
			}
		}
		rows = append(rows, row)
	}
	return columnar.Build(p.mem, in.Source, rows)
}
