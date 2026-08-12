// Package logfmt parses logfmt lines (key=value key2="quoted value" flag) into
// a schema-discovered RecordBatch via the columnar builder. A bare key with no
// '=' becomes key=true. It never panics on malformed input; unparseable bytes
// still yield a best-effort row (the raw token as a message) rather than an
// error, because logs are noisy by nature.
package logfmt

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/columnar"
	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

// Type is the config identifier for this parser.
const Type = "logfmt"

// Config configures the logfmt parser (no options).
type Config struct{}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the logfmt parser factory.
func NewFactory() component.Factory[component.Parser] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Parser, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("parser/logfmt: unexpected config type %T", cfg)
	}
	return &parser{}, nil
}

type parser struct{ mem memory.Allocator }

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
		rows = append(rows, parseLine(string(rec)))
	}
	return columnar.Build(p.mem, in.Source, rows)
}

// parseLine tokenizes one logfmt line into a key→value map. Values are kept as
// strings; the columnar builder infers numeric/bool types across the batch.
func parseLine(line string) map[string]any {
	out := map[string]any{}
	i := 0
	n := len(line)
	for i < n {
		for i < n && line[i] == ' ' {
			i++
		}
		if i >= n {
			break
		}
		// key: up to '=' or space
		keyStart := i
		for i < n && line[i] != '=' && line[i] != ' ' {
			i++
		}
		key := line[keyStart:i]
		if key == "" {
			i++
			continue
		}
		if i >= n || line[i] == ' ' {
			out[key] = true // bare flag
			continue
		}
		i++ // skip '='
		if i < n && line[i] == '"' {
			val, next := scanQuoted(line, i)
			out[key] = val
			i = next
			continue
		}
		valStart := i
		for i < n && line[i] != ' ' {
			i++
		}
		out[key] = line[valStart:i]
	}
	return out
}

// scanQuoted reads a double-quoted value starting at s[start]=='"', honoring
// backslash escapes, and returns the unquoted value and the index past it.
func scanQuoted(s string, start int) (string, int) {
	var b []byte
	i := start + 1
	for i < len(s) {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 'n':
				b = append(b, '\n')
			case 't':
				b = append(b, '\t')
			default:
				b = append(b, s[i])
			}
			i++
			continue
		}
		if c == '"' {
			return string(b), i + 1
		}
		b = append(b, c)
		i++
	}
	return string(b), i // unterminated: take the rest
}
