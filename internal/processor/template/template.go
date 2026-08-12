// Package template mines a stable log template from a message column and adds
// it as a new column. Variable tokens (anything containing a digit, or obvious
// value-like tokens) are replaced with a "<*>" placeholder, so lines like
//
//	user 4821 logged in from 10.0.0.2
//	user 12 logged in from 10.9.1.4
//
// share the template "user <*> logged in from <*>". This is a lightweight,
// deterministic Drain-style normalization; grouping/counting by the template
// column then reveals log shapes. With enabled:false the processor is identity.
package template

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

// Type is the config identifier for this processor.
const Type = "template"

const defaultSource = data.LineColumn

// Config configures the template processor.
type Config struct {
	// Source is the string column to mine (default "line").
	Source string `json:"source"`
	// Target is the added template column name (default "template").
	Target string `json:"target"`
	// Enabled toggles mining; false makes the processor an identity pass.
	Enabled *bool `json:"enabled"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Target != "" && c.Target == c.Source {
		return fmt.Errorf("template.target: must differ from source %q", c.Source)
	}
	return nil
}

func (c *Config) enabled() bool { return c.Enabled == nil || *c.Enabled }
func (c *Config) source() string {
	if c.Source == "" {
		return defaultSource
	}
	return c.Source
}
func (c *Config) target() string {
	if c.Target == "" {
		return "template"
	}
	return c.Target
}

type factory struct{}

// NewFactory returns the template processor factory.
func NewFactory() component.Factory[component.Processor] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Processor, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("processor/template: unexpected config type %T", cfg)
	}
	return &processor{enabled: c.enabled(), source: c.source(), target: c.target()}, nil
}

type processor struct {
	enabled bool
	source  string
	target  string
	mem     memory.Allocator
}

func (p *processor) Start(_ context.Context, host component.Host) error {
	if host != nil {
		p.mem = host.Allocator()
	}
	if p.mem == nil {
		p.mem = memory.DefaultAllocator
	}
	return nil
}
func (p *processor) Shutdown(context.Context) error { return nil }

func (p *processor) Process(_ context.Context, in data.RecordBatch) (data.RecordBatch, error) {
	if !p.enabled {
		return in, nil // identity: caller keeps ownership
	}
	defer in.Release() // we build a new batch and own the input
	rec := in.Record()
	if rec == nil {
		return in, nil
	}
	idx := rec.Schema().FieldIndices(p.source)
	if len(idx) == 0 {
		return data.RecordBatch{}, fmt.Errorf("processor/template: source column %q not found", p.source)
	}
	src, ok := rec.Column(idx[0]).(*array.String)
	if !ok {
		// Fall back to binary-typed line columns.
		if bin, okb := rec.Column(idx[0]).(*array.Binary); okb {
			return p.appendTemplate(in.Source, rec, func(i int) string { return string(bin.Value(i)) })
		}
		return data.RecordBatch{}, fmt.Errorf("processor/template: source column %q is %s, want string", p.source, rec.Column(idx[0]).DataType())
	}
	return p.appendTemplate(in.Source, rec, func(i int) string {
		if src.IsNull(i) {
			return ""
		}
		return src.Value(i)
	})
}

// appendTemplate returns a new batch with every existing column plus a mined
// template column, preserving the input's source provenance.
func (p *processor) appendTemplate(source string, rec arrow.RecordBatch, get func(i int) string) (data.RecordBatch, error) {
	rows := int(rec.NumRows())
	tb := array.NewStringBuilder(p.mem)
	defer tb.Release()
	for i := 0; i < rows; i++ {
		tb.Append(mine(get(i)))
	}

	fields := append([]arrow.Field(nil), rec.Schema().Fields()...)
	fields = append(fields, arrow.Field{Name: p.target, Type: arrow.BinaryTypes.String})
	cols := make([]arrow.Array, 0, len(fields))
	for c := 0; c < int(rec.NumCols()); c++ {
		col := rec.Column(c)
		col.Retain() // shared with the input; NewRecordBatch adds its own ref
		cols = append(cols, col)
	}
	cols = append(cols, tb.NewArray())
	out := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, int64(rows))
	for _, c := range cols {
		c.Release() // NewRecordBatch retained everything it needs
	}
	return data.NewRecordBatch(source, out), nil
}

// mine reduces a message to its template by replacing variable tokens.
func mine(msg string) string {
	if msg == "" {
		return ""
	}
	fields := strings.Fields(msg)
	for i, tok := range fields {
		if isVariable(tok) {
			fields[i] = "<*>"
		}
	}
	return strings.Join(fields, " ")
}

// isVariable reports whether a token looks like a value rather than a word: it
// contains a digit (ids, ips, durations, timestamps) or is a key=value pair.
func isVariable(tok string) bool {
	if strings.ContainsRune(tok, '=') {
		return true
	}
	for _, r := range tok {
		if r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}
