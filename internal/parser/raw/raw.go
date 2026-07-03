// Package raw implements a passthrough Parser: it carries raw record bytes into
// a RecordBatch without imposing structure. It exists so the foundation has a
// working end-to-end pipeline; structured parsers (json, logfmt, regex,
// template) and field auto-discovery land in Phase 4 of docs/PLAN.md.
package raw

import (
	"context"
	"fmt"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this parser.
const Type = "raw"

// Config configures the raw parser (no options).
type Config struct{}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the raw parser factory.
func NewFactory() component.Factory[component.Parser] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Parser, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("parser/raw: unexpected config type %T", cfg)
	}
	return parser{}, nil
}

type parser struct{}

func (parser) Start(context.Context, component.Host) error { return nil }
func (parser) Shutdown(context.Context) error              { return nil }

func (parser) Parse(_ context.Context, in data.RawBatch) (data.RecordBatch, error) {
	// Passthrough: carry raw rows into a RecordBatch unchanged. The interim
	// layouts are identical today; Phase 4 replaces this with real parsing.
	return data.RecordBatch(in), nil
}
