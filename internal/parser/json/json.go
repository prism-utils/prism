// Package json parses one JSON object per record into a schema-discovered
// RecordBatch via the columnar builder. Nested objects/arrays are flattened to
// their JSON text so every column stays scalar. A record that is not a JSON
// object yields an error the runtime routes per the failure policy.
package json

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/columnar"
	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

// Type is the config identifier for this parser.
const Type = "json"

// Config configures the json parser (no options).
type Config struct{}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the json parser factory.
func NewFactory() component.Factory[component.Parser] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Parser, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("parser/json: unexpected config type %T", cfg)
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
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(rec, &obj); err != nil {
			return data.RecordBatch{}, fmt.Errorf("parser/json: %w", err)
		}
		rows = append(rows, flatten(obj))
	}
	return columnar.Build(p.mem, in.Source, rows)
}

// flatten converts each JSON value to a scalar the columnar builder accepts:
// numbers→int64/float64, bools→bool, strings→string, and objects/arrays/null
// to their JSON text (or nil for JSON null).
func flatten(obj map[string]json.RawMessage) map[string]any {
	out := make(map[string]any, len(obj))
	for k, raw := range obj {
		out[k] = scalar(raw)
	}
	return out
}

func scalar(raw json.RawMessage) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		return x
	case float64:
		if x == float64(int64(x)) {
			return int64(x)
		}
		return x
	case string:
		return x
	default: // object or array
		return string(raw)
	}
}
