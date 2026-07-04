// Package json implements an Encoder that serializes a RecordBatch as a JSON
// array of row objects: [{"col": value, …}, …]. It is the sink format for
// summary branches, whose small aggregate rows are stored server-side (e.g.
// SQLite). Column order follows the schema; nulls encode as JSON null.
package json

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this encoder.
const Type = "json"

// Config configures the json encoder (no options).
type Config struct{}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the json encoder factory.
func NewFactory() component.Factory[component.Encoder] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Encoder, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("encoder/json: unexpected config type %T", cfg)
	}
	return encoder{}, nil
}

type encoder struct{}

func (encoder) Start(context.Context, component.Host) error { return nil }
func (encoder) Shutdown(context.Context) error              { return nil }

func (encoder) Encode(_ context.Context, in data.RecordBatch) (data.EncodedBlock, error) {
	defer in.Release() // encoders own their input's buffers
	rec := in.Record()
	if rec == nil || rec.NumRows() == 0 {
		return data.EncodedBlock{Format: Type, Bytes: []byte("[]"), Rows: 0}, nil
	}
	rows := int(rec.NumRows())
	fields := rec.Schema().Fields()
	out := make([]map[string]any, rows)
	for r := 0; r < rows; r++ {
		obj := make(map[string]any, len(fields))
		for c := range fields {
			v, err := value(rec.Column(c), r)
			if err != nil {
				return data.EncodedBlock{}, fmt.Errorf("encoder/json: column %q: %w", fields[c].Name, err)
			}
			obj[fields[c].Name] = v
		}
		out[r] = obj
	}
	b, err := json.Marshal(out)
	if err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/json: marshal: %w", err)
	}
	return data.EncodedBlock{Format: Type, Bytes: b, Rows: rows}, nil
}

// value extracts the Go value of one cell for JSON encoding.
func value(col arrow.Array, r int) (any, error) {
	if col.IsNull(r) {
		return nil, nil
	}
	switch a := col.(type) {
	case *array.String:
		return a.Value(r), nil
	case *array.Binary:
		return string(a.Value(r)), nil
	case *array.Boolean:
		return a.Value(r), nil
	case *array.Int64:
		return a.Value(r), nil
	case *array.Int32:
		return a.Value(r), nil
	case *array.Float64:
		return finite(a.Value(r)), nil
	case *array.Float32:
		return finite(float64(a.Value(r))), nil
	default:
		return nil, fmt.Errorf("unsupported type %s", col.DataType())
	}
}

// finite maps non-finite floats (NaN, ±Inf) to nil so they encode as JSON
// null; JSON has no literal for them and json.Marshal would otherwise error.
func finite(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return f
}
