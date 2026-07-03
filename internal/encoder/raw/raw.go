// Package raw implements a passthrough Encoder: it serializes a RecordBatch as
// newline-delimited record bytes. It gives the foundation a working, inspectable
// output format; the Parquet encoder (Arrow→Parquet) lands in Phase 5 of
// docs/PLAN.md.
package raw

import (
	"bytes"
	"context"
	"fmt"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this encoder.
const Type = "raw"

// Config configures the raw encoder (no options).
type Config struct{}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the raw encoder factory.
func NewFactory() component.Factory[component.Encoder] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Encoder, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("encoder/raw: unexpected config type %T", cfg)
	}
	return encoder{}, nil
}

type encoder struct{}

func (encoder) Start(context.Context, component.Host) error { return nil }
func (encoder) Shutdown(context.Context) error              { return nil }

func (encoder) Encode(_ context.Context, in data.RecordBatch) (data.EncodedBlock, error) {
	var buf bytes.Buffer
	for _, rec := range in.Records {
		buf.Write(rec)
		buf.WriteByte('\n')
	}
	return data.EncodedBlock{Format: Type, Bytes: buf.Bytes(), Rows: in.Len()}, nil
}
