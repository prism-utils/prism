// Package arrow implements an Encoder that serializes an Arrow RecordBatch into
// an Arrow IPC stream (schema + record). This is the columnar wire format the
// flight output reframes as FlightData, so a server ingests the columns
// directly instead of re-parsing a row format. Pure Go, no CGO.
package arrow

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this encoder.
const Type = "arrow"

// Config configures the arrow encoder (no options).
type Config struct{}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the arrow encoder factory.
func NewFactory() component.Factory[component.Encoder] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Encoder, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("encoder/arrow: unexpected config type %T", cfg)
	}
	return &encoder{}, nil
}

type encoder struct{ mem memory.Allocator }

func (e *encoder) Start(_ context.Context, host component.Host) error {
	if host != nil {
		e.mem = host.Allocator()
	}
	if e.mem == nil {
		e.mem = memory.DefaultAllocator
	}
	return nil
}
func (*encoder) Shutdown(context.Context) error { return nil }

func (e *encoder) Encode(_ context.Context, in data.RecordBatch) (data.EncodedBlock, error) {
	defer in.Release() // encoders own their input's buffers
	rec := in.Record()
	if rec == nil || rec.NumRows() == 0 {
		return data.EncodedBlock{Format: Type, Rows: 0}, nil
	}
	mem := e.mem
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		_ = w.Close()
		return data.EncodedBlock{}, fmt.Errorf("encoder/arrow: write: %w", err)
	}
	if err := w.Close(); err != nil {
		return data.EncodedBlock{}, fmt.Errorf("encoder/arrow: close: %w", err)
	}
	return data.EncodedBlock{Format: Type, Bytes: buf.Bytes(), Rows: int(rec.NumRows())}, nil
}
