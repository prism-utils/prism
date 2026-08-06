//go:build !cgo

package duckdb

import (
	"context"
	"fmt"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this encoder.
const Type = "duckdb"

// Config configures the duckdb encoder.
type Config struct {
	// StorageVersion pins STORAGE_VERSION on created files (default v1.0.0).
	StorageVersion string `json:"storage_version"`
}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns a stub factory; Encode requires a CGO build.
func NewFactory() component.Factory[component.Encoder] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Encoder, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("encoder/duckdb: unexpected config type %T", cfg)
	}
	return stubEncoder{}, nil
}

type stubEncoder struct{}

func (stubEncoder) Start(context.Context, component.Host) error { return nil }
func (stubEncoder) Shutdown(context.Context) error              { return nil }

func (stubEncoder) Encode(context.Context, data.RecordBatch) (data.EncodedBlock, error) {
	return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: requires CGO (build with CGO_ENABLED=1)")
}
