//go:build cgo

// Package duckdb implements an Encoder that seals an Arrow RecordBatch into a
// checkpointed single-table .duckdb file (STORAGE_VERSION pinned).
package duckdb

import (
	"context"
	"fmt"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/duckdbfile"
)

// Type is the config identifier for this encoder.
const Type = "duckdb"

// Config configures the duckdb encoder.
type Config struct {
	// StorageVersion pins STORAGE_VERSION on created files. Empty uses the
	// shared default matching the store's go-duckdb line.
	StorageVersion string `json:"storage_version"`
}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the duckdb encoder factory.
func NewFactory() component.Factory[component.Encoder] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Encoder, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("encoder/duckdb: unexpected config type %T", cfg)
	}
	sv := c.StorageVersion
	if sv == "" {
		sv = duckdbfile.DefaultStorageVersion
	}
	return &encoder{storageVersion: sv}, nil
}

type encoder struct {
	storageVersion string
}

func (*encoder) Start(context.Context, component.Host) error { return nil }
func (*encoder) Shutdown(context.Context) error              { return nil }

// Encode is intentionally unimplemented in the test: commit (TDD red).
func (e *encoder) Encode(_ context.Context, in data.RecordBatch) (data.EncodedBlock, error) {
	defer in.Release()
	_ = e.storageVersion
	return data.EncodedBlock{}, fmt.Errorf("encoder/duckdb: not implemented")
}
