// Package file implements an Output that appends encoded blocks to a file.
// Rotation (size/time) and atomic rename land in Phase 5 of docs/PLAN.md.
package file

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this output.
const Type = "file"

// Config configures the file output.
type Config struct {
	// Path is the destination file. Required.
	Path string `json:"path"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("file.path: required, must not be empty")
	}
	return nil
}

type factory struct{}

// NewFactory returns the file output factory.
func NewFactory() component.Factory[component.Output] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Output, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("output/file: unexpected config type %T", cfg)
	}
	return &Output{cfg: *c}, nil
}

// Output appends block bytes to a file opened at Start.
type Output struct {
	cfg Config
	mu  sync.Mutex
	f   *os.File
}

// Start opens (creating if needed) the destination file for appending.
func (o *Output) Start(context.Context, component.Host) error {
	f, err := os.OpenFile(o.cfg.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("output/file: open %q: %w", o.cfg.Path, err)
	}
	o.f = f
	return nil
}

// Shutdown closes the file.
func (o *Output) Shutdown(context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.f == nil {
		return nil
	}
	err := o.f.Close()
	o.f = nil
	return err
}

// Consume appends the block payload to the file.
func (o *Output) Consume(_ context.Context, block data.EncodedBlock) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.f == nil {
		return fmt.Errorf("output/file: not started")
	}
	if _, err := o.f.Write(block.Bytes); err != nil {
		return fmt.Errorf("output/file: write: %w", err)
	}
	return nil
}
