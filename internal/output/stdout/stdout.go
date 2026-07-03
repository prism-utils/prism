// Package stdout implements an Output that writes encoded blocks to standard
// output (or any io.Writer, for tests).
package stdout

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this output.
const Type = "stdout"

// Config configures the stdout output (no options).
type Config struct{}

// Validate implements component.Config.
func (*Config) Validate() error { return nil }

type factory struct{}

// NewFactory returns the stdout output factory.
func NewFactory() component.Factory[component.Output] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Output, error) {
	if _, ok := cfg.(*Config); !ok {
		return nil, fmt.Errorf("output/stdout: unexpected config type %T", cfg)
	}
	return &Output{w: os.Stdout}, nil
}

// Output writes block bytes to w.
type Output struct {
	w io.Writer
}

// Start is a no-op; stdout needs no setup.
func (*Output) Start(context.Context, component.Host) error { return nil }

// Shutdown is a no-op; stdout is not owned by this component.
func (*Output) Shutdown(context.Context) error { return nil }

// Consume writes the block payload to the underlying writer.
func (o *Output) Consume(_ context.Context, block data.EncodedBlock) error {
	if _, err := o.w.Write(block.Bytes); err != nil {
		return fmt.Errorf("output/stdout: write: %w", err)
	}
	return nil
}
