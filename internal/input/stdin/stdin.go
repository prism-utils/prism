// Package stdin implements an Input that reads newline-delimited records from
// standard input (or any io.Reader, for tests) and emits them as bounded
// RawBatches.
package stdin

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/lineio"
)

const (
	// Type is the config identifier for this input.
	Type = "stdin"

	defaultBatchSize = 1000
)

// Config configures the stdin input.
type Config struct {
	// BatchSize is the max number of records per emitted RawBatch.
	BatchSize int `json:"batch_size"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.BatchSize <= 0 {
		return fmt.Errorf("stdin.batch_size: must be > 0, got %d", c.BatchSize)
	}
	return nil
}

type factory struct{}

// NewFactory returns the stdin input factory.
func NewFactory() component.Factory[component.Input] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{BatchSize: defaultBatchSize} }

func (factory) Create(cfg component.Config, set component.Settings) (component.Input, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("stdin: unexpected config type %T", cfg)
	}
	return &Input{
		cfg:     *c,
		reader:  os.Stdin,
		batches: make(chan data.RawBatch, 1),
		log:     set.Logger,
	}, nil
}

// Input reads lines from reader and emits RawBatches.
type Input struct {
	cfg     Config
	reader  io.Reader
	batches chan data.RawBatch
	log     *slog.Logger
}

// Batches returns the channel of emitted RawBatches; closed at EOF/cancel.
func (in *Input) Batches() <-chan data.RawBatch { return in.batches }

// Start launches the producer goroutine. It returns immediately.
func (in *Input) Start(ctx context.Context, _ component.Host) error {
	go in.produce(ctx)
	return nil
}

// Shutdown is a no-op; the producer stops on ctx cancellation or EOF.
func (in *Input) Shutdown(context.Context) error { return nil }

func (in *Input) produce(ctx context.Context) {
	defer close(in.batches)
	if err := lineio.ScanLines(ctx, in.reader, Type, in.cfg.BatchSize, in.batches); err != nil && in.log != nil {
		in.log.Debug("stdin: scan stopped", "err", err)
	}
}
