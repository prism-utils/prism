// Package file implements an Input that reads a file. The foundation supports
// mode "batch" (read the whole file as bounded RawBatches, then reach EOF and
// stop). Mode "tail" (follow + rotation) lands in Phase 3 of docs/PLAN.md.
package file

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/lineio"
)

const (
	// Type is the config identifier for this input.
	Type = "file"

	// ModeBatch reads the whole file then stops.
	ModeBatch = "batch"
	// ModeTail follows the file (Phase 3, not yet implemented).
	ModeTail = "tail"

	defaultBatchSize = 1000
)

// Config configures the file input.
type Config struct {
	// Path is the file to read. Required.
	Path string `json:"path"`
	// Mode is "batch" (default) or "tail" (Phase 3).
	Mode string `json:"mode"`
	// BatchSize is the max number of records per emitted RawBatch.
	BatchSize int `json:"batch_size"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("file.path: required, must not be empty")
	}
	switch c.Mode {
	case "", ModeBatch:
		// ok
	case ModeTail:
		return fmt.Errorf("file.mode: %q is not implemented until Phase 3", ModeTail)
	default:
		return fmt.Errorf("file.mode: must be %q or %q, got %q", ModeBatch, ModeTail, c.Mode)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("file.batch_size: must be > 0, got %d", c.BatchSize)
	}
	return nil
}

type factory struct{}

// NewFactory returns the file input factory.
func NewFactory() component.Factory[component.Input] { return factory{} }

func (factory) Type() string { return Type }
func (factory) DefaultConfig() component.Config {
	return &Config{Mode: ModeBatch, BatchSize: defaultBatchSize}
}

func (factory) Create(cfg component.Config, set component.Settings) (component.Input, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("file: unexpected config type %T", cfg)
	}
	return &Input{cfg: *c, batches: make(chan data.RawBatch, 1), log: set.Logger}, nil
}

// Input reads a file and emits RawBatches.
type Input struct {
	cfg     Config
	f       *os.File
	batches chan data.RawBatch
	log     *slog.Logger
}

// Batches returns the channel of emitted RawBatches; closed at EOF/cancel.
func (in *Input) Batches() <-chan data.RawBatch { return in.batches }

// Start opens the file (surfacing a missing-file error synchronously) and
// launches the producer goroutine.
func (in *Input) Start(ctx context.Context, _ component.Host) error {
	f, err := os.Open(in.cfg.Path)
	if err != nil {
		return fmt.Errorf("file: open %q: %w", in.cfg.Path, err)
	}
	in.f = f
	go in.produce(ctx)
	return nil
}

// Shutdown closes the underlying file if still open.
func (in *Input) Shutdown(context.Context) error {
	if in.f != nil {
		return in.f.Close()
	}
	return nil
}

func (in *Input) produce(ctx context.Context) {
	defer close(in.batches)
	if err := lineio.ScanLines(ctx, in.f, in.cfg.Path, in.cfg.BatchSize, in.batches); err != nil && in.log != nil {
		in.log.Debug("file: scan stopped", "path", in.cfg.Path, "err", err)
	}
}
