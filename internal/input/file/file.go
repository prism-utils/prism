// Package file implements an Input that reads a file. Mode "batch" reads the
// whole file as bounded RawBatches then reaches EOF and stops. Mode "tail"
// follows the file (like tail -F) across truncation and rotation via
// nxadm/tail, emitting one line per RawBatch until the run is cancelled.
package file

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/nxadm/tail"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/lineio"
)

const (
	// Type is the config identifier for this input.
	Type = "file"

	// ModeBatch reads the whole file then stops.
	ModeBatch = "batch"
	// ModeTail follows the file across rotation until cancelled.
	ModeTail = "tail"

	defaultBatchSize = 1000
)

// Config configures the file input.
type Config struct {
	// Path is the file to read. Required.
	Path string `json:"path"`
	// Mode is "batch" (default) or "tail".
	Mode string `json:"mode"`
	// BatchSize is the max number of records per emitted RawBatch (batch mode).
	BatchSize int `json:"batch_size"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("file.path: required, must not be empty")
	}
	switch c.Mode {
	case "", ModeBatch, ModeTail:
		// ok
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
	tailer  *tail.Tail
	batches chan data.RawBatch
	log     *slog.Logger
}

// Batches returns the channel of emitted RawBatches; closed at EOF/cancel.
func (in *Input) Batches() <-chan data.RawBatch { return in.batches }

// Start opens the source and launches the producer goroutine. A missing file
// surfaces synchronously in batch mode.
func (in *Input) Start(ctx context.Context, _ component.Host) error {
	if in.cfg.Mode == ModeTail {
		t, err := tail.TailFile(in.cfg.Path, tail.Config{
			Follow:        true,
			ReOpen:        true,
			CompleteLines: true,
			Location:      &tail.SeekInfo{Whence: io.SeekStart},
			Logger:        tail.DiscardingLogger,
		})
		if err != nil {
			return fmt.Errorf("file: tail %q: %w", in.cfg.Path, err)
		}
		in.tailer = t
		go in.tailProduce(ctx)
		return nil
	}
	f, err := os.Open(in.cfg.Path)
	if err != nil {
		return fmt.Errorf("file: open %q: %w", in.cfg.Path, err)
	}
	in.f = f
	go in.batchProduce(ctx)
	return nil
}

// Shutdown closes the source; the tailer's goroutine stops on ctx or here.
func (in *Input) Shutdown(context.Context) error {
	if in.tailer != nil {
		return in.tailer.Stop()
	}
	if in.f != nil {
		return in.f.Close()
	}
	return nil
}

func (in *Input) batchProduce(ctx context.Context) {
	defer close(in.batches)
	if err := lineio.ScanLines(ctx, in.f, in.cfg.Path, in.cfg.BatchSize, in.batches); err != nil && in.log != nil {
		in.log.Debug("file: scan stopped", "path", in.cfg.Path, "err", err)
	}
}

func (in *Input) tailProduce(ctx context.Context) {
	defer close(in.batches)
	defer func() { _ = in.tailer.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-in.tailer.Lines:
			if !ok {
				return
			}
			if line.Err != nil {
				if in.log != nil {
					in.log.Debug("file: tail error", "path", in.cfg.Path, "err", line.Err)
				}
				continue
			}
			batch := data.RawBatch{Source: in.cfg.Path, Records: [][]byte{[]byte(line.Text)}}
			select {
			case in.batches <- batch:
			case <-ctx.Done():
				return
			}
		}
	}
}
