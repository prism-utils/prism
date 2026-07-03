// Package dir implements an Output that writes each encoded block to its own
// file in a directory, via a temp-file + atomic rename so a reader never sees a
// partial file. This is the correct sink for self-contained blocks (one Parquet
// or JSON file per buffer window); the append-only file output would corrupt
// such formats by concatenating them.
package dir

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// Type is the config identifier for this output.
const Type = "dir"

// Config configures the directory output.
type Config struct {
	// Dir is the destination directory (created if missing). Required.
	Dir string `json:"dir"`
	// Prefix is prepended to every file name (optional).
	Prefix string `json:"prefix"`
	// Extension overrides the file extension; defaults to the block's format.
	Extension string `json:"extension"`
}

// Validate implements component.Config.
func (c *Config) Validate() error {
	if c.Dir == "" {
		return fmt.Errorf("dir.dir: required, must not be empty")
	}
	return nil
}

type factory struct{}

// NewFactory returns the directory output factory.
func NewFactory() component.Factory[component.Output] { return factory{} }

func (factory) Type() string                    { return Type }
func (factory) DefaultConfig() component.Config { return &Config{} }

func (factory) Create(cfg component.Config, _ component.Settings) (component.Output, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("output/dir: unexpected config type %T", cfg)
	}
	return &Output{cfg: *c}, nil
}

// Output writes blocks to files in a directory.
type Output struct {
	cfg Config
	seq atomic.Uint64
	mu  sync.Mutex
}

// Start ensures the destination directory exists.
func (o *Output) Start(context.Context, component.Host) error {
	if err := os.MkdirAll(o.cfg.Dir, 0o750); err != nil {
		return fmt.Errorf("output/dir: mkdir %q: %w", o.cfg.Dir, err)
	}
	return nil
}

// Shutdown is a no-op; each block is fully flushed in Consume.
func (o *Output) Shutdown(context.Context) error { return nil }

// Consume writes one block to a uniquely-named file (temp + atomic rename).
func (o *Output) Consume(_ context.Context, block data.EncodedBlock) error {
	if len(block.Bytes) == 0 {
		return nil // nothing to persist for an empty window
	}
	ext := o.cfg.Extension
	if ext == "" {
		ext = block.Format
	}
	name := fmt.Sprintf("%s%d-%d.%s", o.cfg.Prefix, time.Now().UnixNano(), o.seq.Add(1), ext)
	final := filepath.Join(o.cfg.Dir, name)
	tmp := final + ".tmp"

	// Serialize renames so concurrent branches never collide on a name.
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := os.WriteFile(tmp, block.Bytes, 0o600); err != nil {
		return fmt.Errorf("output/dir: write %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("output/dir: rename %q: %w", final, err)
	}
	return nil
}
