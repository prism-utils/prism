// Package dir implements an Output that writes each encoded block to its own
// file in a directory, via a temp-file + atomic rename so a reader never sees a
// partial file. This is the correct sink for self-contained blocks (one Parquet
// or JSON file per buffer window); the append-only file output would corrupt
// such formats by concatenating them.
package dir

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prism-utils/prism/internal/component"
	"github.com/prism-utils/prism/internal/data"
)

// tsLayout is a compact, filesystem-safe, lexically-sortable UTC timestamp
// (basic ISO-8601, fixed 9-digit nanos) used for the window bounds in a name.
const tsLayout = "20060102T150405.000000000Z"

// Type is the config identifier for this output.
const Type = "dir"

// Config configures the directory output.
type Config struct {
	// Dir is the destination directory (created if missing). Required. Exactly
	// one output should write to a given directory; two outputs sharing a
	// directory is a misconfiguration (their sequence counters are independent).
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

// freeName returns a destination path that does not yet exist, bumping the
// sequence counter until it finds a free name. The caller must hold o.mu.
func (o *Output) freeName(block data.EncodedBlock, ext string) string {
	for {
		final := filepath.Join(o.cfg.Dir, o.fileName(block, o.seq.Add(1), ext))
		if _, err := os.Stat(final); errors.Is(err, os.ErrNotExist) {
			return final
		}
	}
}

// fileName builds the artifact name. With window provenance it encodes the time
// range — <prefix><pipeline>-<phase>-<start>-<end>-<seq>.<ext> — so a consumer
// selects files for a timestamp range by name alone. Without provenance it falls
// back to the legacy <prefix><nanos>-<seq>.<ext>.
func (o *Output) fileName(block data.EncodedBlock, seq uint64, ext string) string {
	if m := block.Meta; m != nil && m.Pipeline != "" && m.Branch != "" &&
		!m.Window.Start.IsZero() && !m.Window.End.IsZero() {
		return fmt.Sprintf("%s%s-%s-%s-%s-%d.%s",
			o.cfg.Prefix,
			safe(m.Pipeline), safe(m.Branch),
			m.Window.Start.UTC().Format(tsLayout),
			m.Window.End.UTC().Format(tsLayout),
			seq, ext)
	}
	return fmt.Sprintf("%s%d-%d.%s", o.cfg.Prefix, time.Now().UnixNano(), seq, ext)
}

// safe maps any character outside [A-Za-z0-9_.] to '-' so names stay portable
// and the '-' field separator is unambiguous for pipeline/branch tokens.
func safe(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '.':
			return r
		default:
			return '-'
		}
	}, s)
}

// Consume writes one block to a uniquely-named file (temp + atomic rename).
func (o *Output) Consume(_ context.Context, block data.EncodedBlock) error {
	if len(block.Bytes) == 0 {
		return nil // nothing to persist for an empty window
	}
	ext := o.cfg.Extension
	if ext == "" {
		ext = block.Format
	}

	// Serialize name selection + rename so concurrent branches never collide,
	// and so a restart (seq resets to 0) with a deterministic time-range name
	// never overwrites an existing window file.
	o.mu.Lock()
	defer o.mu.Unlock()
	final := o.freeName(block, ext)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, block.Bytes, 0o600); err != nil {
		return fmt.Errorf("output/dir: write %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("output/dir: rename %q: %w", final, err)
	}
	return nil
}
