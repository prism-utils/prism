// Package component defines the core contracts every prism building block
// implements, and the Registry/Factory machinery that turns a config "type"
// string into a live component.
//
// This is the backbone described in docs/DESIGN.md §3–§4. The rule that keeps
// prism extensible: adding a capability means implementing one of these small
// interfaces plus a Factory, and registering it — with zero edits to the
// pipeline runtime.
package component

import (
	"context"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/data"
)

// Config is the typed configuration for a single component. Every component's
// config implements Validate, which is total and runs at load time so a bad
// config never reaches the runtime (docs/DESIGN.md §7). Validate errors must
// name the offending path and constraint.
type Config interface {
	Validate() error
}

// Settings carries build-time capabilities handed to a Factory when it creates
// a component. It is deliberately small; grow it only with genuinely
// cross-cutting concerns (never a grab-bag).
type Settings struct {
	// Logger is the structured logger the component should use. Never nil;
	// callers pass a no-op logger if they want silence.
	Logger *slog.Logger
}

// Host is the narrow capability surface a running component may touch. It is
// how components get dependencies — never via globals (docs/DESIGN.md §3.1).
// It grows in later phases (metrics, temp dir, buffer allocator).
type Host interface {
	// Logger returns the structured logger for this component.
	Logger() *slog.Logger
	// Allocator returns the Arrow buffer allocator components must use for
	// RecordBatch columns, so buffer ownership is accountable and poolable.
	Allocator() memory.Allocator
}

// Component is the common lifecycle contract. Start MUST return promptly;
// long-running work belongs on a goroutine that respects ctx cancellation.
// Shutdown MUST be idempotent and flush/close within the caller's ctx deadline.
type Component interface {
	Start(ctx context.Context, host Host) error
	Shutdown(ctx context.Context) error
}

// Input produces RawBatches until exhausted (batch/stdin) or ctx is cancelled
// (tail). The returned channel is closed when no more data will arrive.
// Backpressure is the channel itself: a slow pipeline slows the input.
type Input interface {
	Component
	Batches() <-chan data.RawBatch
}

// Parser converts raw bytes into a structured RecordBatch, discovering and
// evolving schema as needed. It must never panic on malformed input; it returns
// an error that the runtime routes per the configured failure policy.
type Parser interface {
	Component
	Parse(ctx context.Context, in data.RawBatch) (data.RecordBatch, error)
}

// Processor transforms a batch: it may drop rows, add columns, or aggregate.
// Returning a zero-row batch is valid and means "fully filtered".
type Processor interface {
	Component
	Process(ctx context.Context, in data.RecordBatch) (data.RecordBatch, error)
}

// Encoder serializes a batch into a self-contained EncodedBlock. Encoders own
// their buffering and MUST release the batch's buffers when done.
type Encoder interface {
	Component
	Encode(ctx context.Context, in data.RecordBatch) (data.EncodedBlock, error)
}

// Output ships one encoded block, owning ret/ack semantics for its transport.
// Errors are returned for the pipeline's retry/failure policy to act on.
type Output interface {
	Component
	Consume(ctx context.Context, block data.EncodedBlock) error
}

// Factory builds a typed component of kind T from its typed Config. There is
// exactly one Factory per "type" string used in configuration.
type Factory[T any] interface {
	// Type is the stable identifier used in config (e.g. "file", "parquet").
	Type() string
	// DefaultConfig returns a Config populated with sane defaults. User config
	// is overlaid on top of this, then Validate is called.
	DefaultConfig() Config
	// Create builds the component from a validated Config and build Settings.
	Create(cfg Config, set Settings) (T, error)
}
