// Package obs provides prism's observability plumbing: structured logging and
// the Host implementation handed to running components.
//
// It stays deliberately small in the foundation. Phase 8 of docs/PLAN.md grows
// Host with a metrics registry, a temp dir, and the Arrow buffer allocator
// (docs/DESIGN.md §10–§11).
package obs

import (
	"io"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/component"
)

// NewLogger returns a structured text logger writing to w at the given level.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
}

// Host is the concrete capability surface handed to components at Start. It
// satisfies component.Host.
type Host struct {
	logger *slog.Logger
	alloc  memory.Allocator
}

// NewHost builds a Host backed by the given logger and the default Arrow
// allocator. A nil logger is replaced with a discard logger so components never
// have to nil-check.
func NewHost(logger *slog.Logger) *Host {
	return NewHostWithAllocator(logger, nil)
}

// NewHostWithAllocator builds a Host with an explicit allocator. Tests pass a
// checked allocator to assert buffer ownership balances. A nil allocator uses
// the default; a nil logger becomes a discard logger.
func NewHostWithAllocator(logger *slog.Logger, alloc memory.Allocator) *Host {
	if logger == nil {
		logger = NewLogger(io.Discard, slog.LevelInfo)
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	return &Host{logger: logger, alloc: alloc}
}

// Logger returns the component logger.
func (h *Host) Logger() *slog.Logger { return h.logger }

// Allocator returns the Arrow buffer allocator for this host.
func (h *Host) Allocator() memory.Allocator { return h.alloc }

// static assertion: *Host implements component.Host.
var _ component.Host = (*Host)(nil)
