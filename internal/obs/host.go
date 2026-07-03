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
}

// NewHost builds a Host backed by the given logger. A nil logger is replaced
// with a discard logger so components never have to nil-check.
func NewHost(logger *slog.Logger) *Host {
	if logger == nil {
		logger = NewLogger(io.Discard, slog.LevelInfo)
	}
	return &Host{logger: logger}
}

// Logger returns the component logger.
func (h *Host) Logger() *slog.Logger { return h.logger }

// static assertion: *Host implements component.Host.
var _ component.Host = (*Host)(nil)
