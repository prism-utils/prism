package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// shutdownGrace bounds how long Shutdown may take once the run loop has stopped.
const shutdownGrace = 10 * time.Second

// Run starts every component, drives records input → parse → processors →
// encode → output until the input reaches EOF or ctx is cancelled, then shuts
// components down in reverse order.
//
// This is the foundation's runtime: the input emits over its own bounded
// channel (that channel IS the backpressure between the source and the rest of
// the chain). Phase 2 of docs/PLAN.md expands this into per-stage goroutines
// with bounded channels between every stage and a configurable failure policy;
// for now malformed data fails the run (fail-fast), which is the safest default
// while the surface is small.
func (p *Pipeline) Run(ctx context.Context, host component.Host) error {
	components := p.orderedComponents()

	g, gctx := errgroup.WithContext(ctx)
	for _, c := range components {
		if err := c.Start(gctx, host); err != nil {
			// Best-effort shutdown of whatever already started.
			_ = p.shutdown(ctx)
			return fmt.Errorf("pipeline: start: %w", err)
		}
	}

	g.Go(func() error { return p.consume(gctx) })

	runErr := g.Wait()
	shutErr := p.shutdown(ctx)
	return errors.Join(runErr, shutErr)
}

// consume pulls RawBatches from the input and pushes each through the chain. It
// returns nil on clean EOF (input channel closed) or on ctx cancellation
// (graceful stop), and a wrapped error on the first processing failure.
func (p *Pipeline) consume(ctx context.Context) error {
	batches := p.input.Batches()
	for {
		select {
		case <-ctx.Done():
			return nil
		case raw, ok := <-batches:
			if !ok {
				return nil // EOF: input will emit no more.
			}
			if err := p.processOne(ctx, raw); err != nil {
				return err
			}
		}
	}
}

func (p *Pipeline) processOne(ctx context.Context, raw data.RawBatch) error {
	rb, err := p.parser.Parse(ctx, raw)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	for i, proc := range p.processors {
		rb, err = proc.Process(ctx, rb)
		if err != nil {
			return fmt.Errorf("processor[%d]: %w", i, err)
		}
	}
	block, err := p.encoder.Encode(ctx, rb)
	rb.Release()
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if err := p.output.Consume(ctx, block); err != nil {
		return fmt.Errorf("output: %w", err)
	}
	return nil
}

// orderedComponents returns the components in start order (input first).
func (p *Pipeline) orderedComponents() []component.Component {
	cs := make([]component.Component, 0, 4+len(p.processors))
	cs = append(cs, p.input, p.parser)
	for _, proc := range p.processors {
		cs = append(cs, proc)
	}
	cs = append(cs, p.encoder, p.output)
	return cs
}

// shutdown stops components in reverse start order within a bounded grace
// period, joining any errors so none are lost. It detaches cancellation from
// the parent (context.WithoutCancel) so a cancelled run still drains cleanly,
// while preserving the parent's values.
func (p *Pipeline) shutdown(parent context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), shutdownGrace)
	defer cancel()

	ordered := p.orderedComponents()
	var errs []error
	for i := len(ordered) - 1; i >= 0; i-- {
		if err := ordered[i].Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
