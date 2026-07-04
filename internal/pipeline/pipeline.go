package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"

	"github.com/elk-utilities/prism/internal/buffer"
	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
)

// shutdownGrace bounds how long Shutdown may take once a pipeline has stopped.
const shutdownGrace = 10 * time.Second

// chanCap is the per-stage bounded-channel capacity. Capacity IS the
// backpressure: a slow branch fills its channel, stalling fan-out, the buffer,
// the parser, and finally the input. Kept small so memory stays flat.
const chanCap = 8

// Run drives every pipeline concurrently, one worker per input. Pipelines are
// isolated: a fatal error in one is logged and stops only that pipeline; the
// others keep running. Cancelling ctx (a signal) stops them all. Run returns
// the joined non-nil errors after every pipeline has drained.
func (s *Set) Run(ctx context.Context, host component.Host) error {
	var wg sync.WaitGroup
	errs := make([]error, len(s.pipelines))
	for i := range s.pipelines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.runPipeline(ctx, host, &s.pipelines[i]); err != nil {
				s.log.Error("pipeline stopped with error", "pipeline", s.pipelines[i].name, "err", err)
				errs[i] = fmt.Errorf("pipeline %q: %w", s.pipelines[i].name, err)
			}
		}(i)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (s *Set) runPipeline(ctx context.Context, host component.Host, p *builtPipeline) error {
	// gctx is the pipeline's lifetime: components start under it so a stage
	// failure (which cancels gctx) also stops the input's producer goroutine,
	// not just the internal stages.
	g, gctx := errgroup.WithContext(ctx)

	comps := p.orderedComponents()
	for i, c := range comps {
		if err := c.Start(gctx, host); err != nil {
			_ = shutdown(ctx, comps[:i])
			return fmt.Errorf("start: %w", err)
		}
	}

	mem := host.Allocator()
	parsed := make(chan data.RecordBatch, chanCap)
	windows := make(chan data.RecordBatch, chanCap)
	branchChs := make([]chan data.RecordBatch, len(p.branches))
	for i := range branchChs {
		branchChs[i] = make(chan data.RecordBatch, chanCap)
	}

	g.Go(func() error { return p.parseStage(gctx, parsed) })
	g.Go(func() error { return p.bufferStage(gctx, mem, parsed, windows) })
	g.Go(func() error { return p.fanoutStage(gctx, windows, branchChs) })
	for i := range p.branches {
		g.Go(func() error { return p.branchStage(gctx, s.log, i, branchChs[i]) })
	}

	runErr := g.Wait()
	shutErr := shutdown(ctx, comps)
	return errors.Join(runErr, shutErr)
}

// parseStage parses each RawBatch, applies pre-buffer processors in order, and
// forwards the RecordBatch. It closes parsed when the input is exhausted or ctx
// is cancelled.
func (p *builtPipeline) parseStage(ctx context.Context, parsed chan<- data.RecordBatch) error {
	defer close(parsed)
	in := p.input.Batches()
	for {
		select {
		case <-ctx.Done():
			return nil
		case raw, ok := <-in:
			if !ok {
				return nil
			}
			rb, err := p.parser.Parse(ctx, raw)
			if err != nil {
				if dropped, derr := p.onProcessingError("parse", err); !dropped {
					return derr
				}
				continue
			}
			rb, err = applyProcessors(ctx, p.pre, rb)
			if err != nil {
				rb.Release()
				if dropped, derr := p.onProcessingError("pre-processor", err); !dropped {
					return derr
				}
				continue
			}
			if !send(ctx, parsed, rb) {
				rb.Release()
				return nil
			}
		}
	}
}

// bufferStage accumulates parsed batches and flushes a window on the first of
// the buffer's bounds; an age timer flushes an idle window. It closes windows
// when parsed closes (draining the partial window) or ctx is cancelled.
func (p *builtPipeline) bufferStage(ctx context.Context, mem memory.Allocator, parsed <-chan data.RecordBatch, windows chan<- data.RecordBatch) error {
	defer close(windows)
	acc := buffer.New(p.bufCfg, mem)

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	armed := false
	arm := func() {
		if armed {
			return
		}
		if dl, ok := acc.Deadline(); ok {
			timer.Reset(time.Until(dl))
			armed = true
		}
	}

	flush := func() error {
		start := acc.WindowStart() // capture before Flush resets it
		win, ok, err := acc.Flush()
		armed = false
		if err != nil {
			return fmt.Errorf("buffer: %w", err)
		}
		if !ok {
			return nil
		}
		win.Window = &data.TimeWindow{Start: start, End: time.Now()}
		if !send(ctx, windows, win) {
			win.Release()
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return flush()
		case <-timer.C:
			armed = false
			if acc.AgeExceeded(time.Now()) {
				if err := flush(); err != nil {
					return err
				}
			} else {
				arm()
			}
		case rb, ok := <-parsed:
			if !ok {
				return flush()
			}
			if acc.Add(rb, time.Now()) {
				if err := flush(); err != nil {
					return err
				}
			} else {
				arm()
			}
		}
	}
}

// fanoutStage dispatches each window to every branch. Each branch receives an
// independently-owned reference (Retain) so branches release independently.
func (p *builtPipeline) fanoutStage(ctx context.Context, windows <-chan data.RecordBatch, branchChs []chan data.RecordBatch) error {
	defer func() {
		for _, ch := range branchChs {
			close(ch)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case win, ok := <-windows:
			if !ok {
				return nil
			}
			// One reference already exists; add one per additional branch.
			for range branchChs[1:] {
				win.Retain()
			}
			for _, ch := range branchChs {
				if !send(ctx, ch, win) {
					win.Release()
				}
			}
		}
	}
}

// branchStage runs one branch's processors → encoder → output for each window.
func (p *builtPipeline) branchStage(ctx context.Context, log *slog.Logger, idx int, in <-chan data.RecordBatch) error {
	br := p.branches[idx]
	for {
		select {
		case <-ctx.Done():
			return nil
		case win, ok := <-in:
			if !ok {
				return nil
			}
			if err := p.runBranch(ctx, br, win); err != nil {
				if dropped, derr := p.onProcessingError("branch "+br.name, err); !dropped {
					return derr
				}
				log.Warn("branch dropped a window", "pipeline", p.name, "branch", br.name, "err", err)
			}
		}
	}
}

func (p *builtPipeline) runBranch(ctx context.Context, br branch, win data.RecordBatch) error {
	// Capture the window's provenance before branch processors replace the batch
	// (summary/template build new batches without the window fields).
	var window data.TimeWindow
	if win.Window != nil {
		window = *win.Window
	}
	cur, err := applyProcessors(ctx, br.procs, win)
	if err != nil {
		cur.Release()
		return err
	}
	// The encoder takes ownership of cur and releases it (component.Encoder).
	block, err := br.encoder.Encode(ctx, cur)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	block.Meta = &data.BlockMeta{Pipeline: p.name, Branch: br.name, Window: window}
	if err := br.output.Consume(ctx, block); err != nil {
		return fmt.Errorf("output: %w", err)
	}
	return nil
}

// applyProcessors runs processors in order. Each takes ownership of its input
// and returns the batch the caller now owns.
func applyProcessors(ctx context.Context, procs []component.Processor, rb data.RecordBatch) (data.RecordBatch, error) {
	cur := rb
	for i, proc := range procs {
		next, err := proc.Process(ctx, cur)
		if err != nil {
			return cur, fmt.Errorf("processor[%d]: %w", i, err)
		}
		cur = next
	}
	return cur, nil
}

// onProcessingError applies the pipeline's failure policy. dropped=true means
// "skip and continue"; dropped=false returns the error to stop the pipeline.
func (p *builtPipeline) onProcessingError(stage string, err error) (dropped bool, out error) {
	if p.onError == policyDrop {
		return true, nil
	}
	return false, fmt.Errorf("%s: %w", stage, err)
}

// send delivers v on ch unless ctx is cancelled first. It returns false when
// ctx won the race, so the caller can release v.
func send(ctx context.Context, ch chan<- data.RecordBatch, v data.RecordBatch) bool {
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *builtPipeline) orderedComponents() []component.Component {
	cs := make([]component.Component, 0, 3+len(p.pre)+len(p.branches)*2)
	cs = append(cs, p.input, p.parser)
	for _, proc := range p.pre {
		cs = append(cs, proc)
	}
	for _, br := range p.branches {
		for _, proc := range br.procs {
			cs = append(cs, proc)
		}
		cs = append(cs, br.encoder, br.output)
	}
	return cs
}

// shutdown stops components in reverse start order within a bounded grace
// period, detaching cancellation so a cancelled run still drains cleanly.
func shutdown(parent context.Context, comps []component.Component) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), shutdownGrace)
	defer cancel()
	var errs []error
	for i := len(comps) - 1; i >= 0; i-- {
		if err := comps[i].Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
