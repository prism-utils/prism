package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/buffer"
	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/data"
	"github.com/elk-utilities/prism/internal/obs"
)

// --- fakes -----------------------------------------------------------------

// sliceInput emits a fixed set of RawBatches then closes, counting successful
// sends so backpressure can be observed.
type sliceInput struct {
	batches []data.RawBatch
	ch      chan data.RawBatch
	sent    *int32
}

func (s *sliceInput) Start(ctx context.Context, _ component.Host) error {
	s.ch = make(chan data.RawBatch)
	go func() {
		defer close(s.ch)
		for _, b := range s.batches {
			select {
			case s.ch <- b:
				if s.sent != nil {
					atomic.AddInt32(s.sent, 1)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
func (s *sliceInput) Shutdown(context.Context) error { return nil }
func (s *sliceInput) Batches() <-chan data.RawBatch  { return s.ch }

// linesParser turns raw records into a one-column lines batch via the host
// allocator (so the checked allocator can prove ownership balances).
type linesParser struct{ mem memory.Allocator }

func (p *linesParser) Start(_ context.Context, host component.Host) error {
	p.mem = host.Allocator()
	return nil
}
func (p *linesParser) Shutdown(context.Context) error { return nil }
func (p *linesParser) Parse(_ context.Context, in data.RawBatch) (data.RecordBatch, error) {
	return data.NewLinesBatch(p.mem, in.Source, in.Records), nil
}

// errParser always fails, to exercise the failure policy.
type errParser struct{}

func (errParser) Start(context.Context, component.Host) error { return nil }
func (errParser) Shutdown(context.Context) error              { return nil }
func (errParser) Parse(context.Context, data.RawBatch) (data.RecordBatch, error) {
	return data.RecordBatch{}, errors.New("parse boom")
}

// countEncoder records the row count and releases the batch (encoders own it).
type countEncoder struct{}

func (countEncoder) Start(context.Context, component.Host) error { return nil }
func (countEncoder) Shutdown(context.Context) error              { return nil }
func (countEncoder) Encode(_ context.Context, in data.RecordBatch) (data.EncodedBlock, error) {
	rows := in.Len()
	in.Release()
	return data.EncodedBlock{Format: "count", Rows: rows}, nil
}

// collectOutput tallies rows and blocks it consumes.
type collectOutput struct {
	mu     sync.Mutex
	rows   int
	blocks int
}

func (c *collectOutput) Start(context.Context, component.Host) error { return nil }
func (c *collectOutput) Shutdown(context.Context) error              { return nil }
func (c *collectOutput) Consume(_ context.Context, block data.EncodedBlock) error {
	c.mu.Lock()
	c.rows += block.Rows
	c.blocks++
	c.mu.Unlock()
	return nil
}
func (c *collectOutput) snapshot() (rows, blocks int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rows, c.blocks
}

// gateOutput blocks in Consume until gate is closed, so backpressure is
// observable, then tallies like collectOutput.
type gateOutput struct {
	gate chan struct{}
	collectOutput
}

func (g *gateOutput) Consume(ctx context.Context, block data.EncodedBlock) error {
	<-g.gate
	return g.collectOutput.Consume(ctx, block)
}

func testLogger() *slog.Logger { return obs.NewLogger(io.Discard, slog.LevelError) }

func rawBatch(src string, recs ...string) data.RawBatch {
	rb := data.RawBatch{Source: src}
	for _, r := range recs {
		rb.Records = append(rb.Records, []byte(r))
	}
	return rb
}

func runWithTimeout(t *testing.T, set *Set, host component.Host, d time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- set.Run(context.Background(), host) }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatal("Run did not return within timeout (possible deadlock)")
		return nil
	}
}

// --- tests -----------------------------------------------------------------

// Fan-out gives every branch an independent reference to the same window; both
// branches must observe every row, and the allocator must balance afterwards.
func TestRun_FanoutDeliversToEveryBranch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	host := obs.NewHostWithAllocator(testLogger(), mem)

	outA, outB := &collectOutput{}, &collectOutput{}
	bp := builtPipeline{
		name:    "p",
		input:   &sliceInput{batches: []data.RawBatch{rawBatch("s", "a", "b"), rawBatch("s", "c"), rawBatch("s", "d", "e", "f")}},
		parser:  &linesParser{},
		bufCfg:  buffer.Config{MaxRows: 1}, // one window per input batch
		onError: policyBlock,
		branches: []branch{
			{name: "A", encoder: countEncoder{}, output: outA},
			{name: "B", encoder: countEncoder{}, output: outB},
		},
	}
	set := &Set{pipelines: []builtPipeline{bp}, log: testLogger()}

	if err := runWithTimeout(t, set, host, 5*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}

	const wantRows, wantBlocks = 6, 3
	for name, out := range map[string]*collectOutput{"A": outA, "B": outB} {
		rows, blocks := out.snapshot()
		if rows != wantRows || blocks != wantBlocks {
			t.Fatalf("branch %s: rows=%d blocks=%d, want %d/%d", name, rows, blocks, wantRows, wantBlocks)
		}
	}
	mem.AssertSize(t, 0)
}

// "block" makes a parse error fatal to the pipeline; Run reports it.
func TestRun_FailurePolicyBlockStops(t *testing.T) {
	host := obs.NewHost(testLogger())
	bp := builtPipeline{
		name:     "p",
		input:    &sliceInput{batches: []data.RawBatch{rawBatch("s", "x"), rawBatch("s", "y")}},
		parser:   errParser{},
		bufCfg:   buffer.Config{MaxRows: 1},
		onError:  policyBlock,
		branches: []branch{{name: "A", encoder: countEncoder{}, output: &collectOutput{}}},
	}
	set := &Set{pipelines: []builtPipeline{bp}, log: testLogger()}

	if err := runWithTimeout(t, set, host, 5*time.Second); err == nil {
		t.Fatal("Run: expected error under block policy, got nil")
	}
}

// "drop" skips malformed batches and lets the pipeline finish cleanly.
func TestRun_FailurePolicyDropContinues(t *testing.T) {
	host := obs.NewHost(testLogger())
	out := &collectOutput{}
	bp := builtPipeline{
		name:     "p",
		input:    &sliceInput{batches: []data.RawBatch{rawBatch("s", "x"), rawBatch("s", "y")}},
		parser:   errParser{},
		bufCfg:   buffer.Config{MaxRows: 1},
		onError:  policyDrop,
		branches: []branch{{name: "A", encoder: countEncoder{}, output: out}},
	}
	set := &Set{pipelines: []builtPipeline{bp}, log: testLogger()}

	if err := runWithTimeout(t, set, host, 5*time.Second); err != nil {
		t.Fatalf("Run: drop policy should not error: %v", err)
	}
	if rows, blocks := out.snapshot(); rows != 0 || blocks != 0 {
		t.Fatalf("drop policy: output saw rows=%d blocks=%d, want 0/0", rows, blocks)
	}
}

// One pipeline failing (block) must not stop a healthy sibling; Run reports the
// failure but the healthy pipeline still delivers all its data.
func TestRun_PipelinesAreIsolated(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	host := obs.NewHostWithAllocator(testLogger(), mem)

	okOut := &collectOutput{}
	failing := builtPipeline{
		name:     "bad",
		input:    &sliceInput{batches: []data.RawBatch{rawBatch("s", "x")}},
		parser:   errParser{},
		bufCfg:   buffer.Config{MaxRows: 1},
		onError:  policyBlock,
		branches: []branch{{name: "A", encoder: countEncoder{}, output: &collectOutput{}}},
	}
	healthy := builtPipeline{
		name:     "good",
		input:    &sliceInput{batches: []data.RawBatch{rawBatch("s", "a", "b"), rawBatch("s", "c")}},
		parser:   &linesParser{},
		bufCfg:   buffer.Config{MaxRows: 1},
		onError:  policyBlock,
		branches: []branch{{name: "A", encoder: countEncoder{}, output: okOut}},
	}
	set := &Set{pipelines: []builtPipeline{failing, healthy}, log: testLogger()}

	err := runWithTimeout(t, set, host, 5*time.Second)
	if err == nil {
		t.Fatal("Run: expected the failing pipeline's error, got nil")
	}
	if rows, _ := okOut.snapshot(); rows != 3 {
		t.Fatalf("healthy pipeline delivered rows=%d, want 3 (isolation broken)", rows)
	}
	mem.AssertSize(t, 0)
}

// Backpressure: with the output blocked, bounded channels cap how far the input
// can run ahead. Once unblocked, everything drains.
func TestRun_BackpressureBoundsInput(t *testing.T) {
	host := obs.NewHost(testLogger())

	const total = 200
	var sent int32
	batches := make([]data.RawBatch, total)
	for i := range batches {
		batches[i] = rawBatch("s", "x")
	}
	out := &gateOutput{gate: make(chan struct{})}
	bp := builtPipeline{
		name:     "p",
		input:    &sliceInput{batches: batches, sent: &sent},
		parser:   &linesParser{},
		bufCfg:   buffer.Config{MaxRows: 1},
		onError:  policyBlock,
		branches: []branch{{name: "A", encoder: countEncoder{}, output: out}},
	}
	set := &Set{pipelines: []builtPipeline{bp}, log: testLogger()}

	done := make(chan error, 1)
	go func() { done <- set.Run(context.Background(), host) }()

	time.Sleep(150 * time.Millisecond) // let the pipeline fill every channel
	if n := atomic.LoadInt32(&sent); n >= total {
		t.Fatalf("input emitted all %d batches while output was blocked; no backpressure", n)
	} else if n == 0 {
		t.Fatal("input emitted nothing; pipeline never started")
	}

	close(out.gate) // release the output; everything should drain now
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not drain after unblocking output")
	}
	if rows, _ := out.snapshot(); rows != total {
		t.Fatalf("after drain rows=%d, want %d", rows, total)
	}
}
