package buffer_test

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/elk-utilities/prism/internal/buffer"
	"github.com/elk-utilities/prism/internal/data"
)

func lines(mem memory.Allocator, n int) data.RecordBatch {
	rows := make([][]byte, n)
	for i := range rows {
		rows[i] = []byte("row")
	}
	return data.NewLinesBatch(mem, "s", rows)
}

func mustFlush(t *testing.T, acc *buffer.Accumulator) (data.RecordBatch, bool) {
	t.Helper()
	win, ok, err := acc.Flush()
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return win, ok
}

func TestAccumulator_WindowStart(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	acc := buffer.New(buffer.Config{MaxRows: 100}, mem)

	if !acc.WindowStart().IsZero() {
		t.Fatal("empty accumulator should report a zero window start")
	}
	t0 := time.Unix(1000, 0)
	acc.Add(lines(mem, 1), t0)
	acc.Add(lines(mem, 1), t0.Add(time.Second)) // later add must not move start
	if got := acc.WindowStart(); !got.Equal(t0) {
		t.Fatalf("window start = %v, want %v (the oldest add)", got, t0)
	}
	win, _ := mustFlush(t, acc)
	win.Release()
	if !acc.WindowStart().IsZero() {
		t.Fatal("window start should reset to zero after flush")
	}
	mem.AssertSize(t, 0)
}

func TestAccumulator_FlushOnRows(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	acc := buffer.New(buffer.Config{MaxRows: 5}, mem)

	t0 := time.Unix(0, 0)
	if acc.Add(lines(mem, 3), t0) {
		t.Fatal("3 rows should not trigger a flush (max 5)")
	}
	if !acc.Add(lines(mem, 4), t0) {
		t.Fatal("7 rows should trigger a flush (max 5)")
	}
	win, ok := mustFlush(t, acc)
	if !ok {
		t.Fatal("Flush: expected a window")
	}
	if win.Len() != 7 {
		win.Release()
		t.Fatalf("window rows = %d, want 7", win.Len())
	}
	win.Release()
	mem.AssertSize(t, 0)
}

func TestAccumulator_FlushOnBytes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	acc := buffer.New(buffer.Config{MaxBytes: 1}, mem) // tiny cap: first add trips it

	if !acc.Add(lines(mem, 1), time.Unix(0, 0)) {
		t.Fatal("a non-empty batch should exceed a 1-byte cap")
	}
	win, ok := mustFlush(t, acc)
	if !ok {
		t.Fatal("expected a window")
	}
	win.Release()
	mem.AssertSize(t, 0)
}

func TestAccumulator_AgeExceeded(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	acc := buffer.New(buffer.Config{MaxAge: 30 * time.Second}, mem)

	t0 := time.Unix(100, 0)
	acc.Add(lines(mem, 1), t0)

	if acc.AgeExceeded(t0.Add(29 * time.Second)) {
		t.Fatal("29s < 30s should not be exceeded")
	}
	if !acc.AgeExceeded(t0.Add(30 * time.Second)) {
		t.Fatal("30s should be exceeded (>=)")
	}
	dl, ok := acc.Deadline()
	if !ok || !dl.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("deadline = %v ok=%v, want %v", dl, ok, t0.Add(30*time.Second))
	}
	win, _ := mustFlush(t, acc)
	win.Release()
}

func TestAccumulator_FlushEmptyReturnsFalse(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	acc := buffer.New(buffer.Config{MaxRows: 10}, mem)

	if _, ok := mustFlush(t, acc); ok {
		t.Fatal("Flush on empty accumulator should return ok=false")
	}
	if _, ok := acc.Deadline(); ok {
		t.Fatal("no data => no deadline")
	}
}

func TestAccumulator_ConcatenatesAndBalances(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	acc := buffer.New(buffer.Config{MaxRows: 100}, mem)

	t0 := time.Unix(0, 0)
	acc.Add(lines(mem, 2), t0)
	acc.Add(lines(mem, 3), t0)
	win, ok := mustFlush(t, acc)
	if !ok || win.Len() != 5 {
		win.Release()
		t.Fatalf("combined window rows = %d ok=%v, want 5", win.Len(), ok)
	}
	win.Release()
	mem.AssertSize(t, 0) // inputs released, result released: balanced
}

func TestAccumulator_ResetsAfterFlush(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	acc := buffer.New(buffer.Config{MaxRows: 100}, mem)

	acc.Add(lines(mem, 2), time.Unix(0, 0))
	w1, _ := mustFlush(t, acc)
	w1.Release()

	if _, ok := mustFlush(t, acc); ok {
		t.Fatal("second Flush should be empty after reset")
	}
}
