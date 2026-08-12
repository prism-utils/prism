package data_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/prism-utils/prism/internal/data"
)

func TestNewLinesBatch_ShapeAndValues(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	rows := [][]byte{[]byte("line-1"), []byte("line-2"), []byte("line-3")}
	b := data.NewLinesBatch(mem, "src", rows)
	defer b.Release()

	if b.Source != "src" {
		t.Fatalf("source = %q, want src", b.Source)
	}
	if b.Len() != 3 {
		t.Fatalf("len = %d, want 3", b.Len())
	}
	rec := b.Record()
	if rec == nil || rec.NumCols() != 1 {
		t.Fatalf("record cols = %v, want 1", rec)
	}
}

func TestRecordBatch_ReleaseIsIdempotentAndBalances(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)

	b := data.NewLinesBatch(mem, "s", [][]byte{[]byte("a"), []byte("b")})
	b.Release()
	b.Release() // idempotent: must not panic or double-free

	mem.AssertSize(t, 0)
}

func TestRecordBatch_EmptyIsZeroRows(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	b := data.NewLinesBatch(mem, "s", nil)
	defer b.Release()
	if b.Len() != 0 {
		t.Fatalf("empty batch len = %d, want 0", b.Len())
	}

	var zero data.RecordBatch
	if zero.Len() != 0 {
		t.Fatalf("zero-value len = %d, want 0", zero.Len())
	}
	zero.Release() // zero value must be safe to release
}

// Fan-out ownership: two branches each hold an independent reference to the same
// immutable columns; each Releases its own copy and the allocator balances only
// after both do.
func TestRecordBatch_RetainEnablesIndependentFanoutRelease(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)

	src := data.NewLinesBatch(mem, "s", [][]byte{[]byte("x")})

	branchA := src
	branchB := src
	branchB.Retain() // second reference for the second branch

	branchA.Release()
	if mem.CurrentAlloc() == 0 {
		t.Fatal("columns freed after first branch released; retain did not hold a ref")
	}
	branchB.Release()

	mem.AssertSize(t, 0)
}
