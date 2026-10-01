package engine

import (
	"testing"
	"time"
)

func TestHasOpenPeeksWithoutTouchingLRU(t *testing.T) {
	e := New(Config{DataDir: t.TempDir(), HotWindow: time.Hour, MaxOpenTenants: 2}, time.Now)
	defer func() { _ = e.Close() }()

	const (
		a = "user-hasopen-a-apps"
		b = "user-hasopen-b-apps"
		c = "user-hasopen-c-apps"
	)
	if e.HasOpen(a) {
		t.Fatal("HasOpen before any open must be false")
	}
	if _, err := e.DB(a); err != nil {
		t.Fatalf("open A: %v", err)
	}
	if _, err := e.DB(b); err != nil {
		t.Fatalf("open B: %v", err)
	}
	if !e.HasOpen(a) || !e.HasOpen(b) {
		t.Fatal("HasOpen must report both resident tenants")
	}
	if e.HasOpen(c) {
		t.Fatal("HasOpen of a never-opened tenant must be false")
	}

	// Peek must not refresh recency: A stays oldest, so opening C evicts A.
	if !e.HasOpen(a) {
		t.Fatal("peek must leave A resident")
	}
	if _, err := e.DB(c); err != nil {
		t.Fatalf("open C: %v", err)
	}
	if e.HasOpen(a) {
		t.Fatal("opening C must evict A when HasOpen did not refresh A")
	}
	if !e.HasOpen(b) || !e.HasOpen(c) {
		t.Fatal("B and C must remain resident")
	}
}
