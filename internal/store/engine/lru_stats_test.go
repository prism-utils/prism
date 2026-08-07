package engine

import (
	"testing"
	"time"
)

func TestOpenTenantsTracksResidentHandles(t *testing.T) {
	e := New(Config{DataDir: t.TempDir(), HotWindow: time.Hour, MaxOpenTenants: 4}, time.Now)
	defer func() { _ = e.Close() }()

	if got := e.OpenTenants(); got != 0 {
		t.Fatalf("OpenTenants before use = %d, want 0", got)
	}
	if got := e.MaxOpenTenants(); got != 4 {
		t.Fatalf("MaxOpenTenants = %d, want 4", got)
	}

	for _, ns := range []string{"user-6f3a9c2b-apps", "user-7a4b1c9d-web"} {
		if _, err := e.DB(ns); err != nil {
			t.Fatalf("open %s: %v", ns, err)
		}
	}
	if got := e.OpenTenants(); got != 2 {
		t.Fatalf("OpenTenants = %d, want 2", got)
	}
}

func TestEvictedTenantsTotalCountsCapacityEvictionsOnly(t *testing.T) {
	e := New(Config{DataDir: t.TempDir(), HotWindow: time.Hour, MaxOpenTenants: 1}, time.Now)

	for _, ns := range []string{"user-6f3a9c2b-apps", "user-7a4b1c9d-web", "user-8b5c2d0e-db"} {
		if _, err := e.DB(ns); err != nil {
			t.Fatalf("open %s: %v", ns, err)
		}
	}
	if got := e.EvictedTenantsTotal(); got != 2 {
		t.Fatalf("EvictedTenantsTotal = %d, want 2", got)
	}
	if got := e.OpenTenants(); got != 1 {
		t.Fatalf("OpenTenants = %d, want the LRU cap of 1", got)
	}

	// Shutdown closes every handle, but a planned close is not saturation and
	// must not inflate the eviction counter operators alert on.
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := e.EvictedTenantsTotal(); got != 2 {
		t.Fatalf("EvictedTenantsTotal after Close = %d, want 2", got)
	}
}

func TestMaxOpenTenantsReportsAppliedDefault(t *testing.T) {
	e := New(Config{DataDir: t.TempDir(), HotWindow: time.Hour}, time.Now)
	defer func() { _ = e.Close() }()

	if got := e.MaxOpenTenants(); got != defaultLRUSize {
		t.Fatalf("MaxOpenTenants = %d, want the applied default %d", got, defaultLRUSize)
	}
}
