package cluster_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/elk-utilities/prism/internal/store/cluster"
	"github.com/elk-utilities/prism/internal/store/query"
)

func TestOwnedTenantGuardAllowsOwnedTenant(t *testing.T) {
	var engineHits atomic.Int32
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		engineHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	owned, err := cluster.ParseOwnedTenants(validTenantA)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle(query.QueryRoutePattern(""), cluster.OwnedTenantGuard(owned, inner))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/"+validTenantA+"/query", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if engineHits.Load() != 1 {
		t.Fatalf("engine hits = %d, want 1", engineHits.Load())
	}
}

func TestOwnedTenantGuardRejectsNonOwnedBeforeEngine(t *testing.T) {
	var engineHits atomic.Int32
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		engineHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	owned, err := cluster.ParseOwnedTenants(validTenantA)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle(query.QueryRoutePattern(""), cluster.OwnedTenantGuard(owned, inner))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/"+validTenantB+"/query", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if engineHits.Load() != 0 {
		t.Fatalf("engine hits = %d, want 0 (guard must reject before engine)", engineHits.Load())
	}
}

func TestOwnedTenantGuardInvalidTenant404(t *testing.T) {
	var engineHits atomic.Int32
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		engineHits.Add(1)
	})

	owned, err := cluster.ParseOwnedTenants(validTenantA)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle(query.QueryRoutePattern(""), cluster.OwnedTenantGuard(owned, inner))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/INVALID!/query", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if engineHits.Load() != 0 {
		t.Fatal("engine must not run for invalid tenant")
	}
}
