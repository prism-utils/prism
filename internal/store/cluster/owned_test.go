package cluster_test

import (
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/store/cluster"
)

func TestParseOwnedTenantsValid(t *testing.T) {
	owned, err := cluster.ParseOwnedTenants(validTenantA + "," + validTenantB)
	if err != nil {
		t.Fatal(err)
	}
	if len(owned) != 2 {
		t.Fatalf("len = %d, want 2", len(owned))
	}
	if _, ok := owned[validTenantA]; !ok {
		t.Fatal("missing tenant A")
	}
	if _, ok := owned[validTenantB]; !ok {
		t.Fatal("missing tenant B")
	}
}

func TestParseOwnedTenantsTrimsWhitespace(t *testing.T) {
	owned, err := cluster.ParseOwnedTenants("  " + validTenantA + " , " + validTenantB + "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(owned) != 2 {
		t.Fatalf("len = %d, want 2", len(owned))
	}
}

func TestParseOwnedTenantsEmpty(t *testing.T) {
	_, err := cluster.ParseOwnedTenants("")
	if err == nil {
		t.Fatal("expected error for empty CLIENT_TENANTS")
	}
	if !strings.Contains(err.Error(), "CLIENT_TENANTS") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseOwnedTenantsInvalidTenant(t *testing.T) {
	_, err := cluster.ParseOwnedTenants("bad tenant!")
	if err == nil {
		t.Fatal("expected error for invalid tenant")
	}
}

func TestParseOwnedTenantsDuplicateIgnored(t *testing.T) {
	owned, err := cluster.ParseOwnedTenants(validTenantA + "," + validTenantA)
	if err != nil {
		t.Fatal(err)
	}
	if len(owned) != 1 {
		t.Fatalf("len = %d, want 1", len(owned))
	}
}
