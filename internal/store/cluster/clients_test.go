package cluster_test

import (
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/store/cluster"
)

const validTenantA = "user-6f3a9c2b-apps"
const validTenantB = "user-7a4b1c9d-web"

func TestParseClientsValid(t *testing.T) {
	env := validTenantA + "=http://host1:8080," + validTenantB + "=http://host2:9090"
	clients, err := cluster.ParseClients(env)
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != 2 {
		t.Fatalf("len = %d, want 2", len(clients))
	}
	if clients[validTenantA].Host != "host1:8080" {
		t.Fatalf("tenant A host = %q", clients[validTenantA].Host)
	}
	if clients[validTenantB].Host != "host2:9090" {
		t.Fatalf("tenant B host = %q", clients[validTenantB].Host)
	}
}

func TestParseClientsTwoTenantsSameURL(t *testing.T) {
	env := validTenantA + "=http://shared:8080," + validTenantB + "=http://shared:8080"
	clients, err := cluster.ParseClients(env)
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != 2 {
		t.Fatalf("len = %d, want 2", len(clients))
	}
}

func TestParseClientsEmptyEnv(t *testing.T) {
	_, err := cluster.ParseClients("")
	if err == nil {
		t.Fatal("expected error for empty CLUSTER_CLIENTS")
	}
	if !strings.Contains(err.Error(), "CLUSTER_CLIENTS") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseClientsMalformedEntry(t *testing.T) {
	_, err := cluster.ParseClients(validTenantA + "=http://h:8080,nobequalsign")
	if err == nil {
		t.Fatal("expected error for entry without =")
	}
}

func TestParseClientsMalformedURL(t *testing.T) {
	_, err := cluster.ParseClients(validTenantA + "=%zz://bad")
	if err == nil {
		t.Fatal("expected error for malformed URL")
	}
}

func TestParseClientsRelativeURL(t *testing.T) {
	_, err := cluster.ParseClients(validTenantA + "=/relative/path")
	if err == nil {
		t.Fatal("expected error for relative URL")
	}
}

func TestParseClientsNonHTTPScheme(t *testing.T) {
	_, err := cluster.ParseClients(validTenantA + "=ftp://host:21")
	if err == nil {
		t.Fatal("expected error for non-http(s) URL")
	}
}

func TestParseClientsEmptyTenant(t *testing.T) {
	_, err := cluster.ParseClients("=http://host:8080")
	if err == nil {
		t.Fatal("expected error for empty tenant")
	}
}

func TestParseClientsInvalidTenant(t *testing.T) {
	_, err := cluster.ParseClients("INVALID!=http://host:8080")
	if err == nil {
		t.Fatal("expected error for invalid tenant name")
	}
}

func TestParseClientsDuplicateTenant(t *testing.T) {
	env := validTenantA + "=http://h1:8080," + validTenantA + "=http://h2:8080"
	_, err := cluster.ParseClients(env)
	if err == nil {
		t.Fatal("expected error for duplicate tenant")
	}
	if !strings.Contains(err.Error(), validTenantA) {
		t.Fatalf("err = %v", err)
	}
}
