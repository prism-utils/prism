package authz_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/store/authz"
)

const tenantA = "user-6f3a9c2b-apps"
const tenantB = "user-7a4b1c9d-web"

func writePolicy(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func samplePolicy() string {
	return `
bindings:
  - subject: "alice@corp"
    role: reader
    tenants: ["` + tenantA + `"]
  - subject: "writer@corp"
    role: writer
    tenants: ["` + tenantA + `"]
  - subject: "admin@corp"
    role: admin
    tenants: ["*"]
  - subject: "scoped-admin@corp"
    role: admin
    tenants: ["` + tenantA + `"]
`
}

func TestPolicyStartupFailsOnBadInitial(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"unknown role", `bindings: [{subject: x, role: superuser, tenants: ["` + tenantA + `"]}]`},
		{"empty subject", `bindings: [{subject: "  ", role: reader, tenants: ["` + tenantA + `"]}]`},
		{"bad tenant", `bindings: [{subject: x, role: reader, tenants: ["INVALID!"]}]`},
		{"empty bindings", `bindings: []`},
		{"contradictory dup", `bindings: [{subject: x, role: reader, tenants: ["` + tenantA + `"]}, {subject: x, role: writer, tenants: ["` + tenantA + `"]}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writePolicy(t, tc.body)
			_, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: path, ReloadSeconds: 0})
			if err == nil {
				t.Fatal("expected startup error")
			}
		})
	}
}

func TestAuthorizePermissionMatrix(t *testing.T) {
	path := writePolicy(t, samplePolicy())
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: path, ReloadSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}

	type row struct {
		principal string
		action    authz.Action
		tenant    string
		want      authz.Decision
	}
	rows := []row{
		{"alice@corp", authz.ActionQuery, tenantA, authz.DecisionAllow},
		{"alice@corp", authz.ActionIngest, tenantA, authz.DecisionDenyForbidden},
		{"alice@corp", authz.ActionQuery, tenantB, authz.DecisionDenyNotFound},
		{"writer@corp", authz.ActionIngest, tenantA, authz.DecisionAllow},
		{"writer@corp", authz.ActionQuery, tenantA, authz.DecisionDenyForbidden},
		{"admin@corp", authz.ActionStats, tenantB, authz.DecisionAllow},
		{"admin@corp", authz.ActionEnsure, tenantA, authz.DecisionAllow},
		{"scoped-admin@corp", authz.ActionStats, tenantA, authz.DecisionAllow},
		{"scoped-admin@corp", authz.ActionStats, tenantB, authz.DecisionDenyNotFound},
		{"unknown@corp", authz.ActionQuery, tenantA, authz.DecisionDenyNotFound},
	}
	for _, r := range rows {
		got := a.Authorize(r.principal, r.action, r.tenant)
		if got != r.want {
			t.Errorf("%s %s %s = %v, want %v", r.principal, r.action, r.tenant, got, r.want)
		}
	}
}

func TestAuthorizedTenantsStats(t *testing.T) {
	path := writePolicy(t, samplePolicy())
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: path, ReloadSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	all := a.AuthorizedTenants("admin@corp", authz.ActionStats)
	if !all.All {
		t.Fatalf("admin scope = %+v, want All", all)
	}
	scoped := a.AuthorizedTenants("scoped-admin@corp", authz.ActionStats)
	if scoped.All || len(scoped.Tenants) != 1 || scoped.Tenants[0] != tenantA {
		t.Fatalf("scoped admin = %+v", scoped)
	}
	none := a.AuthorizedTenants("alice@corp", authz.ActionStats)
	if none.All || len(none.Tenants) != 0 {
		t.Fatalf("reader stats scope = %+v", none)
	}
}

func TestReloadKeepsOldOnBadEdit(t *testing.T) {
	path := writePolicy(t, samplePolicy())
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: path, ReloadSeconds: 3600})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	if got := a.Authorize("alice@corp", authz.ActionQuery, tenantA); got != authz.DecisionAllow {
		t.Fatalf("before edit = %v", got)
	}
	if err := os.WriteFile(path, []byte("bindings: [{subject: x, role: bad, tenants: ['a']}]"), 0o600); err != nil {
		t.Fatal(err)
	}
	a.ReloadNow()
	if got := a.Authorize("alice@corp", authz.ActionQuery, tenantA); got != authz.DecisionAllow {
		t.Fatalf("after bad reload = %v, want prior policy", got)
	}
}

func TestReloadAppliesValidEdit(t *testing.T) {
	path := writePolicy(t, samplePolicy())
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: path, ReloadSeconds: 3600})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	newBody := strings.Replace(samplePolicy(), "reader", "writer", 1)
	if err := os.WriteFile(path, []byte(newBody), 0o600); err != nil {
		t.Fatal(err)
	}
	a.ReloadNow()
	if got := a.Authorize("alice@corp", authz.ActionIngest, tenantA); got != authz.DecisionAllow {
		t.Fatalf("after reload ingest = %v", got)
	}
}
