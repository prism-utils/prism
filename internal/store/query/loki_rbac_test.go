package query_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/prism-utils/prism/internal/store/authtest"
	"github.com/prism-utils/prism/internal/store/authz"
	"github.com/prism-utils/prism/internal/store/cluster"
	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

// lokiRBACFixture seeds logs for tenantSQLA and tenantSQLB and serves the Loki
// API behind the RBAC `query` action, mirroring how the store wires it.
func lokiRBACFixture(t *testing.T) (*httptest.Server, *authtest.JWTEnv) {
	t.Helper()
	dataDir := t.TempDir()
	for _, ns := range []string{tenantSQLA, tenantSQLB} {
		landLokiRaw(t, dataDir, ns, "raw.parquet", lokiBase, []testparquet.LogRow{
			{Message: "line for " + ns, Format: "none"},
		})
	}
	env := authtest.NewJWTEnv(t, "prism-store")
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(rbacPolicySQL()), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: path, ReloadSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mw := authz.NewMiddleware(env.Verifier(t), a, logger)
	h := mw.WrapQuery(query.LokiHandler(lokiConfig(dataDir), logger))
	mux := http.NewServeMux()
	for _, p := range query.LokiRoutePatterns("") {
		mux.Handle(p, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, env
}

func lokiGetAuth(t *testing.T, u, token string) (int, lokiEnvelope) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return lokiDo(t, req)
}

func TestLokiRBACReaderAllowed(t *testing.T) {
	srv, env := lokiRBACFixture(t)
	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	status, resp := lokiGetAuth(t, lokiRangeURL(srv.URL, tenantSQLA, `{job="prism"}`), tok)
	if status != http.StatusOK || resp.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, resp)
	}
	if got := lokiLines(t, resp); len(got) != 1 || got[0] != "line for "+tenantSQLA {
		t.Fatalf("lines = %v", got)
	}
}

// TestLokiRBACWriter403 proves the Loki surface requires the read action, not
// merely a valid token.
func TestLokiRBACWriter403(t *testing.T) {
	srv, env := lokiRBACFixture(t)
	tok, err := env.SignToken(authtest.WithSubject("writer-a"))
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := lokiGetAuth(t, lokiRangeURL(srv.URL, tenantSQLA, ""), tok); status != http.StatusForbidden {
		t.Fatalf("status=%d, want 403", status)
	}
}

// TestLokiRBACCrossTenantDenied proves a reader bound to one tenant cannot read
// another tenant's logs, and never reaches the sandbox.
func TestLokiRBACCrossTenantDenied(t *testing.T) {
	srv, env := lokiRBACFixture(t)
	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/" + tenantSQLB + "/loki/api/v1/query_range",
		"/" + tenantSQLB + "/loki/api/v1/labels",
		"/" + tenantSQLB + "/loki/api/v1/label/format/values",
	} {
		if status, _ := lokiGetAuth(t, srv.URL+path, tok); status != http.StatusNotFound {
			t.Fatalf("%s status=%d, want 404 for an unbound tenant", path, status)
		}
	}
}

func TestLokiRBACMissingOrInvalidJWT401(t *testing.T) {
	srv, _ := lokiRBACFixture(t)
	for _, tok := range []string{"", "not.a.jwt"} {
		if status, _ := lokiGetAuth(t, lokiRangeURL(srv.URL, tenantSQLA, ""), tok); status != http.StatusUnauthorized {
			t.Fatalf("token %q status=%d, want 401", tok, status)
		}
	}
}

// TestLokiOwnedTenantGuard proves a cluster client rejects a tenant it does not
// own before the Loki handler opens a sandbox.
func TestLokiOwnedTenantGuard(t *testing.T) {
	dataDir := t.TempDir()
	landLokiRaw(t, dataDir, lokiTenant, "raw.parquet", lokiBase, []testparquet.LogRow{
		{Message: "owned", Format: "none"},
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	owned := map[string]struct{}{lokiTenant: {}}
	h := cluster.OwnedTenantGuard(owned, query.LokiHandler(lokiConfig(dataDir), logger))
	mux := http.NewServeMux()
	for _, p := range query.LokiRoutePatterns("") {
		mux.Handle(p, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	if status, _ := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, "")); status != http.StatusOK {
		t.Fatalf("owned tenant status=%d, want 200", status)
	}
	if status, _ := lokiGet(t, lokiRangeURL(srv.URL, "user-loki-unowned", "")); status != http.StatusNotFound {
		t.Fatal("unowned tenant must be 404")
	}
}
