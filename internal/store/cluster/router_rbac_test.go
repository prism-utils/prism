package cluster_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/elk-utilities/prism/internal/store/authtest"
	"github.com/elk-utilities/prism/internal/store/authz"
	"github.com/elk-utilities/prism/internal/store/cluster"
)

func rbacQueryGuard(t *testing.T, policyPath string, env *authtest.JWTEnv) func(http.Handler) http.Handler {
	t.Helper()
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: policyPath, ReloadSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	v := env.Verifier(t)
	mw := authz.NewMiddleware(v, a, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mw.WrapQuery
}

func writeClusterPolicy(t *testing.T) (string, *authtest.JWTEnv) {
	t.Helper()
	env := authtest.NewJWTEnv(t, "prism-store")
	body := `bindings:
  - subject: "reader-a"
    role: reader
    tenants: ["` + validTenantA + `"]
`
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, env
}

func TestRouterRBACDenyBeforeProxy(t *testing.T) {
	var hits atomic.Int32
	up := fakeUpstream(t, "A", &hits)
	t.Cleanup(up.Close)

	policy, env := writeClusterPolicy(t)
	clients, err := cluster.ParseClients(validTenantA + "=" + up.URL + "," + validTenantB + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}
	mux := cluster.NewServeMux(clients, "", rbacQueryGuard(t, policy, env))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	path := "/" + validTenantB + "/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	if hits.Load() != 0 {
		t.Fatalf("upstream hits = %d, want 0", hits.Load())
	}
}

func TestRouterRBACForwardsJWT(t *testing.T) {
	var hits atomic.Int32
	up := fakeUpstream(t, "A", &hits)
	t.Cleanup(up.Close)

	policy, env := writeClusterPolicy(t)
	clients, err := cluster.ParseClients(validTenantA + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}
	mux := cluster.NewServeMux(clients, "", rbacQueryGuard(t, policy, env))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	path := "/" + validTenantA + "/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Got-Auth"); got != "Bearer "+tok {
		t.Fatalf("auth forwarded = %q", got)
	}
}
