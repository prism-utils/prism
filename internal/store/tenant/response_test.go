package tenant_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/store/authtest"
	"github.com/elk-utilities/prism/internal/store/authz"
	"github.com/elk-utilities/prism/internal/store/cluster"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/query"
	"github.com/elk-utilities/prism/internal/store/tenant"
)

func TestUnknownTenantBodyByteIdenticalAcrossHandlers(t *testing.T) {
	want := tenant.UnknownTenantBody

	recRouter := httptest.NewRecorder()
	cluster.NewRouter(map[string]*url.URL{}).ServeHTTP(recRouter, reqWithNS("GET", "/not-valid!/query", "not-valid!"))
	assert404Body(t, recRouter, want, "cluster router invalid tenant")

	recGuard := httptest.NewRecorder()
	cluster.OwnedTenantGuard(map[string]struct{}{"user-6f3a9c2b-apps": {}}, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).
		ServeHTTP(recGuard, reqWithNS("GET", "/user-7a4b1c9d-web/query", "user-7a4b1c9d-web"))
	assert404Body(t, recGuard, want, "owned guard non-owned tenant")

	env := authtest.NewJWTEnv(t, "prism-store")
	policy := writePolicyFile(t, `bindings:
  - subject: "alice"
    role: reader
    tenants: ["user-6f3a9c2b-apps"]
`)
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: policy, ReloadSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	mw := authz.NewMiddleware(env.Verifier(t), a, slog.New(slog.NewTextHandler(io.Discard, nil)))
	tok, _ := env.SignToken(authtest.WithSubject("alice"))
	recAuthz := httptest.NewRecorder()
	req := reqWithNS("GET", "/user-7a4b1c9d-web/query", "user-7a4b1c9d-web")
	req.Header.Set("Authorization", "Bearer "+tok)
	mw.WrapQuery(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(recAuthz, req)
	assert404Body(t, recAuthz, want, "authz cross-tenant deny")

	recIngest := httptest.NewRecorder()
	reqIngest := reqWithNS("POST", "/not-valid!/ingest/metrics-raw", "not-valid!")
	storeingest.Handler(&storeingest.Config{AllowedArtifacts: []string{"metrics-raw"}, AuthMode: storeingest.AuthNone}, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).
		ServeHTTP(recIngest, reqIngest)
	assert404Body(t, recIngest, want, "ingest handler invalid tenant")

	recQuery := httptest.NewRecorder()
	query.Handler(&query.Config{DataDir: t.TempDir()}, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).
		ServeHTTP(recQuery, reqWithNS("GET", "/not-valid!/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z", "not-valid!"))
	assert404Body(t, recQuery, want, "query handler invalid tenant")
}

func assert404Body(t *testing.T, rec *httptest.ResponseRecorder, want, label string) {
	t.Helper()
	if rec.Code != http.StatusNotFound {
		t.Fatalf("%s: status = %d, want 404", label, rec.Code)
	}
	body := strings.TrimSuffix(rec.Body.String(), "\n")
	if body != want {
		t.Fatalf("%s: body = %q, want %q (byte-identical)", label, body, want)
	}
}

func reqWithNS(method, target, ns string) *http.Request {
	req := httptest.NewRequestWithContext(context.Background(), method, "http://example.com"+target, nil)
	req.SetPathValue("ns", ns)
	return req
}

func writePolicyFile(t *testing.T, body string) string {
	t.Helper()
	path := t.TempDir() + "/policy.yaml"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
