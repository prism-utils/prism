package authz_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/elk-utilities/prism/internal/store/authtest"
	"github.com/elk-utilities/prism/internal/store/authz"
)

func testStack(t *testing.T, policy string) (*authz.Middleware, *authtest.JWTEnv) {
	t.Helper()
	env := authtest.NewJWTEnv(t, "prism-store")
	path := writePolicy(t, policy)
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: path, ReloadSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	v := env.Verifier(t)
	mw := authz.NewMiddleware(v, a, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mw, env
}

func bearerRequest(method, url, token string) *http.Request {
	req, _ := http.NewRequestWithContext(context.Background(), method, url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func TestMiddleware401MissingToken(t *testing.T) {
	mw, _ := testStack(t, samplePolicy())
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	rec := httptest.NewRecorder()
	mw.WrapQuery(next).ServeHTTP(rec, bearerRequest(http.MethodGet, "/"+tenantA+"/query", ""))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestMiddleware404CrossTenant(t *testing.T) {
	mw, env := testStack(t, samplePolicy())
	tok, err := env.SignToken(authtest.WithSubject("alice@corp"))
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	rec := httptest.NewRecorder()
	req := bearerRequest(http.MethodGet, "/"+tenantB+"/query", tok)
	req.SetPathValue("ns", tenantB)
	mw.WrapQuery(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unknown tenant") {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestMiddleware403ForbiddenAction(t *testing.T) {
	mw, env := testStack(t, samplePolicy())
	tok, err := env.SignToken(authtest.WithSubject("alice@corp"))
	if err != nil {
		t.Fatal(err)
	}
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true })
	rec := httptest.NewRecorder()
	req := bearerRequest(http.MethodPost, "/admin/tenants/"+tenantA+"/ensure", tok)
	req.SetPathValue("ns", tenantA)
	mw.WrapEnsure(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
	if called {
		t.Fatal("handler invoked on forbidden")
	}
}

func TestMiddlewareIgnoresIdentityHeaders(t *testing.T) {
	mw, env := testStack(t, samplePolicy())
	tok, err := env.SignToken(authtest.WithSubject("alice@corp"))
	if err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	rec := httptest.NewRecorder()
	req := bearerRequest(http.MethodGet, "/"+tenantB+"/query", tok)
	req.SetPathValue("ns", tenantB)
	req.Header.Set("X-User", "admin@corp")
	req.Header.Set("X-Tenant", tenantB)
	mw.WrapQuery(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 despite spoof headers", rec.Code)
	}
}

func TestMiddlewareStatsScoped403NonAdmin(t *testing.T) {
	mw, env := testStack(t, samplePolicy())
	tok, err := env.SignToken(authtest.WithSubject("writer@corp"))
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := bearerRequest(http.MethodGet, "/stats", tok)
	mw.WrapStats(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, ok := authz.StatsScopeFromContext(r.Context())
		if ok {
			t.Fatal("stats scope should not be set for forbidden aggregate")
		}
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestMiddlewareStatsNsRequiresStatsOnTenant(t *testing.T) {
	mw, env := testStack(t, samplePolicy())
	tok, err := env.SignToken(authtest.WithSubject("alice@corp"))
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := bearerRequest(http.MethodGet, "/stats?ns="+tenantA, tok)
	mw.WrapStats(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("reader stats?ns status = %d, want 403", rec.Code)
	}
}

func TestMiddlewareAllowQuery(t *testing.T) {
	mw, env := testStack(t, samplePolicy())
	tok, err := env.SignToken(authtest.WithSubject("alice@corp"))
	if err != nil {
		t.Fatal(err)
	}
	called := false
	rec := httptest.NewRecorder()
	req := bearerRequest(http.MethodGet, "/"+tenantA+"/query", tok)
	req.SetPathValue("ns", tenantA)
	mw.WrapQuery(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !called {
		t.Fatalf("status=%d called=%v", rec.Code, called)
	}
}

func TestBOLAIsolationQueryIngestStats(t *testing.T) {
	policy := `
bindings:
  - subject: "user-a"
    role: admin
    tenants: ["` + tenantA + `"]
`
	mw, env := testStack(t, policy)
	tok, _ := env.SignToken(authtest.WithSubject("user-a"))
	routes := []struct {
		name string
		fn   func(http.Handler) http.Handler
		req  *http.Request
	}{
		{"query", mw.WrapQuery, bearerRequest(http.MethodGet, "/"+tenantB+"/query", tok)},
		{"ingest", mw.WrapIngest, bearerRequest(http.MethodPost, "/"+tenantB+"/ingest/metrics-raw", tok)},
		{"stats", mw.WrapStats, bearerRequest(http.MethodGet, "/stats?ns="+tenantB, tok)},
	}
	for _, rt := range routes {
		t.Run(rt.name, func(t *testing.T) {
			rt.req.SetPathValue("ns", tenantB)
			rec := httptest.NewRecorder()
			rt.fn(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Fatal("must not reach handler")
			})).ServeHTTP(rec, rt.req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d", rec.Code)
			}
		})
	}
}

func TestMiddlewareInvalidToken401(t *testing.T) {
	mw, _ := testStack(t, samplePolicy())
	rec := httptest.NewRecorder()
	req := bearerRequest(http.MethodGet, "/"+tenantA+"/query", "not.valid.jwt")
	req.SetPathValue("ns", tenantA)
	mw.WrapQuery(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {})).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
}
