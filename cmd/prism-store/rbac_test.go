package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/admin"
	"github.com/elk-utilities/prism/internal/store/authtest"
	"github.com/elk-utilities/prism/internal/store/engine"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
)

func TestValidateRBACFlightRejectAuthNone(t *testing.T) {
	err := validateRBACFlight(&serverConfig{
		flightAddr: ":9090",
		authMode:   "none",
	}, &rbacStack{})
	if err == nil {
		t.Fatal("expected fail-fast when RBAC on, Flight enabled, AUTH_MODE=none")
	}
	if !strings.Contains(err.Error(), "AUTH_MODE") {
		t.Fatalf("err = %v, want AUTH_MODE guidance", err)
	}
}

func TestValidateRBACFlightAllowsBearerWithRBAC(t *testing.T) {
	if err := validateRBACFlight(&serverConfig{
		flightAddr:  ":9090",
		authMode:    "bearer",
		ingestToken: "secret",
	}, &rbacStack{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTPIngestAuthModeNoneWhenRBACOn(t *testing.T) {
	mode, err := httpIngestAuthMode(&serverConfig{authMode: "bearer"}, &rbacStack{})
	if err != nil {
		t.Fatal(err)
	}
	if mode != storeingest.AuthNone {
		t.Fatalf("http ingest mode = %q, want none under rbac middleware", mode)
	}
}

func TestFlightIngestAuthModeKeepsOperatorConfigWithRBAC(t *testing.T) {
	cfg := &serverConfig{authMode: "bearer", ingestToken: "secret"}
	mode, err := flightIngestAuthMode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if mode != storeingest.AuthBearer {
		t.Fatalf("flight mode = %q, want bearer", mode)
	}
}

func TestRBACHTTPIngestViaJWT(t *testing.T) {
	dir := t.TempDir()
	env := authtest.NewJWTEnv(t, "prism-store")
	policyBody := `bindings:
  - subject: "writer-a"
    role: writer
    tenants: ["user-6f3a9c2b-apps"]
`
	policyPath := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyPath, []byte(policyBody), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testServeConfig(dir, "127.0.0.1:0", "")
	cfg.rbac = &rbacConfig{
		policyFile:    policyPath,
		issuer:        env.Issuer,
		jwksFile:      env.JWKSPath,
		audience:      []string{"prism-store"},
		reloadSeconds: 0,
	}
	stack, err := buildRBACStack(context.Background(), cfg.rbac, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stack.close)
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := newServeMux(cfg, eng, logger, planeCombined, nil, stack, nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	tok, err := env.SignToken(authtest.WithSubject("writer-a"))
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/user-6f3a9c2b-apps/ingest/metrics-raw", strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusUnauthorized {
		if resp.StatusCode == http.StatusUnauthorized {
			t.Fatalf("ingest rejected valid jwt: %d", resp.StatusCode)
		}
	}
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatal("ingest rejected valid JWT under RBAC")
	}

	req2, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/user-6f3a9c2b-apps/ingest/metrics-raw", strings.NewReader(""))
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated ingest = %d, want 401", resp2.StatusCode)
	}
}

func TestRBACOffRoutingUnchanged(t *testing.T) {
	dir := t.TempDir()
	cfg := testServeConfig(dir, "127.0.0.1:0", "")
	cfg.adminToken = "static-admin"
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	mux := newServeMux(cfg, eng, logger, planeCombined, nil, nil, nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("rbac-off stats with bad token = %d, want 401", resp.StatusCode)
	}

	req2, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats", nil)
	req2.Header.Set("Authorization", "Bearer static-admin")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("rbac-off stats with admin token = %d, want 200", resp2.StatusCode)
	}
}

func TestRBACEnabledFailFastMissingOIDC(t *testing.T) {
	policy := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policy, []byte(`bindings: [{subject: x, role: reader, tenants: ["user-6f3a9c2b-apps"]}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &rbacConfig{policyFile: policy}
	_, err := buildRBACStack(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("expected fail-fast without oidc config")
	}
}

func TestRBACStatsScoping(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "user-6f3a9c2b-apps"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "user-7a4b1c9d-web"), 0o750); err != nil {
		t.Fatal(err)
	}

	env := authtest.NewJWTEnv(t, "prism-store")
	policyBody := `bindings:
  - subject: "scoped-admin"
    role: admin
    tenants: ["user-6f3a9c2b-apps"]
`
	policyPath := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyPath, []byte(policyBody), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := testServeConfig(dir, "127.0.0.1:0", "")
	cfg.rbac = &rbacConfig{
		policyFile:    policyPath,
		issuer:        env.Issuer,
		jwksFile:      env.JWKSPath,
		audience:      []string{"prism-store"},
		reloadSeconds: 0,
	}
	stack, err := buildRBACStack(context.Background(), cfg.rbac, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stack.close)
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	adminCfg := cfg.adminConfig()
	statsHandler := stack.wrapStats(admin.StatsHandler(adminCfg, eng))
	srv := httptest.NewServer(statsHandler)
	t.Cleanup(srv.Close)

	tok, err := env.SignToken(authtest.WithSubject("scoped-admin"))
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "user-7a4b1c9d-web") {
		t.Fatalf("stats leaked other tenant metadata: %s", body)
	}
}

func TestRBACClientReEnforcesUnauthorizedTenant(t *testing.T) {
	dir := t.TempDir()
	env := authtest.NewJWTEnv(t, "prism-store")
	policyBody := `bindings:
  - subject: "reader-a"
    role: reader
    tenants: ["user-6f3a9c2b-apps"]
`
	policyPath := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyPath, []byte(policyBody), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testServeConfig(dir, "127.0.0.1:0", "")
	cfg.rbac = &rbacConfig{
		policyFile:    policyPath,
		issuer:        env.Issuer,
		jwksFile:      env.JWKSPath,
		audience:      []string{"prism-store"},
		reloadSeconds: 0,
	}
	stack, err := buildRBACStack(context.Background(), cfg.rbac, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stack.close)
	eng := engine.New(engine.Config{DataDir: dir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	owned := map[string]struct{}{"user-6f3a9c2b-apps": {}}
	mux := newServeMux(cfg, eng, logger, planeCombined, owned, stack, nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	tok, _ := env.SignToken(authtest.WithSubject("reader-a"))
	path := "/user-7a4b1c9d-web/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z"
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("direct client unauthorized tenant = %d, want 404", resp.StatusCode)
	}
}
