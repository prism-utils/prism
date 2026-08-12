package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/admin"
	"github.com/prism-utils/prism/internal/store/authtest"
	"github.com/prism-utils/prism/internal/store/engine"
	storeingest "github.com/prism-utils/prism/internal/store/ingest"
	"github.com/prism-utils/prism/internal/store/queue"
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

// TestRBACQueueSnapshotScoping pins the RBAC wall on the live queue snapshot.
// The snapshot is process-wide but is gated by the tenant-scoped `stats` action,
// so two things need pinning: a principal with no `stats` binding is refused,
// and naming a tenant outside the principal's `stats` scope hides the snapshot
// even though it carries no tenant data. The `ns` answer is the anti-enumeration
// `404`, never a `200` — the route is strictly more restrictive with `ns` than
// without it.
func TestRBACQueueSnapshotScoping(t *testing.T) {
	dir := t.TempDir()
	env := authtest.NewJWTEnv(t, "prism-store")
	policyBody := `bindings:
  - subject: "queue-admin"
    role: admin
    tenants: ["user-6f3a9c2b-apps"]
  - subject: "queue-reader"
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
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 2, MaxQueue: 128, Wait: 120 * time.Second})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(newServeMux(cfg, eng, logger, planeCombined, nil, stack, lim))
	t.Cleanup(srv.Close)

	statsTok, err := env.SignToken(authtest.WithSubject("queue-admin"))
	if err != nil {
		t.Fatal(err)
	}
	noStatsTok, err := env.SignToken(authtest.WithSubject("queue-reader"))
	if err != nil {
		t.Fatal(err)
	}

	get := func(t *testing.T, path, token string) (int, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(body)
	}

	for _, tc := range []struct {
		name     string
		path     string
		token    string
		wantCode int
		wantBody string
	}{
		{"stats binding is admitted", "/admin/queue", statsTok, http.StatusOK, ""},
		{"no stats binding is forbidden", "/admin/queue", noStatsTok, http.StatusForbidden, "forbidden"},
		{"ns outside stats scope is hidden", "/admin/queue?ns=user-7a4b1c9d-web", statsTok, http.StatusNotFound, "unknown tenant"},
		{"unauthenticated is rejected", "/admin/queue", "", http.StatusUnauthorized, "unauthorized"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := get(t, tc.path, tc.token)
			if code != tc.wantCode {
				t.Fatalf("GET %s = %d, want %d (body %q)", tc.path, code, tc.wantCode, body)
			}
			if tc.wantBody != "" && !strings.Contains(body, tc.wantBody) {
				t.Fatalf("GET %s body = %q, want it to contain %q", tc.path, body, tc.wantBody)
			}
		})
	}

	t.Run("admitted principal reads the node caps", func(t *testing.T) {
		code, body := get(t, "/admin/queue", statsTok)
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %q)", code, body)
		}
		var got queue.Snapshot
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatalf("decode: %v body=%s", err, body)
		}
		want := queue.Snapshot{Enabled: true, MaxInFlight: 2, MaxQueue: 128, TimeoutMs: 120000}
		if got != want {
			t.Fatalf("snapshot = %+v, want %+v", got, want)
		}
	})
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
