package cluster_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/cluster"
)

func fakeUpstream(t *testing.T, label string, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if auth := r.Header.Get("Authorization"); auth != "" {
			w.Header().Set("X-Got-Auth", auth)
		}
		w.Header().Set("X-Upstream", label)
		w.Header().Set("X-Path", r.URL.Path)
		w.Header().Set("X-Query", r.URL.RawQuery)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "body-"+label)
	}))
}

func TestRouterRoutesTenantToOwningClient(t *testing.T) {
	var hitsA, hitsB atomic.Int32
	upA := fakeUpstream(t, "A", &hitsA)
	t.Cleanup(upA.Close)
	upB := fakeUpstream(t, "B", &hitsB)
	t.Cleanup(upB.Close)

	clients, err := cluster.ParseClients(
		validTenantA + "=" + upA.URL + "," + validTenantB + "=" + upB.URL,
	)
	if err != nil {
		t.Fatal(err)
	}

	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	start := "2024-01-01T00:00:00Z"
	end := "2024-01-02T00:00:00Z"
	path := "/" + validTenantA + "/query?start=" + start + "&end=" + end

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer cluster-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("X-Upstream"); got != "A" {
		t.Fatalf("upstream = %q, want A", got)
	}
	if got := resp.Header.Get("X-Got-Auth"); got != "Bearer cluster-token" {
		t.Fatalf("auth = %q", got)
	}
	if got := resp.Header.Get("X-Path"); got != "/"+validTenantA+"/query" {
		t.Fatalf("path = %q", got)
	}
	if got := resp.Header.Get("X-Query"); got != "start="+start+"&end="+end {
		t.Fatalf("query = %q", got)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "body-A" {
		t.Fatalf("body = %q", body)
	}
	if hitsA.Load() != 1 {
		t.Fatalf("upstream A hits = %d, want 1", hitsA.Load())
	}
	if hitsB.Load() != 0 {
		t.Fatalf("upstream B hits = %d, want 0", hitsB.Load())
	}
}

func TestRouterTenantBIsolation(t *testing.T) {
	var hitsA, hitsB atomic.Int32
	upA := fakeUpstream(t, "A", &hitsA)
	t.Cleanup(upA.Close)
	upB := fakeUpstream(t, "B", &hitsB)
	t.Cleanup(upB.Close)

	clients, err := cluster.ParseClients(
		validTenantA + "=" + upA.URL + "," + validTenantB + "=" + upB.URL,
	)
	if err != nil {
		t.Fatal(err)
	}

	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	path := "/" + validTenantB + "/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z"
	resp, err := http.Get(srv.URL + path) //nolint:noctx // httptest server
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Header.Get("X-Upstream") != "B" {
		t.Fatalf("upstream = %q, want B", resp.Header.Get("X-Upstream"))
	}
	if hitsA.Load() != 0 {
		t.Fatalf("upstream A hits = %d, want 0", hitsA.Load())
	}
	if hitsB.Load() != 1 {
		t.Fatalf("upstream B hits = %d, want 1", hitsB.Load())
	}
}

func TestRouterUnknownTenant404NoUpstream(t *testing.T) {
	var hits atomic.Int32
	up := fakeUpstream(t, "only", &hits)
	t.Cleanup(up.Close)

	clients, err := cluster.ParseClients(validTenantA + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}

	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/" + validTenantB + "/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "unknown tenant") {
		t.Fatalf("body = %q", body)
	}
	if hits.Load() != 0 {
		t.Fatalf("upstream hits = %d, want 0", hits.Load())
	}
}

func TestRouterInvalidTenant404NoUpstream(t *testing.T) {
	var hits atomic.Int32
	up := fakeUpstream(t, "only", &hits)
	t.Cleanup(up.Close)

	clients, err := cluster.ParseClients(validTenantA + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}

	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/INVALID!/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if hits.Load() != 0 {
		t.Fatalf("upstream hits = %d, want 0", hits.Load())
	}
}

func TestRouterUpstreamFailure502(t *testing.T) {
	clients, err := cluster.ParseClients(validTenantA + "=http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}

	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/"+validTenantA+"/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
}

func TestRouterRoutePrefix(t *testing.T) {
	var hits atomic.Int32
	up := fakeUpstream(t, "A", &hits)
	t.Cleanup(up.Close)

	clients, err := cluster.ParseClients(validTenantA + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}

	mux := cluster.NewServeMux(clients, "/api", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/api/" + validTenantA + "/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1", hits.Load())
	}

	resp2, err := http.Get(srv.URL + "/" + validTenantA + "/query?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("unprefixed status = %d, want 404", resp2.StatusCode)
	}
}

func TestRouterHealthEndpoints(t *testing.T) {
	var hits atomic.Int32
	up := fakeUpstream(t, "A", &hits)
	t.Cleanup(up.Close)

	clients, err := cluster.ParseClients(validTenantA + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}

	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path) //nolint:noctx
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", path, resp.StatusCode)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("upstream hits = %d during health checks", hits.Load())
	}
}
