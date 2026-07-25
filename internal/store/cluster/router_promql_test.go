package cluster_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/elk-utilities/prism/internal/store/cluster"
)

func TestRouterForwardsPromQLRoutes(t *testing.T) {
	var hitsA, hitsB atomic.Int32
	upA := fakeUpstream(t, "A", &hitsA)
	t.Cleanup(upA.Close)
	upB := fakeUpstream(t, "B", &hitsB)
	t.Cleanup(upB.Close)

	clients, err := cluster.ParseClients(validTenantA + "=" + upA.URL + "," + validTenantB + "=" + upB.URL)
	if err != nil {
		t.Fatal(err)
	}
	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cases := []struct {
		name     string
		path     string
		wantPath string
	}{
		{"instant", "/" + validTenantA + "/api/v1/query?query=up", "/" + validTenantA + "/api/v1/query"},
		{"range", "/" + validTenantA + "/api/v1/query_range?query=up&step=15s", "/" + validTenantA + "/api/v1/query_range"},
		{"series", "/" + validTenantA + "/api/v1/series?match[]=up", "/" + validTenantA + "/api/v1/series"},
		{"labels", "/" + validTenantA + "/api/v1/labels", "/" + validTenantA + "/api/v1/labels"},
		{"label_values", "/" + validTenantA + "/api/v1/label/job/values", "/" + validTenantA + "/api/v1/label/job/values"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d body = %s", resp.StatusCode, body)
			}
			if got := resp.Header.Get("X-Upstream"); got != "A" {
				t.Fatalf("upstream = %q, want A (tenant must route to owner)", got)
			}
			if got := resp.Header.Get("X-Path"); got != tc.wantPath {
				t.Fatalf("forwarded path = %q, want %q", got, tc.wantPath)
			}
		})
	}
}

func TestRouterPromQLUnknownTenantIs404(t *testing.T) {
	var hits atomic.Int32
	upA := fakeUpstream(t, "A", &hits)
	t.Cleanup(upA.Close)
	clients, err := cluster.ParseClients(validTenantA + "=" + upA.URL)
	if err != nil {
		t.Fatal(err)
	}
	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		srv.URL+"/"+validTenantB+"/api/v1/query?query=up", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unowned tenant", resp.StatusCode)
	}
	if hits.Load() != 0 {
		t.Fatalf("must not forward unknown tenant: hits = %d", hits.Load())
	}
}
