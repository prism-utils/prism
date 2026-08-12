package cluster_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/prism-utils/prism/internal/store/admin"
	"github.com/prism-utils/prism/internal/store/cluster"
)

// TestRouterOmitsQueueSnapshotRoute pins the coordinator's answer for the live
// queue snapshot: a coordinator runs no in-flight limiter of its own, so the
// route is absent (404) rather than reporting zeros that operators would read as
// "no queries queued" on the data nodes behind it.
func TestRouterOmitsQueueSnapshotRoute(t *testing.T) {
	var hits atomic.Int32
	up := fakeUpstream(t, "A", &hits)
	t.Cleanup(up.Close)

	clients, err := cluster.ParseClients(validTenantA + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(cluster.NewServeMux(clients, "", nil))
	t.Cleanup(srv.Close)

	method, path, ok := splitRoutePattern(admin.QueueRoutePattern())
	if !ok {
		t.Fatalf("pattern %q is not \"METHOD /path\"", admin.QueueRoutePattern())
	}
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("coordinator %s = %d, want 404", admin.QueueRoutePattern(), resp.StatusCode)
	}
	if hits.Load() != 0 {
		t.Fatalf("upstream hits = %d, want 0", hits.Load())
	}
}

func splitRoutePattern(p string) (method, path string, ok bool) {
	for i := 0; i < len(p); i++ {
		if p[i] == ' ' {
			return p[:i], p[i+1:], true
		}
	}
	return "", "", false
}
