package admin_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/admin"
	"github.com/prism-utils/prism/internal/store/queue"
)

func TestQueueRoutePatternIsAdminPlaneGet(t *testing.T) {
	if got := admin.QueueRoutePattern(); got != "GET /admin/queue" {
		t.Fatalf("QueueRoutePattern() = %q, want GET /admin/queue", got)
	}
}

func TestQueueHandlerJSONShape(t *testing.T) {
	lim := queue.NewLimiter(queue.LimiterConfig{
		Enabled:     true,
		MaxInFlight: 2,
		MaxQueue:    128,
		Wait:        120 * time.Second,
	})
	rec := httptest.NewRecorder()
	admin.QueueHandler(lim).ServeHTTP(rec, httptest.NewRequestWithContext(
		context.Background(), http.MethodGet, "/admin/queue", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	want := `{"enabled":true,"maxInFlight":2,"maxQueue":128,"timeoutMs":120000,"inFlight":0,"waiting":0,"rejectedTotal":0}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Fatalf("body mismatch\ngot:  %s\nwant: %s", got, want)
	}
}

func TestQueueHandlerWithoutLimiterReportsDisabled(t *testing.T) {
	rec := httptest.NewRecorder()
	admin.QueueHandler(nil).ServeHTTP(rec, httptest.NewRequestWithContext(
		context.Background(), http.MethodGet, "/admin/queue", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	want := `{"enabled":false,"maxInFlight":0,"maxQueue":0,"timeoutMs":0,"inFlight":0,"waiting":0,"rejectedTotal":0}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Fatalf("body mismatch\ngot:  %s\nwant: %s", got, want)
	}
}

func TestQueueHandlerReportsLiveOccupancy(t *testing.T) {
	lim := queue.NewLimiter(queue.LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    2,
		Wait:        time.Minute,
	})
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	gated := queue.Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))
	go gated.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(
		context.Background(), http.MethodGet, "/user-6f3a9c2b-apps/sql", nil))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("gated request did not acquire a slot")
	}
	defer close(block)

	rec := httptest.NewRecorder()
	admin.QueueHandler(lim).ServeHTTP(rec, httptest.NewRequestWithContext(
		context.Background(), http.MethodGet, "/admin/queue", nil))
	want := `{"enabled":true,"maxInFlight":1,"maxQueue":2,"timeoutMs":60000,"inFlight":1,"waiting":0,"rejectedTotal":0}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Fatalf("body mismatch\ngot:  %s\nwant: %s", got, want)
	}
}

func TestQueueAdminTokenEnforced(t *testing.T) {
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 2, MaxQueue: 8, Wait: time.Second})
	mux := http.NewServeMux()
	mux.Handle(admin.QueueRoutePattern(), admin.WithBearerAuth("s3cret", admin.QueueHandler(lim)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	for _, tc := range []struct {
		name   string
		header string
		want   int
	}{
		{name: "no token", header: "", want: http.StatusUnauthorized},
		{name: "wrong token", header: "Bearer wrong", want: http.StatusUnauthorized},
		{name: "right token", header: "Bearer s3cret", want: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/admin/queue", nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			resp := doAdminReq(t, req)
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}
