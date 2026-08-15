package queue

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSnapshotReportsCapsAndTimeout(t *testing.T) {
	t.Parallel()
	lim := NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 3,
		MaxQueue:    9,
		Wait:        2500 * time.Millisecond,
	})
	want := Snapshot{Enabled: true, MaxInFlight: 3, MaxQueue: 9, TimeoutMs: 2500}
	if got := lim.Snapshot(); got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func TestSnapshotNilLimiterIsDisabledZeros(t *testing.T) {
	t.Parallel()
	var lim *Limiter
	if got := lim.Snapshot(); got != (Snapshot{}) {
		t.Fatalf("snapshot = %+v, want zero value", got)
	}
}

func TestSnapshotCountsInFlightAndWaiting(t *testing.T) {
	t.Parallel()
	lim := NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    4,
		Wait:        time.Minute,
	})
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire slot")
	}
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))

	deadline := time.After(time.Second)
	for {
		got := lim.Snapshot()
		if got.InFlight == 1 && got.Waiting == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("snapshot = %+v, want InFlight 1 and Waiting 1", got)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if got := lim.Snapshot().RejectedTotal; got != 0 {
		t.Fatalf("rejectedTotal = %d, want 0 while waiters still wait", got)
	}

	close(block)
}

func TestSnapshotRejectedTotalCountsQueueFull429(t *testing.T) {
	t.Parallel()
	lim := NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    1,
		Wait:        time.Minute,
	})
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire slot")
	}
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	waitForWaiting(t, lim, 1)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := lim.Snapshot().RejectedTotal; got != 1 {
		t.Fatalf("rejectedTotal = %d, want 1", got)
	}

	close(block)
}

func TestSnapshotRejectedTotalCountsWaitTimeout429(t *testing.T) {
	t.Parallel()
	lim := NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    4,
		Wait:        20 * time.Millisecond,
	})
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire slot")
	}

	for i := 1; i <= 2; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("request %d status = %d, want 429", i, rec.Code)
		}
		if got := lim.Snapshot().RejectedTotal; got != int64(i) {
			t.Fatalf("rejectedTotal after %d timeouts = %d", i, got)
		}
	}

	close(block)
}

func TestSnapshotRejectedTotalCountsClientCancel(t *testing.T) {
	t.Parallel()
	lim := NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    4,
		Wait:        time.Minute,
	})
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire slot")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil))
		close(done)
	}()
	waitForWaiting(t, lim, 1)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not return")
	}
	if rec.Code != 499 {
		t.Fatalf("status = %d, want 499", rec.Code)
	}
	if got := lim.Snapshot().RejectedTotal; got != 1 {
		t.Fatalf("rejectedTotal = %d, want 1", got)
	}

	close(block)
}

func TestSnapshotDisabledLimiterNeverCounts(t *testing.T) {
	t.Parallel()
	lim := NewLimiter(LimiterConfig{Enabled: false, MaxInFlight: 1, MaxQueue: 1, Wait: time.Millisecond})
	h := Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, rec.Code)
		}
	}
	got := lim.Snapshot()
	if got.Enabled {
		t.Fatal("snapshot reports enabled for a disabled limiter")
	}
	if got.InFlight != 0 || got.Waiting != 0 || got.RejectedTotal != 0 {
		t.Fatalf("snapshot = %+v, want zero counters when disabled", got)
	}
}

func waitForWaiting(t *testing.T, lim *Limiter, want int64) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if got := lim.Snapshot().Waiting; got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("waiting = %d, want %d", lim.Snapshot().Waiting, want)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}
