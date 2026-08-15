package queue

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMiddleware_disabledPassthrough(t *testing.T) {
	t.Parallel()
	var called atomic.Bool
	h := Middleware(&Limiter{Enabled: false}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Store(true)
		w.WriteHeader(http.StatusTeapot)
	}))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !called.Load() {
		t.Fatal("handler not invoked when limiter disabled")
	}
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rec.Code)
	}
}

func TestMiddleware_maxInFlightConcurrency(t *testing.T) {
	t.Parallel()
	const maxInFlight = 2
	start := make(chan struct{})
	release := make(chan struct{})
	var inFlight atomic.Int32

	blocking := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inFlight.Add(1)
		defer inFlight.Add(-1)
		<-start
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	})

	lim := NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: maxInFlight,
		MaxQueue:    8,
		Wait:        time.Second,
	})
	h := Middleware(lim, blocking)

	var wg sync.WaitGroup
	for i := 0; i < maxInFlight+2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
		}()
	}

	deadline := time.After(500 * time.Millisecond)
	for {
		if n := inFlight.Load(); n == maxInFlight {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("inFlight = %d, want %d", inFlight.Load(), maxInFlight)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if n := inFlight.Load(); n > maxInFlight {
		t.Fatalf("inFlight = %d, exceeds MaxInFlight %d", n, maxInFlight)
	}

	close(start)
	close(release)
	wg.Wait()
}

func TestMiddleware_waiterBeyondMaxQueueImmediate429(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	started := make(chan struct{}, 2)
	h := Middleware(NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    1,
		Wait:        time.Second,
	}), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec1 := httptest.NewRecorder()
	done1 := make(chan struct{})
	go func() {
		h.ServeHTTP(rec1, req1)
		close(done1)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire slot")
	}

	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec2 := httptest.NewRecorder()
	done2 := make(chan struct{})
	go func() {
		h.ServeHTTP(rec2, req2)
		close(done2)
	}()

	time.Sleep(50 * time.Millisecond)

	req3 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("third request status = %d, want 429", rec3.Code)
	}
	if rec3.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", rec3.Header().Get("Retry-After"))
	}

	close(block)
	<-done1
	<-done2
}

func TestMiddleware_waitTimeout429(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    4,
		Wait:        30 * time.Millisecond,
	}), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec1 := httptest.NewRecorder()
	go h.ServeHTTP(rec1, req1)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire slot")
	}

	req2 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec2 := httptest.NewRecorder()
	start := time.Now()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec2.Code)
	}
	if rec2.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", rec2.Header().Get("Retry-After"))
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("waited %v, expected at least ~30ms timeout", elapsed)
	}

	close(block)
}

func TestMiddleware_slotReleasedAfterHandler(t *testing.T) {
	t.Parallel()
	var seq atomic.Int32
	h := Middleware(NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    2,
		Wait:        time.Second,
	}), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		seq.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 3; i++ {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, rec.Code)
		}
	}
	if got := seq.Load(); got != 3 {
		t.Fatalf("handler calls = %d, want 3", got)
	}
}

func TestMiddleware_panicInHandlerDoesNotLeakWaiter(t *testing.T) {
	t.Parallel()
	lim := NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    1,
		Wait:        time.Second,
	})

	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(lim, http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		panic("boom")
	}))

	// Occupy the single slot, then queue a waiter that acquires and panics.
	go func() {
		defer func() { _ = recover() }()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	}()
	<-started
	waiterDone := make(chan struct{})
	go func() {
		defer func() { _ = recover(); close(waiterDone) }()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	}()
	time.Sleep(50 * time.Millisecond)

	close(block) // first handler panics, releasing its slot to the waiter
	<-waiterDone // waiter acquires slot, then panics

	// After both panics the waiter count is back to zero (no leak, and the
	// idempotent release never over-decrements) and the slot is free.
	if got := lim.waiters.Load(); got != 0 {
		t.Fatalf("waiters = %d after panics, want 0", got)
	}
	if got := len(lim.sem); got != 0 {
		t.Fatalf("in-flight slots = %d after panics, want 0", got)
	}
}

func TestMiddleware_clientCancelReturnsPromptly(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	lim := NewLimiter(LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    4,
		Wait:        time.Minute,
	})
	h := Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	req1 := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
	rec1 := httptest.NewRecorder()
	go h.ServeHTTP(rec1, req1)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not acquire slot")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req2 := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	rec2 := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rec2, req2)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cancelled waiter did not return promptly")
	}
	if rec2.Code != 499 {
		t.Fatalf("status = %d, want 499 body=%s", rec2.Code, rec2.Body.String())
	}
	if rec2.Header().Get("Retry-After") != "" {
		t.Fatalf("Retry-After = %q, want empty", rec2.Header().Get("Retry-After"))
	}
	if !strings.Contains(rec2.Body.String(), "client closed") {
		t.Fatalf("body = %q, want client closed", rec2.Body.String())
	}
	if got := lim.Snapshot().Waiting; got != 0 {
		t.Fatalf("waiting = %d after cancel, want 0", got)
	}
	if got := lim.Snapshot().InFlight; got != 1 {
		t.Fatalf("inFlight = %d after waiter cancel, want 1 (holder still running)", got)
	}

	close(block)
}
