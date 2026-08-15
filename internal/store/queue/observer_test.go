package queue

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingObserver struct {
	mu       sync.Mutex
	admitted []time.Duration
	rejected []RejectReason
	waits    []time.Duration
}

func (o *recordingObserver) ObserveAdmitted(wait time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.admitted = append(o.admitted, wait)
}

func (o *recordingObserver) ObserveRejected(reason RejectReason, wait time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rejected = append(o.rejected, reason)
	o.waits = append(o.waits, wait)
}

func (o *recordingObserver) reasons() []RejectReason {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]RejectReason(nil), o.rejected...)
}

func (o *recordingObserver) admissions() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.admitted)
}

func TestObserverSeesUncontendedAdmission(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	h := Middleware(NewLimiter(LimiterConfig{
		Enabled: true, MaxInFlight: 1, MaxQueue: 1, Wait: time.Second, Observer: obs,
	}), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))

	if got := obs.admissions(); got != 1 {
		t.Fatalf("admissions = %d, want 1", got)
	}
	if got := obs.admitted[0]; got != 0 {
		t.Fatalf("uncontended wait = %v, want 0", got)
	}
}

func TestObserverSeesQueueFullRejection(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(NewLimiter(LimiterConfig{
		Enabled: true, MaxInFlight: 1, MaxQueue: 1, Wait: time.Minute, Observer: obs,
	}), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	<-started
	waiterDone := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
		close(waiterDone)
	}()
	time.Sleep(50 * time.Millisecond)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	close(block)
	<-waiterDone

	got := obs.reasons()
	if len(got) != 1 || got[0] != RejectQueueFull {
		t.Fatalf("reasons = %v, want [%s]", got, RejectQueueFull)
	}
}

func TestObserverSeesWaitTimeoutRejection(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(NewLimiter(LimiterConfig{
		Enabled: true, MaxInFlight: 1, MaxQueue: 4, Wait: 30 * time.Millisecond, Observer: obs,
	}), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	<-started
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	close(block)

	got := obs.reasons()
	if len(got) != 1 || got[0] != RejectWaitTimeout {
		t.Fatalf("reasons = %v, want [%s]", got, RejectWaitTimeout)
	}
	if obs.waits[0] <= 0 {
		t.Fatalf("timed-out wait = %v, want the time actually spent queued", obs.waits[0])
	}
}

func TestObserverSeesClientCancelRejection(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(NewLimiter(LimiterConfig{
		Enabled: true, MaxInFlight: 1, MaxQueue: 4, Wait: time.Minute, Observer: obs,
	}), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	rec := httptest.NewRecorder()
	go func() {
		h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodPost, "/sql", nil))
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	close(block)

	got := obs.reasons()
	if len(got) != 1 || got[0] != RejectClientCanceled {
		t.Fatalf("reasons = %v, want [%s]", got, RejectClientCanceled)
	}
	if rec.Code != 499 {
		t.Fatalf("status = %d, want 499 body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Fatalf("Retry-After = %q, want empty", rec.Header().Get("Retry-After"))
	}
	if !strings.Contains(rec.Body.String(), "client closed") {
		t.Fatalf("body = %q, want client closed", rec.Body.String())
	}
}

func TestObservedRejectionsMatchSnapshotTotal(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	lim := NewLimiter(LimiterConfig{
		Enabled: true, MaxInFlight: 1, MaxQueue: 1, Wait: 10 * time.Millisecond, Observer: obs,
	})
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	<-started
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	}
	close(block)

	if got, want := int64(len(obs.reasons())), lim.Snapshot().RejectedTotal; got != want {
		t.Fatalf("observed rejections = %d, snapshot RejectedTotal = %d", got, want)
	}
}

func TestLimiterWithoutObserverStillServes(t *testing.T) {
	t.Parallel()
	h := Middleware(NewLimiter(LimiterConfig{Enabled: true, MaxInFlight: 1, MaxQueue: 1, Wait: time.Second}),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rec.Code)
	}
}
