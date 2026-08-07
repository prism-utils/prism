package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/metrics"
	"github.com/elk-utilities/prism/internal/store/queue"
)

type fakeEngine struct {
	open     int
	max      int
	evicted  int64
	openCall int
}

func (f *fakeEngine) OpenTenants() int           { f.openCall++; return f.open }
func (f *fakeEngine) MaxOpenTenants() int        { return f.max }
func (f *fakeEngine) EvictedTenantsTotal() int64 { return f.evicted }

func TestQueueGaugesReportLimiterCaps(t *testing.T) {
	reg := metrics.New(enabledConfig())
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 2, MaxQueue: 128, Wait: 90 * time.Second})
	if err := reg.SetQueueSource(lim); err != nil {
		t.Fatalf("SetQueueSource: %v", err)
	}

	body := scrape(t, reg)
	assertContains(t, body,
		"prism_store_queue_enabled 1",
		"prism_store_queue_max_in_flight 2",
		"prism_store_queue_max_queue 128",
		"prism_store_queue_wait_timeout_seconds 90",
		"prism_store_queue_in_flight 0",
		"prism_store_queue_waiting 0",
	)
}

func TestQueueInFlightGaugeMovesUnderLoad(t *testing.T) {
	reg := metrics.New(enabledConfig())
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 1, MaxQueue: 4, Wait: time.Minute, Observer: reg})
	if err := reg.SetQueueSource(lim); err != nil {
		t.Fatalf("SetQueueSource: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	h := queue.Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	<-entered

	assertContains(t, scrape(t, reg), "prism_store_queue_in_flight 1")
	close(release)
}

func TestQueueRejectionsCarryReasonAndMatchSnapshotTotal(t *testing.T) {
	reg := metrics.New(enabledConfig())
	lim := queue.NewLimiter(queue.LimiterConfig{
		Enabled:     true,
		MaxInFlight: 1,
		MaxQueue:    1,
		Wait:        20 * time.Millisecond,
		Observer:    reg,
	})
	if err := reg.SetQueueSource(lim); err != nil {
		t.Fatalf("SetQueueSource: %v", err)
	}

	block := make(chan struct{})
	started := make(chan struct{}, 1)
	h := queue.Middleware(lim, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-block
		w.WriteHeader(http.StatusOK)
	}))
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	<-started

	// One waiter times out, and while it waits a third request finds the wait
	// queue full — the two shed reasons the limiter distinguishes.
	timedOut := make(chan struct{})
	go func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
		close(timedOut)
	}()
	time.Sleep(5 * time.Millisecond)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	<-timedOut
	close(block)

	body := scrape(t, reg)
	assertContains(t, body,
		`prism_store_queue_rejected_total{reason="queue_full"} 1`,
		`prism_store_queue_rejected_total{reason="wait_timeout"} 1`,
	)
	if got := lim.Snapshot().RejectedTotal; got != 2 {
		t.Fatalf("limiter RejectedTotal = %d, want 2 (must agree with the exported reasons)", got)
	}
}

func TestQueueWaitHistogramObservesAdmissions(t *testing.T) {
	reg := metrics.New(enabledConfig())
	lim := queue.NewLimiter(queue.LimiterConfig{Enabled: true, MaxInFlight: 1, MaxQueue: 4, Wait: time.Minute, Observer: reg})
	h := queue.Middleware(lim, statusHandler(http.StatusOK))
	for i := 0; i < 2; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/sql", nil))
	}

	assertContains(t, scrape(t, reg), "prism_store_queue_wait_seconds_count 2")
}

func TestQueueGaugesReportDisabledLimiter(t *testing.T) {
	reg := metrics.New(enabledConfig())
	if err := reg.SetQueueSource(queue.NewLimiter(queue.LimiterConfig{})); err != nil {
		t.Fatalf("SetQueueSource: %v", err)
	}

	assertContains(t, scrape(t, reg), "prism_store_queue_enabled 0")
}

func TestEngineGaugesReportOpenTenantSaturation(t *testing.T) {
	reg := metrics.New(enabledConfig())
	if err := reg.SetEngineSource(&fakeEngine{open: 3, max: 32, evicted: 7}); err != nil {
		t.Fatalf("SetEngineSource: %v", err)
	}

	body := scrape(t, reg)
	assertContains(t, body,
		"prism_store_engine_open_tenants 3",
		"prism_store_engine_max_open_tenants 32",
		"prism_store_engine_tenant_evictions_total 7",
	)
}

func TestEngineGaugesReadSourceOnEachScrape(t *testing.T) {
	reg := metrics.New(enabledConfig())
	src := &fakeEngine{open: 1, max: 8}
	if err := reg.SetEngineSource(src); err != nil {
		t.Fatalf("SetEngineSource: %v", err)
	}
	assertContains(t, scrape(t, reg), "prism_store_engine_open_tenants 1")

	src.open = 5
	assertContains(t, scrape(t, reg), "prism_store_engine_open_tenants 5")
	if src.openCall < 2 {
		t.Fatalf("engine source read %d times, want one read per scrape", src.openCall)
	}
}

func TestSourcesRejectDuplicateRegistration(t *testing.T) {
	reg := metrics.New(enabledConfig())
	if err := reg.SetEngineSource(&fakeEngine{}); err != nil {
		t.Fatalf("first SetEngineSource: %v", err)
	}
	if err := reg.SetEngineSource(&fakeEngine{}); err == nil {
		t.Fatal("duplicate SetEngineSource returned nil error")
	}
}

func TestDisabledRegistryIgnoresSources(t *testing.T) {
	reg := metrics.New(metrics.Config{Enabled: false})
	if err := reg.SetQueueSource(queue.NewLimiter(queue.LimiterConfig{})); err != nil {
		t.Fatalf("SetQueueSource on disabled exporter: %v", err)
	}
	if err := reg.SetEngineSource(&fakeEngine{}); err != nil {
		t.Fatalf("SetEngineSource on disabled exporter: %v", err)
	}
}

func TestQueueCollectorToleratesAbsentSource(t *testing.T) {
	body := scrape(t, metrics.New(enabledConfig()))
	if strings.Contains(body, "prism_store_queue_in_flight") {
		t.Fatal("queue gauges published without a limiter source")
	}
}
