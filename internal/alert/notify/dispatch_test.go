package notify

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type recorder struct {
	mu       sync.Mutex
	payloads []WebhookPayload
	err      error
}

func (r *recorder) Send(_ context.Context, p WebhookPayload) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.payloads = append(r.payloads, p)
	return nil
}

func (r *recorder) sent() []WebhookPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]WebhookPayload, len(r.payloads))
	copy(out, r.payloads)
	return out
}

func testOpts() Options {
	return Options{
		Receiver:       "tenant-webhook",
		GroupBy:        []string{"alertname", "severity"},
		GroupWait:      30 * time.Second,
		GroupInterval:  5 * time.Minute,
		RepeatInterval: 4 * time.Hour,
		ResolveTimeout: 5 * time.Minute,
	}
}

func firing(alertname, instance string) Alert {
	return Alert{
		Labels:      map[string]string{"alertname": alertname, "severity": "warning", "instance": instance},
		Annotations: map[string]string{"summary": alertname + " on " + instance},
		StartsAt:    time.Unix(0, 0).UTC(),
	}
}

func TestGroupWaitDelaysFirstNotification(t *testing.T) {
	rec := &recorder{}
	d := NewDispatcher(testOpts(), rec, nil)
	t0 := time.Unix(1000, 0).UTC()

	d.Ingest(t0, []Alert{firing("HighCPU", "n1")})

	// Before group_wait: nothing.
	d.Tick(context.Background(), t0.Add(29*time.Second))
	assert.Empty(t, rec.sent())

	// After group_wait: one webhook.
	d.Tick(context.Background(), t0.Add(31*time.Second))
	sent := rec.sent()
	require.Len(t, sent, 1)
	assert.Equal(t, statusFiring, sent[0].Status)
	require.Len(t, sent[0].Alerts, 1)
	assert.Equal(t, "n1", sent[0].Alerts[0].Labels["instance"])
}

func TestAlertsWithSameGroupCoalesce(t *testing.T) {
	rec := &recorder{}
	d := NewDispatcher(testOpts(), rec, nil)
	t0 := time.Unix(1000, 0).UTC()

	d.Ingest(t0, []Alert{firing("HighCPU", "n1")})
	d.Ingest(t0.Add(time.Second), []Alert{firing("HighCPU", "n2")})

	d.Tick(context.Background(), t0.Add(31*time.Second))
	sent := rec.sent()
	require.Len(t, sent, 1, "same alertname+severity is one group → one webhook")
	assert.Len(t, sent[0].Alerts, 2)
	assert.Equal(t, "warning", sent[0].CommonLabels["severity"])
	assert.Equal(t, "HighCPU", sent[0].GroupLabels["alertname"])
}

func TestDifferentGroupsNotifySeparately(t *testing.T) {
	rec := &recorder{}
	d := NewDispatcher(testOpts(), rec, nil)
	t0 := time.Unix(1000, 0).UTC()

	d.Ingest(t0, []Alert{firing("HighCPU", "n1"), firing("DiskFull", "n1")})
	d.Tick(context.Background(), t0.Add(31*time.Second))
	assert.Len(t, rec.sent(), 2)
}

func TestRepeatIntervalSuppressesUnchangedResend(t *testing.T) {
	rec := &recorder{}
	d := NewDispatcher(testOpts(), rec, nil)
	t0 := time.Unix(1000, 0).UTC()

	d.Ingest(t0, []Alert{firing("HighCPU", "n1")})
	d.Tick(context.Background(), t0.Add(31*time.Second)) // first flush
	require.Len(t, rec.sent(), 1)

	// No change, well before repeat_interval → no resend.
	d.Tick(context.Background(), t0.Add(time.Hour))
	assert.Len(t, rec.sent(), 1)

	// Re-ingest the same firing alert (ruler resends), still no new group event
	// before repeat_interval elapses from the last flush.
	d.Ingest(t0.Add(time.Hour), []Alert{firing("HighCPU", "n1")})
	d.Tick(context.Background(), t0.Add(time.Hour+time.Minute))
	assert.Len(t, rec.sent(), 1)

	// After repeat_interval from the flush → resend.
	d.Tick(context.Background(), t0.Add(31*time.Second+4*time.Hour+time.Second))
	assert.Len(t, rec.sent(), 2)
}

func TestGroupIntervalOnChangeAfterFirstFlush(t *testing.T) {
	rec := &recorder{}
	d := NewDispatcher(testOpts(), rec, nil)
	t0 := time.Unix(1000, 0).UTC()

	d.Ingest(t0, []Alert{firing("HighCPU", "n1")})
	flush1 := t0.Add(31 * time.Second)
	d.Tick(context.Background(), flush1)
	require.Len(t, rec.sent(), 1)

	// A new alert joins the group after the first flush: it should notify
	// group_interval after the last flush, not immediately and not repeat later.
	change := flush1.Add(time.Minute)
	d.Ingest(change, []Alert{firing("HighCPU", "n2")})

	d.Tick(context.Background(), flush1.Add(4*time.Minute)) // < group_interval after flush1
	assert.Len(t, rec.sent(), 1)

	d.Tick(context.Background(), flush1.Add(5*time.Minute+time.Second)) // >= group_interval
	sent := rec.sent()
	require.Len(t, sent, 2)
	assert.Len(t, sent[1].Alerts, 2)
}

func TestResolvedAlertNotifiedThenGroupCleared(t *testing.T) {
	rec := &recorder{}
	d := NewDispatcher(testOpts(), rec, nil)
	t0 := time.Unix(1000, 0).UTC()

	d.Ingest(t0, []Alert{firing("HighCPU", "n1")})
	d.Tick(context.Background(), t0.Add(31*time.Second))
	require.Len(t, rec.sent(), 1)

	// The alert resolves; ruler sends a resolved copy within the group_interval
	// window that follows the first flush.
	resolved := firing("HighCPU", "n1")
	resolved.Resolved = true
	resolved.ResolvedAt = t0.Add(2 * time.Minute)
	d.Ingest(t0.Add(2*time.Minute), []Alert{resolved})

	// group_interval after first flush → resolved notification.
	d.Tick(context.Background(), t0.Add(31*time.Second+5*time.Minute+time.Second))
	sent := rec.sent()
	require.Len(t, sent, 2)
	assert.Equal(t, statusResolved, sent[1].Status)
	assert.Equal(t, statusResolved, sent[1].Alerts[0].Status)

	// The group is now empty; a much later tick sends nothing more.
	d.Tick(context.Background(), t0.Add(24*time.Hour))
	assert.Len(t, rec.sent(), 2)
}

func TestSendFailureIsLoggedNotFatal(t *testing.T) {
	rec := &recorder{err: errors.New("boom")}
	d := NewDispatcher(testOpts(), rec, nil)
	t0 := time.Unix(1000, 0).UTC()
	d.Ingest(t0, []Alert{firing("HighCPU", "n1")})
	// Must not panic even though the sender always errors.
	d.Tick(context.Background(), t0.Add(31*time.Second))
}

func TestRunStopsOnContextCancel(t *testing.T) {
	rec := &recorder{}
	d := NewDispatcher(testOpts(), rec, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx, func() time.Time { return time.Now() }, 10*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop on context cancel")
	}
}
