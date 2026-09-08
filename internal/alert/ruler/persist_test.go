package ruler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prism-utils/prism/internal/alert/notify"
)

// persistSink records ruler EventSink calls (firing/resolved transitions).
type persistSink struct {
	mu     sync.Mutex
	alerts []notify.Alert
}

func (p *persistSink) fn(_ context.Context, alerts []notify.Alert) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.alerts = append(p.alerts, alerts...)
}

func (p *persistSink) snapshot() []notify.Alert {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]notify.Alert, len(p.alerts))
	copy(out, p.alerts)
	return out
}

func TestPersistOneRowOnPendingToFiringAndFiringToResolved(t *testing.T) {
	dir := writeRule(t, `
groups:
  - name: test
    rules:
      - alert: HighUp
        expr: up == 1
        for: 0s
        labels: { severity: warning }
        annotations:
          summary: "up is high"
          description: "instance {{ $labels.instance }}"
          runbook: "https://runbooks/highup"
`)
	q := &fakeQuery{}
	q.set(oneSample("n1", 1), nil)
	s := &sink{}
	p := &persistSink{}
	r, err := New(Config{
		RulesDir:           dir,
		EvaluationInterval: time.Second,
		ResendDelay:        time.Second,
		EventSink:          p.fn,
	}, q.fn, s.fn, nil, nil)
	require.NoError(t, err)
	rule := r.rules[0]
	ctx := context.Background()
	base := time.Unix(2000, 0).UTC()

	fired, err := r.evalRule(ctx, rule, base)
	require.NoError(t, err)
	require.Len(t, fired, 1)
	require.False(t, fired[0].Resolved)
	persisted := p.snapshot()
	require.Len(t, persisted, 1, "pending→firing must persist exactly one row")
	assert.False(t, persisted[0].Resolved)
	assert.Equal(t, "HighUp", persisted[0].Labels["alertname"])
	assert.Equal(t, "warning", persisted[0].Labels["severity"])
	assert.Equal(t, "https://runbooks/highup", persisted[0].Annotations["runbook"])
	assert.Equal(t, notify.Fingerprint(persisted[0].Labels), notify.Fingerprint(fired[0].Labels))

	// Identical firing eval inside resendDelay: webhook may stay quiet, persist must not grow.
	again, err := r.evalRule(ctx, rule, base.Add(100*time.Millisecond))
	require.NoError(t, err)
	assert.Empty(t, again, "resendDelay must suppress a repeated firing send")
	assert.Len(t, p.snapshot(), 1, "repeated firing eval must not persist another row")

	// ResendDelay elapsed: webhook re-sends, persist still stays at one firing row.
	resent, err := r.evalRule(ctx, rule, base.Add(2*time.Second))
	require.NoError(t, err)
	require.Len(t, resent, 1)
	assert.False(t, resent[0].Resolved)
	assert.Len(t, p.snapshot(), 1, "resend must not persist another firing row")

	q.set(promql.Vector{}, nil)
	resolved, err := r.evalRule(ctx, rule, base.Add(3*time.Second))
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	assert.True(t, resolved[0].Resolved)
	persisted = p.snapshot()
	require.Len(t, persisted, 2, "firing→resolved must persist exactly one more row")
	assert.True(t, persisted[1].Resolved)
	assert.False(t, persisted[1].ResolvedAt.IsZero())
}

func TestPersistIndependentOfWebhookSink(t *testing.T) {
	dir := writeRule(t, `
groups:
  - name: test
    rules:
      - alert: HighUp
        expr: up == 1
        for: 0s
`)
	q := &fakeQuery{}
	q.set(oneSample("n1", 1), nil)
	p := &persistSink{}
	var sinkCalls int
	failingSink := func(_ time.Time, _ []notify.Alert) {
		sinkCalls++
		// Fail-open: a webhook error must not prevent persist (persist already ran).
	}
	r, err := New(Config{
		RulesDir:           dir,
		EvaluationInterval: time.Second,
		EventSink:          p.fn,
	}, q.fn, failingSink, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()
	base := time.Unix(3000, 0).UTC()

	_, err = r.evalRule(ctx, r.rules[0], base)
	require.NoError(t, err)
	require.Len(t, p.snapshot(), 1, "persist at ruler transition even if webhook path is a no-op")
	assert.Equal(t, 1, sinkCalls)

	q.set(promql.Vector{}, nil)
	_, err = r.evalRule(ctx, r.rules[0], base.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, p.snapshot(), 2)
	assert.True(t, p.snapshot()[1].Resolved)
	assert.Equal(t, 2, sinkCalls)
}

func TestPersistNotOnPendingDrop(t *testing.T) {
	dir := writeRule(t, `
groups:
  - name: test
    rules:
      - alert: SlowUp
        expr: up == 1
        for: 1h
`)
	q := &fakeQuery{}
	q.set(oneSample("n1", 1), nil)
	p := &persistSink{}
	r, err := New(Config{
		RulesDir:           dir,
		EvaluationInterval: time.Second,
		EventSink:          p.fn,
	}, q.fn, (&sink{}).fn, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()
	base := time.Unix(4000, 0).UTC()

	pending, err := r.evalRule(ctx, r.rules[0], base)
	require.NoError(t, err)
	assert.Empty(t, pending)
	assert.Empty(t, p.snapshot(), "pending must not persist")

	q.set(promql.Vector{}, nil)
	dropped, err := r.evalRule(ctx, r.rules[0], base.Add(time.Second))
	require.NoError(t, err)
	assert.Empty(t, dropped)
	assert.Empty(t, p.snapshot(), "pending drop must not persist")
}

func TestPersistUsesRecommendationAnnotation(t *testing.T) {
	dir := writeRule(t, `
groups:
  - name: test
    rules:
      - alert: RecAlert
        expr: up == 1
        for: 0s
        annotations:
          recommendation: "page the on-call"
`)
	q := &fakeQuery{}
	q.set(oneSample("n1", 1), nil)
	p := &persistSink{}
	r, err := New(Config{
		RulesDir:           dir,
		EvaluationInterval: time.Second,
		EventSink:          p.fn,
	}, q.fn, (&sink{}).fn, nil, nil)
	require.NoError(t, err)
	_, err = r.evalRule(context.Background(), r.rules[0], time.Unix(5000, 0).UTC())
	require.NoError(t, err)
	got := p.snapshot()
	require.Len(t, got, 1)
	assert.Equal(t, "page the on-call", got[0].Annotations["recommendation"])
}
