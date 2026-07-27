package ruler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/elk-utilities/prism/internal/alert/notify"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakeQuery is a switchable QueryFunc: it returns either a canned vector or an
// error, so tests can simulate prism-store returning data, going empty, or
// becoming unreachable.
type fakeQuery struct {
	mu     sync.Mutex
	vec    promql.Vector
	err    error
	called int
}

func (f *fakeQuery) fn(_ context.Context, _ string, _ time.Time) (promql.Vector, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called++
	if f.err != nil {
		return nil, f.err
	}
	return append(promql.Vector(nil), f.vec...), nil
}

func (f *fakeQuery) set(vec promql.Vector, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.vec, f.err = vec, err
}

func (f *fakeQuery) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

// sink collects alerts the ruler emits.
type sink struct {
	mu     sync.Mutex
	alerts []notify.Alert
}

func (s *sink) fn(_ time.Time, alerts []notify.Alert) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alerts = append(s.alerts, alerts...)
}

func (s *sink) snapshot() []notify.Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]notify.Alert, len(s.alerts))
	copy(out, s.alerts)
	return out
}

func (s *sink) latestFor(alertname string) (notify.Alert, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.alerts) - 1; i >= 0; i-- {
		if s.alerts[i].Labels["alertname"] == alertname {
			return s.alerts[i], true
		}
	}
	return notify.Alert{}, false
}

func writeRule(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yml"), []byte(body), 0o600))
	return dir
}

func oneSample(instance string, v float64) promql.Vector {
	b := labels.NewBuilder(labels.EmptyLabels())
	b.Set("instance", instance)
	return promql.Vector{promql.Sample{T: 0, F: v, Metric: b.Labels()}}
}

// runRuler starts the ruler and returns a stop func that cancels and waits for
// Run to return, so no manager goroutine outlives the test (goleak-clean).
func runRuler(t *testing.T, cfg Config, q QueryFunc, s AlertSink) {
	t.Helper()
	r, err := New(cfg, q, s, nil, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		assert.NoError(t, r.Run(ctx))
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("ruler Run did not return after cancel")
		}
	})
}

func TestFiringImmediatelyWithForZeroAndTemplating(t *testing.T) {
	dir := writeRule(t, `
groups:
  - name: test
    rules:
      - alert: HighUp
        expr: up == 1
        for: 0s
        labels: { severity: warning }
        annotations: { summary: "value {{ $value }} on {{ $labels.instance }}" }
`)
	q := &fakeQuery{}
	q.set(oneSample("n1", 5), nil)
	s := &sink{}
	runRuler(t, Config{RulesDir: dir, EvaluationInterval: 20 * time.Millisecond}, q.fn, s.fn)

	require.Eventually(t, func() bool {
		_, ok := s.latestFor("HighUp")
		return ok
	}, 3*time.Second, 10*time.Millisecond)

	a, _ := s.latestFor("HighUp")
	assert.False(t, a.Resolved)
	assert.Equal(t, "warning", a.Labels["severity"])
	assert.Equal(t, "n1", a.Labels["instance"])
	assert.Equal(t, "value 5 on n1", a.Annotations["summary"], "$value and $labels expanded")
}

func TestPendingHeldDuringForDuration(t *testing.T) {
	dir := writeRule(t, `
groups:
  - name: test
    rules:
      - alert: SlowUp
        expr: up == 1
        for: 1h
        labels: { severity: warning }
`)
	q := &fakeQuery{}
	q.set(oneSample("n1", 1), nil)
	s := &sink{}
	runRuler(t, Config{RulesDir: dir, EvaluationInterval: 20 * time.Millisecond}, q.fn, s.fn)

	// Give the manager many evaluation cycles; with for:1h the alert stays
	// pending and must never be handed to the sink.
	require.Eventually(t, func() bool { return q.callCount() > 5 }, 3*time.Second, 10*time.Millisecond)
	assert.Empty(t, s.snapshot(), "alert must stay pending until the for duration elapses")
}

func TestResolvedWhenExpressionGoesEmpty(t *testing.T) {
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
	s := &sink{}
	runRuler(t, Config{RulesDir: dir, EvaluationInterval: 20 * time.Millisecond}, q.fn, s.fn)

	require.Eventually(t, func() bool {
		a, ok := s.latestFor("HighUp")
		return ok && !a.Resolved
	}, 3*time.Second, 10*time.Millisecond)

	// The series disappears: the rule now returns nothing, so the alert resolves.
	q.set(promql.Vector{}, nil)
	require.Eventually(t, func() bool {
		a, ok := s.latestFor("HighUp")
		return ok && a.Resolved
	}, 3*time.Second, 10*time.Millisecond)
	a, _ := s.latestFor("HighUp")
	assert.False(t, a.ResolvedAt.IsZero())
}

func TestFailOpenKeepsStateWhenStoreUnreachable(t *testing.T) {
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
	s := &sink{}
	runRuler(t, Config{RulesDir: dir, EvaluationInterval: 20 * time.Millisecond}, q.fn, s.fn)

	require.Eventually(t, func() bool {
		a, ok := s.latestFor("HighUp")
		return ok && !a.Resolved
	}, 3*time.Second, 10*time.Millisecond)
	firingCount := len(s.snapshot())

	// prism-store becomes unreachable: the query errors. The alert must NOT be
	// resolved on a query failure — state is kept (fail-open).
	q.set(nil, errors.New("connection refused"))
	require.Eventually(t, func() bool { return q.callCount() > firingCount+5 }, 3*time.Second, 10*time.Millisecond)
	for _, a := range s.snapshot() {
		assert.False(t, a.Resolved, "query failure must not resolve alerts")
	}
}

func TestRuleFilesGlobAndMissingDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.yml"), []byte("groups: []\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.yaml"), []byte("groups: []\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("x"), 0o600))

	files, err := ruleFiles(dir)
	require.NoError(t, err)
	require.Len(t, files, 2)
	assert.Equal(t, filepath.Join(dir, "a.yml"), files[0])
	assert.Equal(t, filepath.Join(dir, "b.yaml"), files[1])

	// A not-yet-mounted ConfigMap must not crash startup.
	missing, err := ruleFiles(filepath.Join(dir, "nope"))
	require.NoError(t, err)
	assert.Empty(t, missing)
}

func TestNewRejectsBadRuleFile(t *testing.T) {
	dir := writeRule(t, "this: is: not: valid: prometheus: rules\n")
	q := &fakeQuery{}
	// New compiles the rules, so a malformed file fails fast at construction.
	_, err := New(Config{RulesDir: dir, EvaluationInterval: time.Second}, q.fn, (&sink{}).fn, nil, nil)
	require.Error(t, err)
}

func TestNewRejectsInvalidExpr(t *testing.T) {
	dir := writeRule(t, `
groups:
  - name: bad
    rules:
      - alert: BadExpr
        expr: "up{"
`)
	q := &fakeQuery{}
	_, err := New(Config{RulesDir: dir, EvaluationInterval: time.Second}, q.fn, (&sink{}).fn, nil, nil)
	require.Error(t, err)
}

// TestKeepFiringForHoldsThenResolves drives evalRule at controlled instants so
// the keep_firing_for window is deterministic (no wall-clock timing): after the
// series stops matching the alert must keep firing until the window elapses,
// then resolve exactly once.
func TestKeepFiringForHoldsThenResolves(t *testing.T) {
	dir := writeRule(t, `
groups:
  - name: test
    rules:
      - alert: KeepUp
        expr: up == 1
        for: 0s
        keep_firing_for: 30s
`)
	q := &fakeQuery{}
	q.set(oneSample("n1", 1), nil)
	r, err := New(Config{RulesDir: dir, EvaluationInterval: time.Second}, q.fn, (&sink{}).fn, nil, nil)
	require.NoError(t, err)
	rule := r.rules[0]
	ctx := context.Background()
	base := time.Unix(1000, 0)

	// t0: series matches, for:0s ⇒ fires immediately.
	fired, err := r.evalRule(ctx, rule, base)
	require.NoError(t, err)
	require.Len(t, fired, 1)
	require.False(t, fired[0].Resolved)

	// Series clears, but we are inside keep_firing_for ⇒ still firing, never resolved.
	q.set(promql.Vector{}, nil)
	held, err := r.evalRule(ctx, rule, base.Add(10*time.Second))
	require.NoError(t, err)
	for _, a := range held {
		assert.False(t, a.Resolved, "must keep firing within keep_firing_for")
	}

	// keep_firing_for elapsed ⇒ resolves exactly once.
	resolved, err := r.evalRule(ctx, rule, base.Add(40*time.Second))
	require.NoError(t, err)
	require.Len(t, resolved, 1)
	assert.True(t, resolved[0].Resolved, "must resolve after keep_firing_for elapses")

	// State is pruned once the resolve is emitted; a further eval sends nothing.
	none, err := r.evalRule(ctx, rule, base.Add(41*time.Second))
	require.NoError(t, err)
	assert.Empty(t, none)
}
