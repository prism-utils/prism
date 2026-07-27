//go:build e2e

// This file drives prism-alert end to end against a *real* PromQL engine: the
// canonical prometheus/promql engine evaluates actual alerting expressions
// (`up == 1`) over a TSDB-backed store exposing prism-store's
// /{ns}/api/v1/query shape. The real ruler HTTP client, for/resolve state
// machine, Alertmanager-style dispatcher, and v4 webhook client all run, and a
// real HTTP notifier receiver asserts it gets a bearer-authenticated,
// contract-shaped webhook — firing first, then resolved.
package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elk-utilities/prism/internal/alert/notify"
	"github.com/elk-utilities/prism/internal/alert/ruler"
)

// evalBase is the instant the ruler starts evaluating at. The loaded series
// carry samples through t=300s, and the engine's 5m lookback means an instant
// query keeps matching for another 300s — so a clock that advances a little
// each cycle (see advancingClock) both makes `up == 1` match and lets
// resolvedAt strictly exceed the firing send time, which the resolve path
// requires.
var evalBase = time.Unix(300, 0).UTC()

// advancingClock returns a monotonically increasing clock starting at evalBase,
// stepping 200ms per call. Real time is unusable here because the loaded series
// live at t≈300s (1970), so wall-clock queries would never match.
func advancingClock() func() time.Time {
	var mu sync.Mutex
	var n int64
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t := evalBase.Add(time.Duration(n) * 200 * time.Millisecond)
		n++
		return t
	}
}

// promStore is a minimal prism-store PromQL endpoint backed by the real engine.
// The backing storage is swappable so the test can flip a series from present
// to absent and observe a real resolve.
type promStore struct {
	engine *promql.Engine
	mu     sync.Mutex
	store  storage.Queryable
}

func (p *promStore) swap(s storage.Queryable) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store = s
}

func (p *promStore) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{ns}/api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		tsUnix, _ := strconv.ParseInt(r.URL.Query().Get("time"), 10, 64)
		p.mu.Lock()
		st := p.store
		p.mu.Unlock()

		query, err := p.engine.NewInstantQuery(r.Context(), st, nil, q, time.Unix(tsUnix, 0))
		if err != nil {
			writeErr(w, err)
			return
		}
		defer query.Close()
		res := query.Exec(r.Context())
		if res.Err != nil {
			writeErr(w, res.Err)
			return
		}
		writeResult(w, res, tsUnix)
	})
	return mux
}

func writeErr(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "errorType": "bad_data", "error": err.Error()})
}

func writeResult(w http.ResponseWriter, res *promql.Result, tsUnix int64) {
	type jsonSample struct {
		Metric map[string]string `json:"metric"`
		Value  [2]any            `json:"value"`
	}
	envelope := map[string]any{"status": "success", "data": map[string]any{}}
	data := envelope["data"].(map[string]any)
	switch v := res.Value.(type) {
	case promql.Vector:
		data["resultType"] = "vector"
		out := make([]jsonSample, 0, len(v))
		for _, s := range v {
			out = append(out, jsonSample{
				Metric: s.Metric.Map(),
				Value:  [2]any{float64(tsUnix), strconv.FormatFloat(s.F, 'g', -1, 64)},
			})
		}
		data["result"] = out
	case promql.Scalar:
		data["resultType"] = "scalar"
		data["result"] = [2]any{float64(tsUnix), strconv.FormatFloat(v.V, 'g', -1, 64)}
	default:
		data["resultType"] = "vector"
		data["result"] = []jsonSample{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(envelope)
}

// notifierReceiver is a real HTTP server standing in for the prism notifier: it
// verifies the bearer token and records every decoded Alertmanager webhook.
type notifierReceiver struct {
	secret string
	mu     sync.Mutex
	got    []notify.WebhookPayload
}

func (n *notifierReceiver) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+n.secret {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var p notify.WebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil || len(p.Alerts) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		n.mu.Lock()
		n.got = append(n.got, p)
		n.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	})
	return mux
}

func (n *notifierReceiver) latest() (notify.WebhookPayload, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.got) == 0 {
		return notify.WebhookPayload{}, false
	}
	return n.got[len(n.got)-1], true
}

// upStore returns a storage.Queryable serving a single `up{instance,job}`
// series whose samples span t=0..300s at the given value. Feeding this to the
// real promql.Engine means `up == 1` is evaluated by the canonical engine —
// actual PromQL — without dragging in teststorage/promqltest (and, through
// them, storage/remote's cloud-provider sigv4 SDKs), keeping the module's
// dependency graph as lean as the shipped binary.
func upStore(value float64) storage.Queryable {
	lset := labels.FromStrings(model.MetricNameLabel, "up", "instance", "node-a", "job", "node")
	smpls := make([]chunks.Sample, 0, 6)
	for ms := int64(0); ms <= 300_000; ms += 60_000 {
		smpls = append(smpls, fSample{t: ms, v: value})
	}
	return &memQueryable{series: []storage.Series{storage.NewListSeries(lset, smpls)}}
}

// memQueryable is a trivial in-memory storage.Queryable over a fixed series
// slice; it is enough for the engine to run instant queries in this test.
type memQueryable struct{ series []storage.Series }

func (m *memQueryable) Querier(int64, int64) (storage.Querier, error) {
	return &memQuerier{series: m.series}, nil
}

type memQuerier struct{ series []storage.Series }

func (q *memQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	var out []storage.Series
	for _, s := range q.series {
		if seriesMatches(s.Labels(), matchers) {
			out = append(out, s)
		}
	}
	return &sliceSeriesSet{series: out, i: -1}
}

func (q *memQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *memQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *memQuerier) Close() error { return nil }

func seriesMatches(lset labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lset.Get(m.Name)) {
			return false
		}
	}
	return true
}

type sliceSeriesSet struct {
	series []storage.Series
	i      int
}

func (s *sliceSeriesSet) Next() bool                        { s.i++; return s.i < len(s.series) }
func (s *sliceSeriesSet) At() storage.Series                { return s.series[s.i] }
func (s *sliceSeriesSet) Err() error                        { return nil }
func (s *sliceSeriesSet) Warnings() annotations.Annotations { return nil }

// fSample is a float-only chunks.Sample (no histograms), matching the engine's
// expectations for the loaded series.
type fSample struct {
	t int64
	v float64
}

func (s fSample) T() int64                      { return s.t }
func (s fSample) ST() int64                     { return 0 }
func (s fSample) F() float64                    { return s.v }
func (s fSample) H() *histogram.Histogram       { return nil }
func (s fSample) FH() *histogram.FloatHistogram { return nil }
func (s fSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s fSample) Copy() chunks.Sample           { return s }

func TestAlertFiresAndResolvesWithRealPromQL(t *testing.T) {
	eng := promql.NewEngine(promql.EngineOpts{MaxSamples: 1_000_000, Timeout: 10 * time.Second, EnableAtModifier: true, LookbackDelta: 5 * time.Minute})
	up := upStore(1)
	down := upStore(0)

	ps := &promStore{engine: eng, store: up}
	storeSrv := httptest.NewServer(ps.handler())
	defer storeSrv.Close()

	notifier := &notifierReceiver{secret: "webhook-secret"}
	notifierSrv := httptest.NewServer(notifier.handler())
	defer notifierSrv.Close()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yml"), []byte(`
groups:
  - name: e2e
    rules:
      - alert: NodeUp
        expr: up == 1
        for: 0s
        labels: { severity: critical }
        annotations: { summary: "up is {{ $value }} on {{ $labels.instance }}" }
`), 0o600))

	client, err := ruler.NewPromQLClient(storeSrv.URL, "", "tenant-e2e", "", nil)
	require.NoError(t, err)

	webhook := notify.NewWebhookClient(notify.WebhookConfig{URL: notifierSrv.URL + "/webhook", Secret: "webhook-secret"}, nil)
	dispatcher := notify.NewDispatcher(notify.Options{
		Receiver:       "tenant-webhook",
		GroupBy:        []string{"alertname", "severity"},
		GroupWait:      0,
		GroupInterval:  time.Millisecond,
		RepeatInterval: time.Hour,
		ResolveTimeout: 5 * time.Minute,
	}, webhook, nil)

	r, err := ruler.New(ruler.Config{RulesDir: dir, EvaluationInterval: 50 * time.Millisecond},
		client.Query, dispatcher.Ingest, nil, advancingClock())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx, time.Now, 20*time.Millisecond)
	go func() { _ = r.Run(ctx) }()

	// Firing: real evaluation of `up == 1` matches, alert fires, notifier gets a
	// valid v4 webhook.
	require.Eventually(t, func() bool {
		p, ok := notifier.latest()
		return ok && p.Status == "firing"
	}, 10*time.Second, 50*time.Millisecond)

	p, _ := notifier.latest()
	assert.Equal(t, "4", p.Version)
	assert.Equal(t, "tenant-webhook", p.Receiver)
	require.Len(t, p.Alerts, 1)
	assert.Equal(t, "NodeUp", p.Alerts[0].Labels["alertname"])
	assert.Equal(t, "critical", p.Alerts[0].Labels["severity"])
	assert.Equal(t, "node-a", p.Alerts[0].Labels["instance"])
	assert.Equal(t, "up is 1 on node-a", p.Alerts[0].Annotations["summary"], "$value/$labels expanded through the real chain")
	assert.NotEmpty(t, p.Alerts[0].Fingerprint)

	// Resolve: the series flips to 0, so `up == 1` no longer matches; the alert
	// resolves and the notifier receives a resolved webhook.
	ps.swap(down)
	require.Eventually(t, func() bool {
		p, ok := notifier.latest()
		return ok && p.Status == "resolved"
	}, 10*time.Second, 50*time.Millisecond)

	p, _ = notifier.latest()
	require.Len(t, p.Alerts, 1)
	assert.Equal(t, "resolved", p.Alerts[0].Status)
	assert.NotEmpty(t, p.Alerts[0].EndsAt)
}
