package query_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

func mustJSON(t *testing.T, raw json.RawMessage, dst any) {
	t.Helper()
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
}

func urlq(expr string) string { return url.QueryEscape(expr) }

func mkTenantDir(dataDir, tenant string) error {
	return os.MkdirAll(filepath.Join(dataDir, tenant), 0o750)
}

const promTenant = "promql-tenant-a"

var promBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// seedPromMetrics writes a metrics tier segment with samples spread over time so
// range and rate queries have real series to evaluate.
func seedPromMetrics(t *testing.T, dataDir string) {
	t.Helper()
	var rows []testparquet.SegRow
	for i := 0; i < 4; i++ {
		ts := promBase.Add(time.Duration(i) * 15 * time.Second)
		rows = append(rows,
			testparquet.SegRow{Name: "up", Labels: `job="api",instance="a"`, Value: 1, Ts: ts},
			testparquet.SegRow{Name: "up", Labels: `job="api",instance="b"`, Value: 0, Ts: ts},
			testparquet.SegRow{Name: "http_requests_total", Labels: `job="api"`, Value: float64(i * 10), Ts: ts},
		)
	}
	path := filepath.Join(dataDir, promTenant, "tiers", "L0", "seg.parquet")
	testparquet.WriteSegmentRows(t, path, rows)
}

func promServer(t *testing.T, cfg *query.PromQLConfig) *httptest.Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := query.PromQLHandler(cfg, nil, logger)
	mux := http.NewServeMux()
	for _, p := range query.PromQLRoutePatterns("") {
		mux.Handle(p, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func promConfig(dataDir string, opts ...func(*query.PromQLConfig)) *query.PromQLConfig {
	cfg := &query.PromQLConfig{
		DataDir:       dataDir,
		MaxSamples:    50_000_000,
		Timeout:       30 * time.Second,
		LookbackDelta: 5 * time.Minute,
		MaxPoints:     11_000,
		RunJobs:       false, // replica semantics: serve from parquet, no engine needed
	}
	for _, o := range opts {
		o(cfg)
	}
	return cfg
}

type promEnvelope struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType"`
	Error     string          `json:"error"`
}

type promQueryData struct {
	ResultType string          `json:"resultType"`
	Result     json.RawMessage `json:"result"`
}

// promGet issues a GET and returns the HTTP status plus the decoded Prometheus
// envelope. It fully reads and closes the body, so callers hold no open response.
func promGet(t *testing.T, url string) (int, promEnvelope) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	var env promEnvelope
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if len(body) > 0 {
		_ = json.Unmarshal(body, &env)
	}
	return resp.StatusCode, env
}

func unixStr(ts time.Time) string { return strconv.FormatInt(ts.Unix(), 10) }

func TestPromQLInstantVector(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))

	end := promBase.Add(45 * time.Second)
	status, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query?query=up&time="+unixStr(end))
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	var data promQueryData
	mustJSON(t, env.Data, &data)
	if data.ResultType != "vector" {
		t.Fatalf("resultType = %q", data.ResultType)
	}
	var result []struct {
		Metric map[string]string `json:"metric"`
		Value  [2]any            `json:"value"`
	}
	mustJSON(t, data.Result, &result)
	if len(result) != 2 {
		t.Fatalf("want 2 series, got %d: %v", len(result), result)
	}
	for _, s := range result {
		if s.Metric["__name__"] != "up" || s.Metric["job"] != "api" {
			t.Fatalf("bad metric labels: %v", s.Metric)
		}
		if _, ok := s.Value[1].(string); !ok {
			t.Fatalf("value[1] must be a string, got %T", s.Value[1])
		}
	}
}

func TestPromQLInstantLabelMatcher(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))

	end := promBase.Add(45 * time.Second)
	_, env := promGet(t, srv.URL+"/"+promTenant+`/api/v1/query?query=`+urlq(`up{instance="a"}`)+"&time="+unixStr(end))
	var data promQueryData
	mustJSON(t, env.Data, &data)
	var result []struct {
		Metric map[string]string `json:"metric"`
		Value  [2]any            `json:"value"`
	}
	mustJSON(t, data.Result, &result)
	if len(result) != 1 || result[0].Metric["instance"] != "a" || result[0].Value[1] != "1" {
		t.Fatalf("matcher result wrong: %v", result)
	}
}

func TestPromQLScalar(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query?query="+urlq("1+1"))
	var data promQueryData
	mustJSON(t, env.Data, &data)
	if data.ResultType != "scalar" {
		t.Fatalf("resultType = %q", data.ResultType)
	}
	var scalar [2]any
	mustJSON(t, data.Result, &scalar)
	if scalar[1] != "2" {
		t.Fatalf("scalar = %v", scalar)
	}
}

func TestPromQLAggregationDropsName(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	end := promBase.Add(45 * time.Second)
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query?query="+urlq("sum(up)")+"&time="+unixStr(end))
	var data promQueryData
	mustJSON(t, env.Data, &data)
	var result []struct {
		Metric map[string]string `json:"metric"`
		Value  [2]any            `json:"value"`
	}
	mustJSON(t, data.Result, &result)
	if len(result) != 1 || result[0].Value[1] != "1" {
		t.Fatalf("sum(up) = %v", result)
	}
	if _, ok := result[0].Metric["__name__"]; ok {
		t.Fatalf("aggregation must drop __name__: %v", result[0].Metric)
	}
}

func TestPromQLRangeMatrix(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	start := unixStr(promBase)
	end := unixStr(promBase.Add(45 * time.Second))
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query_range?query=up&start="+start+"&end="+end+"&step=15s")
	var data promQueryData
	mustJSON(t, env.Data, &data)
	if data.ResultType != "matrix" {
		t.Fatalf("resultType = %q", data.ResultType)
	}
	var result []struct {
		Metric map[string]string `json:"metric"`
		Values [][2]any          `json:"values"`
	}
	mustJSON(t, data.Result, &result)
	if len(result) != 2 {
		t.Fatalf("want 2 series, got %d", len(result))
	}
	if len(result[0].Values) == 0 {
		t.Fatalf("series has no points")
	}
}

func TestPromQLRangeRate(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	start := unixStr(promBase)
	end := unixStr(promBase.Add(45 * time.Second))
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query_range?query="+urlq("rate(http_requests_total[1m])")+"&start="+start+"&end="+end+"&step=15s")
	if env.Status != "success" {
		t.Fatalf("env=%+v", env)
	}
	var data promQueryData
	mustJSON(t, env.Data, &data)
	if data.ResultType != "matrix" {
		t.Fatalf("resultType = %q", data.ResultType)
	}
}

func TestPromQLSeries(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/series?match[]=up")
	var series []map[string]string
	mustJSON(t, env.Data, &series)
	if len(series) != 2 {
		t.Fatalf("want 2 series, got %d: %v", len(series), series)
	}
}

func TestPromQLLabelNames(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/labels")
	var names []string
	mustJSON(t, env.Data, &names)
	want := map[string]bool{"__name__": true, "job": true, "instance": true}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	for w := range want {
		if !got[w] {
			t.Fatalf("missing label %q in %v", w, names)
		}
	}
}

func TestPromQLLabelValues(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))

	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/label/job/values")
	var vals []string
	mustJSON(t, env.Data, &vals)
	if len(vals) != 1 || vals[0] != "api" {
		t.Fatalf("job values = %v", vals)
	}

	_, env = promGet(t, srv.URL+"/"+promTenant+"/api/v1/label/__name__/values")
	mustJSON(t, env.Data, &vals)
	if len(vals) != 2 || vals[0] != "http_requests_total" || vals[1] != "up" {
		t.Fatalf("__name__ values = %v", vals)
	}
}

func TestPromQLBadExpr(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	status, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query?query="+urlq("sum("))
	if status != http.StatusBadRequest || env.Status != "error" || env.ErrorType != "bad_data" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
}

func TestPromQLMissingQuery(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	status, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query")
	if status != http.StatusBadRequest || env.ErrorType != "bad_data" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
}

func TestPromQLUnknownTenant(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	status, _ := promGet(t, srv.URL+"/promql-tenant-absent/api/v1/query?query=up")
	if status != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", status)
	}
}

func TestPromQLRangeExceedsMaxPoints(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir, func(c *query.PromQLConfig) { c.MaxPoints = 2 }))
	start := unixStr(promBase)
	end := unixStr(promBase.Add(45 * time.Second))
	status, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query_range?query=up&start="+start+"&end="+end+"&step=15s")
	if status != http.StatusBadRequest || env.ErrorType != "bad_data" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
}

func TestPromQLMaxSamplesExceeded(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir, func(c *query.PromQLConfig) { c.MaxSamples = 1 }))
	start := unixStr(promBase)
	end := unixStr(promBase.Add(45 * time.Second))
	status, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query_range?query=up&start="+start+"&end="+end+"&step=15s")
	if status != http.StatusUnprocessableEntity || env.ErrorType != "execution" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
}

func TestPromQLEmptyTenant(t *testing.T) {
	dataDir := t.TempDir()
	// Tenant dir exists but has no parquet — a valid query returns an empty vector.
	seedPromMetrics(t, dataDir)
	emptyDir := t.TempDir()
	if err := mkTenantDir(emptyDir, "promql-empty-x"); err != nil {
		t.Fatal(err)
	}
	srv := promServer(t, promConfig(emptyDir))
	status, env := promGet(t, srv.URL+"/promql-empty-x/api/v1/query?query=up")
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	var data promQueryData
	mustJSON(t, env.Data, &data)
	var result []any
	mustJSON(t, data.Result, &result)
	if len(result) != 0 {
		t.Fatalf("empty tenant should yield 0 series, got %d", len(result))
	}
}

// TestPromQLLabelValuesMultiMatchUnion pins the OR semantics of repeated match[]:
// two contradictory single-selectors must union, not intersect.
func TestPromQLLabelValuesMultiMatchUnion(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	u := srv.URL + "/" + promTenant + "/api/v1/label/instance/values" +
		"?match[]=" + urlq(`up{instance="a"}`) + "&match[]=" + urlq(`up{instance="b"}`)
	_, env := promGet(t, u)
	var vals []string
	mustJSON(t, env.Data, &vals)
	if len(vals) != 2 || vals[0] != "a" || vals[1] != "b" {
		t.Fatalf("union of match[] should yield [a b], got %v", vals)
	}
}

// TestPromQLLabelValuesEmptyValue proves a present-but-empty label value is
// returned (lbls.Get cannot distinguish absent from empty; Range can).
func TestPromQLLabelValuesEmptyValue(t *testing.T) {
	dataDir := t.TempDir()
	rows := []testparquet.SegRow{
		{Name: "up", Labels: `job="api",region=""`, Value: 1, Ts: promBase},
	}
	testparquet.WriteSegmentRows(t, filepath.Join(dataDir, promTenant, "tiers", "L0", "seg.parquet"), rows)
	srv := promServer(t, promConfig(dataDir))
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/label/region/values")
	var vals []string
	mustJSON(t, env.Data, &vals)
	if len(vals) != 1 || vals[0] != "" {
		t.Fatalf("empty label value should be returned, got %v", vals)
	}
}

// TestPromQLSeriesLimit checks that limit truncates the /series result.
func TestPromQLSeriesLimit(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/series?match[]="+urlq("up")+"&limit=1")
	var series []map[string]string
	mustJSON(t, env.Data, &series)
	if len(series) != 1 {
		t.Fatalf("limit=1 should yield 1 series, got %d: %v", len(series), series)
	}
}

// TestPromQLLabelValuesLimit checks that limit truncates label values.
func TestPromQLLabelValuesLimit(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	_, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/label/instance/values?limit=1")
	var vals []string
	mustJSON(t, env.Data, &vals)
	if len(vals) != 1 {
		t.Fatalf("limit=1 should yield 1 value, got %v", vals)
	}
}

// TestPromQLBadLimit rejects a negative limit as bad_data.
func TestPromQLBadLimit(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	srv := promServer(t, promConfig(dataDir))
	status, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/series?match[]=up&limit=-1")
	if status != http.StatusBadRequest || env.ErrorType != "bad_data" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
}

func serveProm(t *testing.T, h http.Handler, ctx context.Context, path, ns string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	req.SetPathValue("ns", ns)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPromQLClientCancelReturns499(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := query.PromQLHandler(promConfig(dataDir), nil, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := serveProm(t, h, ctx, "/"+promTenant+"/api/v1/query?query=up", promTenant)
	if rec.Code != 499 {
		t.Fatalf("status=%d want 499 body=%s", rec.Code, rec.Body.String())
	}
	var env promEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if env.ErrorType != "canceled" {
		t.Fatalf("errorType=%q want canceled env=%+v", env.ErrorType, env)
	}
}

func TestPromQLTimeoutStill503(t *testing.T) {
	dataDir := t.TempDir()
	seedPromMetrics(t, dataDir)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := query.PromQLHandler(promConfig(dataDir, func(c *query.PromQLConfig) {
		c.Timeout = time.Nanosecond
	}), nil, logger)

	rec := serveProm(t, h, context.Background(), "/"+promTenant+"/api/v1/query?query=up", promTenant)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	var env promEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body.String())
	}
	if env.ErrorType != "timeout" {
		t.Fatalf("errorType=%q want timeout env=%+v", env.ErrorType, env)
	}
}
