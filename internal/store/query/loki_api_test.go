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
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const lokiTenant = "user-loki-3c1d"

// lokiBase anchors fixture file mtimes so timestamp assertions are exact.
var lokiBase = time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)

type lokiEnvelope struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
	Error  string          `json:"error"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

type lokiStreamsData struct {
	ResultType string       `json:"resultType"`
	Result     []lokiStream `json:"result"`
}

func lokiConfig(dataDir string, opts ...func(*query.LokiConfig)) *query.LokiConfig {
	cfg := &query.LokiConfig{
		DataDir:    dataDir,
		MaxEntries: 5000,
		Timeout:    30 * time.Second,
	}
	for _, o := range opts {
		o(cfg)
	}
	return cfg
}

func lokiServer(t *testing.T, cfg *query.LokiConfig) *httptest.Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := query.LokiHandler(cfg, logger)
	mux := http.NewServeMux()
	for _, p := range query.LokiRoutePatterns("") {
		mux.Handle(p, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// landLokiRaw writes a logs-raw window for the tenant and stamps its mtime, which
// is the ingest-time axis the Loki API reports.
func landLokiRaw(t *testing.T, dataDir, tenant, name string, at time.Time, rows []testparquet.LogRow) {
	t.Helper()
	path := filepath.Join(dataDir, tenant, "logs", "logs-raw", "tiers", "L0", name)
	testparquet.WriteLogsRawFile(t, path, rows)
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
}

func landLokiSummary(t *testing.T, dataDir, tenant, name string, at time.Time, rows []testparquet.LogSummaryRow) {
	t.Helper()
	path := filepath.Join(dataDir, tenant, "logs", "logs-summary", "tiers", "L0", name)
	testparquet.WriteLogsSummaryFile(t, path, rows)
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
}

// seedLokiLogs lands two raw windows an hour apart: the older one carries a
// plain line, the newer one a json-shaped line.
func seedLokiLogs(t *testing.T, dataDir string) {
	t.Helper()
	landLokiRaw(t, dataDir, lokiTenant, "old.parquet", lokiBase, []testparquet.LogRow{
		{Message: "disk full on /dev/sda1", Format: "none"},
	})
	landLokiRaw(t, dataDir, lokiTenant, "new.parquet", lokiBase.Add(time.Hour), []testparquet.LogRow{
		{Message: "user 42 logged in", Format: "json"},
	})
}

func nsStr(ts time.Time) string { return strconv.FormatInt(ts.UnixNano(), 10) }

// lokiRangeURL builds a query_range URL covering the seeded fixture window.
func lokiRangeURL(base, tenant, expr string, extra ...string) string {
	u := base + "/" + tenant + "/loki/api/v1/query_range" +
		"?query=" + url.QueryEscape(expr) +
		"&start=" + nsStr(lokiBase.Add(-time.Hour)) +
		"&end=" + nsStr(lokiBase.Add(2*time.Hour))
	for _, e := range extra {
		u += "&" + e
	}
	return u
}

func lokiGet(t *testing.T, u string) (int, lokiEnvelope) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return lokiDo(t, req)
}

func lokiPostForm(t *testing.T, u string, form url.Values) (int, lokiEnvelope) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, u, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return lokiDo(t, req)
}

func lokiDo(t *testing.T, req *http.Request) (int, lokiEnvelope) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var env lokiEnvelope
	if len(body) > 0 {
		_ = json.Unmarshal(body, &env)
	}
	return resp.StatusCode, env
}

func lokiStreams(t *testing.T, env lokiEnvelope) []lokiStream {
	t.Helper()
	var data lokiStreamsData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("decode data %s: %v", env.Data, err)
	}
	if data.ResultType != "streams" {
		t.Fatalf("resultType = %q, want streams", data.ResultType)
	}
	return data.Result
}

// lokiLines flattens every stream's values into (line, ts) pairs in response order.
func lokiLines(t *testing.T, env lokiEnvelope) []string {
	t.Helper()
	var out []string
	for _, s := range lokiStreams(t, env) {
		for _, v := range s.Values {
			out = append(out, v[1])
		}
	}
	return out
}

func TestLokiQueryRangeStreams(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))

	status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, `{job="prism"}`))
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	streams := lokiStreams(t, env)
	if len(streams) != 2 {
		t.Fatalf("want 2 streams (one per format), got %d: %+v", len(streams), streams)
	}
	byFormat := map[string]lokiStream{}
	for _, s := range streams {
		if s.Stream["job"] != "prism" {
			t.Fatalf("every stream must carry job=prism: %v", s.Stream)
		}
		byFormat[s.Stream["format"]] = s
	}
	jsonStream, ok := byFormat["json"]
	if !ok {
		t.Fatalf("no json stream in %+v", streams)
	}
	if len(jsonStream.Values) != 1 {
		t.Fatalf("json stream values = %v", jsonStream.Values)
	}
	// Timestamps come from the landing file mtime, in nanoseconds, as a string.
	if got, want := jsonStream.Values[0][0], nsStr(lokiBase.Add(time.Hour)); got != want {
		t.Fatalf("ts = %q, want %q (landing file mtime)", got, want)
	}
	if got := jsonStream.Values[0][1]; got != "user 42 logged in" {
		t.Fatalf("line = %q", got)
	}
	// message must not leak into the stream labels — it is the log line.
	if _, dup := jsonStream.Stream["message"]; dup {
		t.Fatalf("message must not be a stream label: %v", jsonStream.Stream)
	}
}

func TestLokiQueryRangeMatchAllEmptyQuery(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))

	for _, expr := range []string{"", "{}"} {
		status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, expr))
		if status != http.StatusOK || env.Status != "success" {
			t.Fatalf("expr %q: status=%d env=%+v", expr, status, env)
		}
		if got := len(lokiLines(t, env)); got != 2 {
			t.Fatalf("expr %q: lines = %d, want 2", expr, got)
		}
	}
}

func TestLokiQueryRangeLabelMatchers(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))

	cases := []struct {
		name  string
		expr  string
		lines []string
	}{
		{"equal", `{format="json"}`, []string{"user 42 logged in"}},
		{"not_equal", `{format!="json"}`, []string{"disk full on /dev/sda1"}},
		{"regex", `{format=~"js.n"}`, []string{"user 42 logged in"}},
		{"not_regex", `{format!~"js.n"}`, []string{"disk full on /dev/sda1"}},
		{"job_synthetic", `{job="prism", format="none"}`, []string{"disk full on /dev/sda1"}},
		{"job_mismatch_is_empty", `{job="other"}`, nil},
		{"absent_label_equal_is_empty", `{nosuchlabel="x"}`, nil},
		{"absent_label_not_equal_matches_all", `{nosuchlabel!="x"}`, []string{"user 42 logged in", "disk full on /dev/sda1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, tc.expr))
			if status != http.StatusOK || env.Status != "success" {
				t.Fatalf("status=%d env=%+v", status, env)
			}
			got := lokiLines(t, env)
			if len(got) != len(tc.lines) {
				t.Fatalf("lines = %v, want %v", got, tc.lines)
			}
			for i := range tc.lines {
				if got[i] != tc.lines[i] {
					t.Fatalf("lines = %v, want %v", got, tc.lines)
				}
			}
		})
	}
}

func TestLokiQueryRangeLineFilters(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))

	cases := []struct {
		name  string
		expr  string
		lines []string
	}{
		{"contains", `{} |= "disk"`, []string{"disk full on /dev/sda1"}},
		{"not_contains", `{} != "disk"`, []string{"user 42 logged in"}},
		{"regex", `{} |~ "d.sk"`, []string{"disk full on /dev/sda1"}},
		{"not_regex", `{} !~ "d.sk"`, []string{"user 42 logged in"}},
		{"chained", `{} |= "disk" |= "sda1"`, []string{"disk full on /dev/sda1"}},
		{"chained_excludes", `{} |= "disk" != "sda1"`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, tc.expr))
			if status != http.StatusOK || env.Status != "success" {
				t.Fatalf("status=%d env=%+v", status, env)
			}
			got := lokiLines(t, env)
			if len(got) != len(tc.lines) {
				t.Fatalf("lines = %v, want %v", got, tc.lines)
			}
			for i := range tc.lines {
				if got[i] != tc.lines[i] {
					t.Fatalf("lines = %v, want %v", got, tc.lines)
				}
			}
		})
	}
}

func TestLokiQueryRangeDirectionAndLimit(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))

	// Default direction is backward: newest entry first.
	_, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, "", "limit=1"))
	if got := lokiLines(t, env); len(got) != 1 || got[0] != "user 42 logged in" {
		t.Fatalf("backward limit=1 = %v, want newest entry", got)
	}
	_, env = lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, "", "limit=1", "direction=forward"))
	if got := lokiLines(t, env); len(got) != 1 || got[0] != "disk full on /dev/sda1" {
		t.Fatalf("forward limit=1 = %v, want oldest entry", got)
	}
}

// TestLokiQueryRangeMaxEntriesCaps proves the server cap wins over a larger
// client limit, so one request cannot pull an unbounded number of lines.
func TestLokiQueryRangeMaxEntriesCaps(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir, func(c *query.LokiConfig) { c.MaxEntries = 1 }))
	_, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, "", "limit=1000"))
	if got := lokiLines(t, env); len(got) != 1 {
		t.Fatalf("lines = %v, want 1 (MaxEntries cap)", got)
	}
}

func TestLokiQueryRangeTimeRangeFilters(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))

	// A window that ends before the newer file was landed sees only the older one.
	u := srv.URL + "/" + lokiTenant + "/loki/api/v1/query_range?query=" + url.QueryEscape("{}") +
		"&start=" + nsStr(lokiBase.Add(-time.Minute)) + "&end=" + nsStr(lokiBase.Add(time.Minute))
	status, env := lokiGet(t, u)
	if status != http.StatusOK {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	if got := lokiLines(t, env); len(got) != 1 || got[0] != "disk full on /dev/sda1" {
		t.Fatalf("lines = %v, want only the older entry", got)
	}
}

// TestLokiQueryRangeDefaultTimeRange proves start/end are optional: the default
// window is the last hour, so a just-landed window is visible without params.
func TestLokiQueryRangeDefaultTimeRange(t *testing.T) {
	dataDir := t.TempDir()
	landLokiRaw(t, dataDir, lokiTenant, "recent.parquet", time.Now().Add(-time.Minute),
		[]testparquet.LogRow{{Message: "just now", Format: "none"}})
	srv := lokiServer(t, lokiConfig(dataDir))
	status, env := lokiGet(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/query_range")
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	if got := lokiLines(t, env); len(got) != 1 || got[0] != "just now" {
		t.Fatalf("lines = %v, want the recent entry", got)
	}
}

// TestLokiQueryRangeSummaryWindow covers the `--quick logs` shape: a summary
// window has no message, so the mined template is the line and count is a label.
func TestLokiQueryRangeSummaryWindow(t *testing.T) {
	dataDir := t.TempDir()
	landLokiSummary(t, dataDir, lokiTenant, "sum.parquet", lokiBase, []testparquet.LogSummaryRow{
		{Template: "user <NUM> logged in", Count: 3},
	})
	srv := lokiServer(t, lokiConfig(dataDir))
	status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, `{job="prism"}`))
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	streams := lokiStreams(t, env)
	if len(streams) != 1 {
		t.Fatalf("streams = %+v, want 1", streams)
	}
	if got := streams[0].Values[0][1]; got != "user <NUM> logged in" {
		t.Fatalf("line = %q, want the template text", got)
	}
	if got := streams[0].Stream["count"]; got != "3" {
		t.Fatalf("count label = %q, want \"3\"", got)
	}
	if got := streams[0].Stream["template"]; got != "user <NUM> logged in" {
		t.Fatalf("template label = %q", got)
	}
}

// TestLokiQueryRangeMixedSchemas proves raw and summary windows unify: files with
// different column sets answer one query (union_by_name NULL-fills).
func TestLokiQueryRangeMixedSchemas(t *testing.T) {
	dataDir := t.TempDir()
	landLokiRaw(t, dataDir, lokiTenant, "raw.parquet", lokiBase, []testparquet.LogRow{
		{Message: "hello world", Format: "none"},
	})
	landLokiSummary(t, dataDir, lokiTenant, "sum.parquet", lokiBase.Add(time.Minute),
		[]testparquet.LogSummaryRow{{Template: "hello <WORD>", Count: 1}})
	srv := lokiServer(t, lokiConfig(dataDir))
	_, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, ""))
	if got := len(lokiLines(t, env)); got != 2 {
		t.Fatalf("lines = %d, want 2 (raw + summary unified)", got)
	}
}

func TestLokiQueryRangeUnsupportedLogQL400(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))
	for _, expr := range []string{`rate({job="prism"}[5m])`, `count_over_time({job="prism"}[1m])`, `{job="prism"} | json`} {
		status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, expr))
		if status != http.StatusBadRequest || env.Status != "error" || env.Error == "" {
			t.Fatalf("expr %q: status=%d env=%+v, want 400 error", expr, status, env)
		}
	}
}

func TestLokiQueryRangeBadParams400(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))
	cases := map[string]string{
		"bad_limit":     "limit=-1",
		"limit_garbage": "limit=many",
		"bad_direction": "direction=sideways",
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, "", extra))
			if status != http.StatusBadRequest || env.Status != "error" {
				t.Fatalf("status=%d env=%+v", status, env)
			}
		})
	}
	u := srv.URL + "/" + lokiTenant + "/loki/api/v1/query_range?start=nope&end=nope"
	if status, env := lokiGet(t, u); status != http.StatusBadRequest || env.Status != "error" {
		t.Fatalf("bad time: status=%d env=%+v", status, env)
	}
	u = srv.URL + "/" + lokiTenant + "/loki/api/v1/query_range?start=" +
		nsStr(lokiBase.Add(time.Hour)) + "&end=" + nsStr(lokiBase)
	if status, env := lokiGet(t, u); status != http.StatusBadRequest || env.Status != "error" {
		t.Fatalf("end before start: status=%d env=%+v", status, env)
	}
}

// TestLokiEmptyTenantEmptySuccess pins the contract that a provisioned tenant
// with no landed logs answers with an empty result, not a 500.
func TestLokiEmptyTenantEmptySuccess(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, lokiTenant), 0o750); err != nil {
		t.Fatal(err)
	}
	srv := lokiServer(t, lokiConfig(dataDir))

	status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, `{job="prism"}`))
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("query_range status=%d env=%+v", status, env)
	}
	if streams := lokiStreams(t, env); len(streams) != 0 {
		t.Fatalf("streams = %+v, want empty", streams)
	}
	if !strings.Contains(string(env.Data), `"result":[]`) {
		t.Fatalf("empty result must serialize as [], got %s", env.Data)
	}

	status, env = lokiGet(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/labels")
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("labels status=%d env=%+v", status, env)
	}
	status, env = lokiGet(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/label/format/values")
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("label values status=%d env=%+v", status, env)
	}
}

func TestLokiUnknownTenant404(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))
	for _, path := range []string{
		"/user-loki-absent/loki/api/v1/query_range",
		"/user-loki-absent/loki/api/v1/labels",
		"/user-loki-absent/loki/api/v1/label/format/values",
	} {
		if status, _ := lokiGet(t, srv.URL+path); status != http.StatusNotFound {
			t.Fatalf("%s status=%d, want 404", path, status)
		}
	}
	if status, _ := lokiGet(t, srv.URL+"/INVALID!/loki/api/v1/query_range"); status != http.StatusNotFound {
		t.Fatal("malformed tenant must be 404")
	}
}

// TestLokiTenantIsolation proves one tenant never sees another tenant's lines.
func TestLokiTenantIsolation(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	const other = "user-loki-other"
	landLokiRaw(t, dataDir, other, "o.parquet", lokiBase, []testparquet.LogRow{
		{Message: "other tenant secret", Format: "none"},
	})
	srv := lokiServer(t, lokiConfig(dataDir))

	_, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, ""))
	for _, line := range lokiLines(t, env) {
		if line == "other tenant secret" {
			t.Fatalf("tenant %s leaked another tenant's line", lokiTenant)
		}
	}
	_, env = lokiGet(t, lokiRangeURL(srv.URL, other, ""))
	got := lokiLines(t, env)
	if len(got) != 1 || got[0] != "other tenant secret" {
		t.Fatalf("other tenant lines = %v", got)
	}
}

func TestLokiLabelNames(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	landLokiSummary(t, dataDir, lokiTenant, "sum.parquet", lokiBase, []testparquet.LogSummaryRow{
		{Template: "t", Count: 1},
	})
	srv := lokiServer(t, lokiConfig(dataDir))

	status, env := lokiGet(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/labels")
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	var names []string
	if err := json.Unmarshal(env.Data, &names); err != nil {
		t.Fatalf("decode %s: %v", env.Data, err)
	}
	want := map[string]bool{"job": true, "format": true, "template": true, "count": true}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	for w := range want {
		if !got[w] {
			t.Fatalf("missing label %q in %v", w, names)
		}
	}
	if got["message"] {
		t.Fatalf("message is the log line, not a label: %v", names)
	}
}

func TestLokiLabelValues(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))

	cases := map[string][]string{
		"job":         {"prism"},
		"format":      {"json", "none"},
		"nosuchlabel": {},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			status, env := lokiGet(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/label/"+name+"/values")
			if status != http.StatusOK || env.Status != "success" {
				t.Fatalf("status=%d env=%+v", status, env)
			}
			var vals []string
			if err := json.Unmarshal(env.Data, &vals); err != nil {
				t.Fatalf("decode %s: %v", env.Data, err)
			}
			if len(vals) != len(want) {
				t.Fatalf("values = %v, want %v", vals, want)
			}
			for i := range want {
				if vals[i] != want[i] {
					t.Fatalf("values = %v, want %v", vals, want)
				}
			}
		})
	}
}

// The label dropdown must offer only values query_range can answer for. A
// window still sitting in the landing buffer is not searchable, so its values
// stay out of the label API until a refresh opens them.
func TestLokiLabelValuesOmitsLandingBufferValues(t *testing.T) {
	dataDir := t.TempDir()
	landLokiRaw(t, dataDir, lokiTenant, "refreshed.parquet", lokiBase, []testparquet.LogRow{
		{Message: "refreshed line", Format: "refreshed-format"},
	})
	landing := filepath.Join(dataDir, lokiTenant, "logs", "logs-raw", "buffered.parquet")
	testparquet.WriteLogsRawFile(t, landing, []testparquet.LogRow{
		{Message: "buffered line", Format: "buffered-format"},
	})
	srv := lokiServer(t, lokiConfig(dataDir))

	status, env := lokiGet(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/label/format/values")
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	var vals []string
	if err := json.Unmarshal(env.Data, &vals); err != nil {
		t.Fatalf("decode %s: %v", env.Data, err)
	}
	if len(vals) != 1 || vals[0] != "refreshed-format" {
		t.Fatalf("label values = %v, want only the refreshed tier value", vals)
	}
}

func TestLokiLabelValuesInvalidName400(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))
	status, env := lokiGet(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/label/not-a-label/values")
	if status != http.StatusBadRequest || env.Status != "error" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
}

// TestLokiPOSTQueryRange covers the form-encoded POST variant Grafana uses for
// long queries.
func TestLokiPOSTQueryRange(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))

	form := url.Values{}
	form.Set("query", `{job="prism"}`)
	form.Set("start", nsStr(lokiBase.Add(-time.Hour)))
	form.Set("end", nsStr(lokiBase.Add(2*time.Hour)))
	status, env := lokiPostForm(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/query_range", form)
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	if got := len(lokiLines(t, env)); got != 2 {
		t.Fatalf("lines = %d, want 2", got)
	}
}

// TestLokiLabelsSelectorFilter proves the optional `query` selector narrows the
// label metadata endpoints the way Grafana expects.
func TestLokiLabelsSelectorFilter(t *testing.T) {
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)
	srv := lokiServer(t, lokiConfig(dataDir))
	u := srv.URL + "/" + lokiTenant + "/loki/api/v1/label/format/values?query=" + url.QueryEscape(`{format="json"}`)
	_, env := lokiGet(t, u)
	var vals []string
	if err := json.Unmarshal(env.Data, &vals); err != nil {
		t.Fatalf("decode %s: %v", env.Data, err)
	}
	if len(vals) != 1 || vals[0] != "json" {
		t.Fatalf("values = %v, want [json]", vals)
	}
}

// TestLokiK8sIdentityLabels proves namespace/pod/container columns are exposed as
// stream labels and LogQL matchers can scope like kubectl logs -n/-c.
func TestLokiK8sIdentityLabels(t *testing.T) {
	dataDir := t.TempDir()
	landLokiRaw(t, dataDir, lokiTenant, "k8s.parquet", lokiBase, []testparquet.LogRow{
		{
			Message:   "cache miss",
			Format:    "k8s",
			Namespace: "user-fknjdouh-apps",
			Pod:       "prism-cache-abc",
			Container: "store",
		},
		{
			Message:   "demo ready",
			Format:    "k8s",
			Namespace: "live-demo",
			Pod:       "demo-prism-store-0",
			Container: "demo-prism-store",
		},
	})
	srv := lokiServer(t, lokiConfig(dataDir))

	status, env := lokiGet(t, srv.URL+"/"+lokiTenant+"/loki/api/v1/labels")
	if status != http.StatusOK {
		t.Fatalf("labels status=%d", status)
	}
	var names []string
	if err := json.Unmarshal(env.Data, &names); err != nil {
		t.Fatalf("decode labels: %v", err)
	}
	for _, want := range []string{"namespace", "pod", "container", "job"} {
		found := false
		for _, n := range names {
			if n == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing label %q in %v", want, names)
		}
	}

	status, env = lokiGet(t, lokiRangeURL(srv.URL, lokiTenant,
		`{namespace="user-fknjdouh-apps", pod="prism-cache-abc", container="store"}`))
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("query status=%d env=%+v", status, env)
	}
	got := lokiLines(t, env)
	if len(got) != 1 || got[0] != "cache miss" {
		t.Fatalf("filtered lines = %v, want [cache miss]", got)
	}
	streams := lokiStreams(t, env)
	if len(streams) != 1 {
		t.Fatalf("streams = %+v, want 1", streams)
	}
	s := streams[0].Stream
	if s["namespace"] != "user-fknjdouh-apps" || s["pod"] != "prism-cache-abc" || s["container"] != "store" {
		t.Fatalf("stream labels = %v", s)
	}

	status, env = lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, `{namespace="live-demo"}`))
	if status != http.StatusOK {
		t.Fatalf("live-demo status=%d", status)
	}
	got = lokiLines(t, env)
	if len(got) != 1 || got[0] != "demo ready" {
		t.Fatalf("live-demo lines = %v", got)
	}
}
