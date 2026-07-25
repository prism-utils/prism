//go:build e2e

// Package e2e_test drives the full PromQL path end to end: a real Prometheus
// exporter (node-exporter) is scraped by the prism agent, shipped to
// prism-store, and queried back over the Prometheus HTTP API. Everything runs in
// docker-compose so the test reproduces a production-shaped deployment.
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"testing"
	"time"
)

const (
	composeFile = "../../deploy/docker-compose.promql-e2e.yml"
	storeBase   = "http://127.0.0.1:19090"
	e2eTenant   = "promqle2e"
)

type e2eEnvelope struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

func TestPromQLEndToEnd(t *testing.T) {
	requireDocker(t)
	composeUp(t)

	// Wait until scraped node-exporter series are queryable via PromQL. The agent
	// scrapes every 2s and the store flushes every 2s, so a minute is generous.
	var seriesCount float64
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := instantScalar(t, `count({__name__=~"node_.+"})`); ok && v > 0 {
			seriesCount = v
			break
		}
		time.Sleep(2 * time.Second)
	}
	if seriesCount == 0 {
		dumpComposeLogs(t)
		t.Fatalf("no node_* series returned by PromQL within deadline")
	}
	t.Logf("PromQL sees %.0f node_* series scraped from the real exporter", seriesCount)

	// A pure scalar proves the engine evaluates arbitrary PromQL, not just selectors.
	if v, ok := instantScalar(t, "1+1"); !ok || v != 2 {
		t.Fatalf("scalar 1+1 = %v ok=%v, want 2", v, ok)
	}

	// node_cpu_seconds_total is a counter every node-exporter emits. rate() needs
	// at least two samples in the window, so poll until enough scrapes accumulate.
	rateExpr := "rate(node_cpu_seconds_total[1m])"
	rateDeadline := time.Now().Add(60 * time.Second)
	rateOK := false
	for time.Now().Before(rateDeadline) {
		if rangeMatrixNonEmpty(t, rateExpr) {
			rateOK = true
			break
		}
		time.Sleep(3 * time.Second)
	}
	if !rateOK {
		dumpComposeLogs(t)
		t.Fatalf("range %q never returned a non-empty matrix", rateExpr)
	}

	// The label API must expose the real exporter's label space.
	assertLabelName(t, "__name__")
	assertLabelValue(t, "__name__", "node_cpu_seconds_total")
}

func requireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Run(); err != nil {
		t.Skipf("docker not available: %v", err)
	}
}

func composeUp(t *testing.T) {
	t.Helper()
	// First run builds the agent + store images from source; allow time for it.
	cmd := exec.Command("docker", "compose", "-f", composeFile, "up", "-d", "--build", "--wait")
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		dumpComposeLogs(t)
		composeDown(t)
		t.Fatalf("compose up: %v", err)
	}
	t.Cleanup(func() { composeDown(t) })
}

func composeDown(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-f", composeFile, "down", "-v")
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	_ = cmd.Run()
}

func dumpComposeLogs(t *testing.T) {
	t.Helper()
	out, _ := exec.Command("docker", "compose", "-f", composeFile, "logs", "--no-color", "--tail", "80").CombinedOutput()
	t.Logf("compose logs:\n%s", out)
}

func queryPromQL(t *testing.T, path string) (*http.Response, e2eEnvelope) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, storeBase+path, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, e2eEnvelope{}
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var env e2eEnvelope
	_ = json.Unmarshal(body, &env)
	return resp, env
}

// instantScalar runs an instant query expected to yield a scalar or a single
// vector element and returns its numeric value.
func instantScalar(t *testing.T, expr string) (float64, bool) {
	t.Helper()
	resp, env := queryPromQL(t, "/"+e2eTenant+"/api/v1/query?query="+urlQuery(expr))
	if resp == nil || resp.StatusCode != http.StatusOK || env.Status != "success" {
		return 0, false
	}
	switch env.Data.ResultType {
	case "scalar":
		var pair [2]any
		if json.Unmarshal(env.Data.Result, &pair) == nil {
			return asFloat(pair[1]), true
		}
	case "vector":
		var vec []struct {
			Value [2]any `json:"value"`
		}
		if json.Unmarshal(env.Data.Result, &vec) == nil && len(vec) == 1 {
			return asFloat(vec[0].Value[1]), true
		}
	}
	return 0, false
}

// rangeMatrixNonEmpty runs a range query over the last ~2 minutes and reports
// whether it yielded at least one series with at least one point.
func rangeMatrixNonEmpty(t *testing.T, expr string) bool {
	t.Helper()
	end := time.Now().Unix()
	start := end - 120
	path := fmt.Sprintf("/%s/api/v1/query_range?query=%s&start=%d&end=%d&step=15s", e2eTenant, urlQuery(expr), start, end)
	resp, env := queryPromQL(t, path)
	if resp == nil || resp.StatusCode != http.StatusOK || env.Status != "success" || env.Data.ResultType != "matrix" {
		return false
	}
	var series []struct {
		Values [][2]any `json:"values"`
	}
	if err := json.Unmarshal(env.Data.Result, &series); err != nil {
		return false
	}
	for _, s := range series {
		if len(s.Values) > 0 {
			return true
		}
	}
	return false
}

// stringListData fetches an endpoint whose `data` is a JSON string array
// (/labels and /label/<name>/values) and returns the decoded list.
func stringListData(t *testing.T, path string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, storeBase+path, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status = %d body=%s", path, resp.StatusCode, body)
	}
	var env struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode %s: %v body=%s", path, err, body)
	}
	return env.Data
}

func assertLabelName(t *testing.T, want string) {
	t.Helper()
	names := stringListData(t, "/"+e2eTenant+"/api/v1/labels")
	for _, n := range names {
		if n == want {
			return
		}
	}
	t.Fatalf("label %q not present in %v", want, names)
}

func assertLabelValue(t *testing.T, name, want string) {
	t.Helper()
	values := stringListData(t, "/"+e2eTenant+"/api/v1/label/"+name+"/values")
	for _, v := range values {
		if v == want {
			return
		}
	}
	t.Fatalf("label %s value %q not present (got %d values)", name, want, len(values))
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case string:
		var f float64
		_, _ = fmt.Sscanf(x, "%g", &f)
		return f
	case float64:
		return x
	default:
		return 0
	}
}

func urlQuery(expr string) string {
	// Minimal escaping for the characters PromQL expressions use in these tests.
	replacer := map[rune]string{'+': "%2B", ' ': "%20", '{': "%7B", '}': "%7D",
		'"': "%22", '=': "%3D", '~': "%7E", '(': "%28", ')': "%29", '[': "%5B", ']': "%5D"}
	out := make([]byte, 0, len(expr)*2)
	for _, r := range expr {
		if s, ok := replacer[r]; ok {
			out = append(out, s...)
			continue
		}
		out = append(out, string(r)...)
	}
	return string(out)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}
