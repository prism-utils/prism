//go:build e2e

// Package e2e_test also covers the logs read path on the reader/writer topology:
// the prism agent tails a log file and ships logs windows to a **writer** store,
// while a separate **reader** store (background jobs off, hot-only queries, the
// writer's data dir mounted read-only) answers both `/sql FROM logs` and the
// Loki-compatible API over the very same files. Reproduce with `make loki-e2e`.
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	lokiComposeFile = "../../deploy/docker-compose.loki-e2e.yml"
	lokiWriterBase  = "http://127.0.0.1:19091"
	lokiReaderBase  = "http://127.0.0.1:19092"
	lokiE2ETenant   = "lokie2e"
)

type lokiE2EEnvelope struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error"`
}

type lokiE2ELabelEnvelope struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

type lokiE2ESQLResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

func TestLokiReaderEndToEnd(t *testing.T) {
	requireDocker(t)
	lokiComposeUp(t)

	// The agent batch-reads the mounted fixture and ships windows; wait until
	// the writer has landed log rows.
	if n := waitForLogRows(t, lokiWriterBase, 90*time.Second); n == 0 {
		dumpLokiComposeLogs(t)
		t.Fatal("writer never landed any log rows")
	}

	// Reader parity: logs are file-backed, so a RUN_JOBS=false /
	// QUERY_HOT_ONLY=true replica on a read-only mount serves them unchanged.
	readerRows := waitForLogRows(t, lokiReaderBase, 60*time.Second)
	if readerRows == 0 {
		dumpLokiComposeLogs(t)
		t.Fatal("reader served no rows from FROM logs")
	}
	t.Logf("reader sees %d log rows via /sql over the read-only mount", readerRows)

	// The same reader answers the Loki API Grafana speaks.
	streams := waitForLokiStreams(t, lokiReaderBase, `{job="prism"}`, 60*time.Second)
	if len(streams) == 0 {
		dumpLokiComposeLogs(t)
		t.Fatal("reader Loki query_range returned no streams")
	}
	var lines int
	for _, s := range streams {
		if s.Stream["job"] != "prism" {
			t.Fatalf("stream missing job=prism: %v", s.Stream)
		}
		lines += len(s.Values)
		for _, v := range s.Values {
			if v[0] == "" || v[1] == "" {
				t.Fatalf("empty (ts, line) pair in stream %v", s.Stream)
			}
		}
	}
	t.Logf("reader Loki API returned %d streams / %d entries", len(streams), lines)

	// A line filter must narrow the result rather than error out.
	filtered := waitForLokiStreams(t, lokiReaderBase, `{job="prism"} |= "logged in"`, 30*time.Second)
	if len(filtered) == 0 {
		dumpLokiComposeLogs(t)
		t.Fatal("line filter returned no streams")
	}

	// Metadata endpoints back Grafana's label browser.
	names := lokiLabelList(t, lokiReaderBase, "/loki/api/v1/labels")
	if !containsString(names, "job") {
		t.Fatalf("labels = %v, want job", names)
	}
	values := lokiLabelList(t, lokiReaderBase, "/loki/api/v1/label/job/values")
	if !containsString(values, "prism") {
		t.Fatalf("job values = %v, want prism", values)
	}

	// Metric LogQL is explicitly out of scope and must fail loudly, not silently.
	status, env := lokiE2EQuery(t, lokiReaderBase, `rate({job="prism"}[5m])`)
	if status != http.StatusBadRequest || env.Status != "error" {
		t.Fatalf("metric LogQL status=%d env=%+v, want 400 error", status, env)
	}
}

func lokiComposeUp(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-f", lokiComposeFile, "up", "-d", "--build", "--wait")
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		dumpLokiComposeLogs(t)
		lokiComposeDown(t)
		t.Fatalf("compose up: %v", err)
	}
	t.Cleanup(func() { lokiComposeDown(t) })
}

func lokiComposeDown(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-f", lokiComposeFile, "down", "-v")
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	_ = cmd.Run()
}

func dumpLokiComposeLogs(t *testing.T) {
	t.Helper()
	out, _ := exec.Command("docker", "compose", "-f", lokiComposeFile, "logs", "--no-color", "--tail", "80").CombinedOutput()
	t.Logf("compose logs:\n%s", out)
}

// waitForLogRows polls `SELECT count(*) FROM logs` on a store until it reports a
// non-zero count or the deadline passes.
func waitForLogRows(t *testing.T, base string, within time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if n := logRowCount(t, base); n > 0 {
			return n
		}
		time.Sleep(2 * time.Second)
	}
	return 0
}

func logRowCount(t *testing.T, base string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	body := `{"sql":"SELECT count(*) AS c FROM logs"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/"+lokiE2ETenant+"/sql", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	var out lokiE2ESQLResponse
	if json.Unmarshal(raw, &out) != nil || len(out.Rows) == 0 || len(out.Rows[0]) == 0 {
		return 0
	}
	switch n := out.Rows[0][0].(type) {
	case float64:
		return int64(n)
	case string:
		var v int64
		_, _ = fmt.Sscan(n, &v)
		return v
	default:
		return 0
	}
}

func waitForLokiStreams(t *testing.T, base, expr string, within time.Duration) []struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
} {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		status, env := lokiE2EQuery(t, base, expr)
		if status == http.StatusOK && env.Status == "success" && len(env.Data.Result) > 0 {
			if env.Data.ResultType != "streams" {
				t.Fatalf("resultType = %q, want streams", env.Data.ResultType)
			}
			return env.Data.Result
		}
		time.Sleep(2 * time.Second)
	}
	return nil
}

func lokiE2EQuery(t *testing.T, base, expr string) (int, lokiE2EEnvelope) {
	t.Helper()
	end := time.Now()
	start := end.Add(-time.Hour)
	path := fmt.Sprintf("/%s/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=100",
		lokiE2ETenant, url.QueryEscape(expr), start.UnixNano(), end.UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, lokiE2EEnvelope{}
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var env lokiE2EEnvelope
	_ = json.Unmarshal(raw, &env)
	return resp.StatusCode, env
}

func lokiLabelList(t *testing.T, base, path string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/"+lokiE2ETenant+path, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status = %d body=%s", path, resp.StatusCode, raw)
	}
	var env lokiE2ELabelEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode %s: %v body=%s", path, err, raw)
	}
	return env.Data
}

func containsString(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}
