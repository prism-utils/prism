//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

const (
	agentDuckComposeFile = "../../deploy/docker-compose.agent-duckdb-e2e.yml"
	agentDuckStorePort   = "19094"
	agentDuckStoreBase   = "http://127.0.0.1:" + agentDuckStorePort
	agentDuckTenant      = "agentduck"
)

// TestAgentDuckDBTransferIngest proves agent→store .duckdb ingest and one mixed
// hot/merge combo with a duckdb agent payload.
func TestAgentDuckDBTransferIngest(t *testing.T) {
	requireDocker(t)

	// Serial scenarios share host port 19094; tear down any leftover stacks from
	// interrupted runs before the first subtest binds the port.
	agentDuckCleanupLeftovers(t)

	t.Run("hot=parquet_merge=parquet", func(t *testing.T) {
		runAgentDuckDBScenario(t, "parquet", "parquet")
	})
	t.Run("hot=duckdb_merge=parquet", func(t *testing.T) {
		runAgentDuckDBScenario(t, "duckdb", "parquet")
	})
}

func agentDuckProject(hot, merge string) string {
	return fmt.Sprintf("agentduck-h-%s-m-%s", hot, merge)
}

func agentDuckCleanupLeftovers(t *testing.T) {
	t.Helper()
	for _, p := range []string{
		"agent-duckdb", // legacy fixed project name
		agentDuckProject("parquet", "parquet"),
		agentDuckProject("duckdb", "parquet"),
	} {
		agentDuckComposeDown(t, p)
	}
	agentDuckWaitPortFree(t)
}

func runAgentDuckDBScenario(t *testing.T, hot, merge string) {
	t.Helper()
	project := agentDuckProject(hot, merge)

	// Tear down this project (and wait for the shared host port) before up, and
	// again before the next sibling subtest — do not rely solely on t.Cleanup
	// ordering relative to compose's async port release.
	teardown := func() {
		agentDuckComposeDown(t, project)
		agentDuckWaitPortFree(t)
	}
	teardown()
	t.Cleanup(teardown)

	cmd := exec.Command("docker", "compose", "-p", project, "-f", agentDuckComposeFile, "up", "-d", "--build", "--wait")
	cmd.Env = append(os.Environ(),
		"HOT_SEGMENT_FORMAT="+hot,
		"MERGE_SEGMENT_FORMAT="+merge,
	)
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		agentDuckComposeLogs(t, project)
		teardown()
		t.Fatalf("compose up: %v", err)
	}

	deadline := time.Now().Add(3 * time.Minute)
	var metricsOK bool
	for time.Now().Before(deadline) {
		if n := agentDuckSQLCount(t, "SELECT COUNT(*) FROM metrics"); n > 0 {
			metricsOK = true
			break
		}
		time.Sleep(3 * time.Second)
	}
	if !metricsOK {
		agentDuckComposeLogs(t, project)
		t.Fatal("metrics never appeared after agent duckdb ship")
	}

	// Release port/containers before the next scenario starts (Cleanup also runs).
	teardown()
}

func agentDuckComposeDown(t *testing.T, project string) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", project, "-f", agentDuckComposeFile, "down", "-v", "--remove-orphans")
	_ = cmd.Run()
}

func agentDuckComposeLogs(t *testing.T, project string) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", project, "-f", agentDuckComposeFile, "logs", "--no-color")
	out, _ := cmd.CombinedOutput()
	t.Logf("compose logs (%s):\n%s", project, out)
}

// agentDuckWaitPortFree blocks until nothing is listening on the shared store
// publish port, so the next compose up does not race docker's port release.
func agentDuckWaitPortFree(t *testing.T) {
	t.Helper()
	addr := "127.0.0.1:" + agentDuckStorePort
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			_ = ln.Close()
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("host port %s still allocated after compose down", agentDuckStorePort)
}

func agentDuckSQLCount(t *testing.T, sqlText string) int {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"sql": sqlText})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		agentDuckStoreBase+"/"+agentDuckTenant+"/sql", bytes.NewReader(payload))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	var out struct {
		Rows [][]any `json:"rows"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0
	}
	if len(out.Rows) == 0 || len(out.Rows[0]) == 0 {
		return 0
	}
	switch v := out.Rows[0][0].(type) {
	case float64:
		return int(v)
	default:
		return 0
	}
}
