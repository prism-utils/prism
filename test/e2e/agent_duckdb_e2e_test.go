//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

const (
	agentDuckComposeFile    = "../../deploy/docker-compose.agent-duckdb-e2e.yml"
	agentDuckComposeProject = "agent-duckdb"
	agentDuckStoreBase      = "http://127.0.0.1:19094"
	agentDuckTenant         = "agentduck"
)

// TestAgentDuckDBTransferIngest proves agent→store .duckdb ingest and one mixed
// hot/merge combo with a duckdb agent payload.
func TestAgentDuckDBTransferIngest(t *testing.T) {
	requireDocker(t)

	t.Run("hot=parquet_merge=parquet", func(t *testing.T) {
		runAgentDuckDBScenario(t, "parquet", "parquet")
	})
	t.Run("hot=duckdb_merge=parquet", func(t *testing.T) {
		runAgentDuckDBScenario(t, "duckdb", "parquet")
	})
}

func runAgentDuckDBScenario(t *testing.T, hot, merge string) {
	t.Helper()
	agentDuckComposeDown(t)
	cmd := exec.Command("docker", "compose", "-p", agentDuckComposeProject, "-f", agentDuckComposeFile, "up", "-d", "--build", "--wait")
	cmd.Env = append(os.Environ(),
		"HOT_SEGMENT_FORMAT="+hot,
		"MERGE_SEGMENT_FORMAT="+merge,
	)
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		agentDuckComposeLogs(t)
		agentDuckComposeDown(t)
		t.Fatalf("compose up: %v", err)
	}
	t.Cleanup(func() { agentDuckComposeDown(t) })

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
		agentDuckComposeLogs(t)
		t.Fatal("metrics never appeared after agent duckdb ship")
	}
}

func agentDuckComposeDown(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", agentDuckComposeProject, "-f", agentDuckComposeFile, "down", "-v", "--remove-orphans")
	_ = cmd.Run()
}

func agentDuckComposeLogs(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", agentDuckComposeProject, "-f", agentDuckComposeFile, "logs", "--no-color")
	out, _ := cmd.CombinedOutput()
	t.Logf("compose logs:\n%s", out)
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
