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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	agentDuckComposeFile = "../../deploy/docker-compose.agent-duckdb-e2e.yml"
	agentDuckTenant      = "agentduck"
	agentDuckBasePort    = 19094
)

// agentDuckRunID isolates compose projects from concurrent e2e processes.
var agentDuckRunID = fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000)

// agentDuckSerial serializes scenarios within one process.
var agentDuckSerial sync.Mutex

// TestAgentDuckDBTransferIngest proves agent→store .duckdb ingest and one mixed
// hot/merge combo with a duckdb agent payload.
func TestAgentDuckDBTransferIngest(t *testing.T) {
	requireDocker(t)
	agentDuckWithLock(t)

	agentDuckCleanupLeftovers(t)

	t.Run("hot=parquet_merge=parquet", func(t *testing.T) {
		runAgentDuckDBScenario(t, "parquet", "parquet", 0)
	})
	t.Run("hot=duckdb_merge=parquet", func(t *testing.T) {
		runAgentDuckDBScenario(t, "duckdb", "parquet", 1)
	})
}

func agentDuckProject(hot, merge string) string {
	return fmt.Sprintf("ad%s-h-%s-m-%s", agentDuckRunID, hot, merge)
}

// agentDuckWithLock holds an exclusive flock for the whole test so concurrent
// make agent-duckdb-e2e processes cannot collide on containers/ports.
func agentDuckWithLock(t *testing.T) {
	t.Helper()
	agentDuckSerial.Lock()
	t.Cleanup(agentDuckSerial.Unlock)

	lockPath := filepath.Join(os.TempDir(), "prism-agent-duckdb-e2e.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open e2e lock: %v", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		t.Fatalf("flock e2e lock: %v", err)
	}
	t.Cleanup(func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	})
}

func agentDuckCleanupLeftovers(t *testing.T) {
	t.Helper()
	for _, p := range []string{
		"agent-duckdb",
		"agent-duckdb-e2e",
		"deploy",
	} {
		agentDuckComposeDown(t, p)
	}
	out, _ := exec.Command("docker", "ps", "-a", "--format", "{{.Names}}").Output()
	for _, name := range strings.Split(string(out), "\n") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		switch {
		case strings.HasPrefix(name, "agent-duckdb"),
			strings.HasPrefix(name, "agentduck-"),
			strings.HasPrefix(name, "ad") && strings.Contains(name, "-h-") && strings.Contains(name, "-m-"),
			name == "deploy-prism-store-1",
			name == "deploy-prism-agent-1",
			name == "deploy-node-exporter-1":
			_ = exec.Command("docker", "rm", "-f", name).Run()
		}
	}
	nets, _ := exec.Command("docker", "network", "ls", "--format", "{{.Name}}").Output()
	for _, name := range strings.Split(string(nets), "\n") {
		name = strings.TrimSpace(name)
		if strings.HasPrefix(name, "agent-duckdb") || strings.HasPrefix(name, "agentduck-") ||
			(strings.HasPrefix(name, "ad") && strings.Contains(name, "-h-")) ||
			name == "deploy_default" {
			_ = exec.Command("docker", "network", "rm", name).Run()
		}
	}
	for _, port := range []int{agentDuckBasePort, agentDuckBasePort + 1} {
		agentDuckWaitPortFree(t, port)
	}
}

func runAgentDuckDBScenario(t *testing.T, hot, merge string, portOffset int) {
	t.Helper()
	project := agentDuckProject(hot, merge)
	hostPort := agentDuckBasePort + portOffset
	storeBase := fmt.Sprintf("http://127.0.0.1:%d", hostPort)

	teardown := func() {
		agentDuckComposeDown(t, project)
		agentDuckForceRemoveProject(t, project)
		agentDuckWaitPortFree(t, hostPort)
	}
	teardown()
	t.Cleanup(teardown)

	cmd := exec.Command("docker", "compose", "-p", project, "-f", agentDuckComposeFile, "up", "-d", "--build", "--wait")
	cmd.Env = append(os.Environ(),
		"HOT_SEGMENT_FORMAT="+hot,
		"MERGE_SEGMENT_FORMAT="+merge,
		"STORE_HOST_PORT="+strconv.Itoa(hostPort),
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
		if n := agentDuckSQLCount(t, storeBase, "SELECT COUNT(*) FROM metrics"); n > 0 {
			metricsOK = true
			break
		}
		time.Sleep(3 * time.Second)
	}
	if !metricsOK {
		agentDuckComposeLogs(t, project)
		t.Fatal("metrics never appeared after agent duckdb ship")
	}

	// Release before the next sibling subtest starts (Cleanup also runs).
	teardown()
}

func agentDuckComposeDown(t *testing.T, project string) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", project, "-f", agentDuckComposeFile, "down", "-v", "--remove-orphans")
	_ = cmd.Run()
}

func agentDuckForceRemoveProject(t *testing.T, project string) {
	t.Helper()
	for _, svc := range []string{"prism-store", "prism-agent", "node-exporter"} {
		_ = exec.Command("docker", "rm", "-f", project+"-"+svc+"-1").Run()
	}
	_ = exec.Command("docker", "network", "rm", project+"_default").Run()
}

func agentDuckComposeLogs(t *testing.T, project string) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-p", project, "-f", agentDuckComposeFile, "logs", "--no-color")
	out, _ := cmd.CombinedOutput()
	t.Logf("compose logs (%s):\n%s", project, out)
}

func agentDuckWaitPortFree(t *testing.T, port int) {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			_ = ln.Close()
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("host port %d still allocated after compose down", port)
}

func agentDuckSQLCount(t *testing.T, storeBase, sqlText string) int {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"sql": sqlText})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		storeBase+"/"+agentDuckTenant+"/sql", bytes.NewReader(payload))
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
