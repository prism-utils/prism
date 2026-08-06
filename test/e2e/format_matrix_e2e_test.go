//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/testparquet"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

const (
	formatComposeFile = "../../deploy/docker-compose.format-matrix.yml"
	formatStoreBase   = "http://127.0.0.1:19093"
	formatTenant      = "formate2e"
)

func TestFormatMatrixHotMergeCombos(t *testing.T) {
	requireDocker(t)
	combos := []struct{ hot, merge string }{
		{"parquet", "parquet"},
		{"duckdb", "parquet"},
		{"parquet", "duckdb"},
		{"duckdb", "duckdb"},
	}
	for _, c := range combos {
		c := c
		t.Run(fmt.Sprintf("hot=%s_merge=%s", c.hot, c.merge), func(t *testing.T) {
			formatComposeUp(t, c.hot, c.merge)
			t.Cleanup(func() { formatComposeDown(t) })

			ingestMetricsWindow(t)
			ingestLogsWindow(t)

			deadline := time.Now().Add(90 * time.Second)
			var metricsOK, logsOK bool
			for time.Now().Before(deadline) {
				if !metricsOK {
					if n := sqlCount(t, "SELECT COUNT(*) FROM metrics"); n > 0 {
						metricsOK = true
					}
				}
				if !logsOK {
					if n := sqlCount(t, "SELECT COUNT(*) FROM logs"); n > 0 {
						logsOK = true
					}
				}
				if metricsOK && logsOK {
					break
				}
				time.Sleep(2 * time.Second)
			}
			if !metricsOK || !logsOK {
				dumpFormatComposeLogs(t)
				t.Fatalf("queries never saw data (metricsOK=%v logsOK=%v)", metricsOK, logsOK)
			}

			// Force a second metrics window past the hot window so flush→L0 can
			// emit MERGE_SEGMENT_FORMAT (duckdb or parquet).
			time.Sleep(3 * time.Second)
			ingestMetricsWindow(t)
			time.Sleep(5 * time.Second)
			if n := sqlCount(t, "SELECT COUNT(*) FROM metrics"); n < 1 {
				t.Fatalf("metrics empty after second ingest")
			}
		})
	}
}

func formatComposeUp(t *testing.T, hot, merge string) {
	t.Helper()
	formatComposeDown(t)
	cmd := exec.Command("docker", "compose", "-f", formatComposeFile, "up", "-d", "--build", "--wait")
	cmd.Env = append(os.Environ(),
		"HOT_SEGMENT_FORMAT="+hot,
		"MERGE_SEGMENT_FORMAT="+merge,
	)
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		dumpFormatComposeLogs(t)
		formatComposeDown(t)
		t.Fatalf("compose up: %v", err)
	}
}

func formatComposeDown(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-f", formatComposeFile, "down", "-v", "--remove-orphans")
	_ = cmd.Run()
}

func dumpFormatComposeLogs(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "-f", formatComposeFile, "logs", "--no-color")
	out, _ := cmd.CombinedOutput()
	t.Logf("compose logs:\n%s", out)
}

func ingestMetricsWindow(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "m.parquet", []testparquet.Row{
		{Name: "up", Labels: `{"job":"format"}`, Value: 1, TimestampMs: time.Now().UnixMilli()},
	})
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	postIngest(t, "metrics-raw", body)
}

func ingestLogsWindow(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "l.parquet")
	writeE2ELogParquet(t, path, "format-matrix logged in")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	postIngest(t, "logs-raw", body)
}

func writeE2ELogParquet(t *testing.T, path, message string) {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf(
		`COPY (SELECT '%s' AS message, 'raw' AS format, 'prism' AS job) TO '%s' (FORMAT parquet)`,
		message, filepath.ToSlash(path),
	)
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}

func postIngest(t *testing.T, artifact string, body []byte) {
	t.Helper()
	url := formatStoreBase + "/" + formatTenant + "/ingest/" + artifact
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest %s status=%d body=%s", artifact, resp.StatusCode, b)
	}
}

func sqlCount(t *testing.T, sqlText string) int {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"sql": sqlText})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		formatStoreBase+"/"+formatTenant+"/sql", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
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
