//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/elk-utilities/prism/internal/store/engine"
	storeingest "github.com/elk-utilities/prism/internal/store/ingest"
	"github.com/elk-utilities/prism/internal/store/lifecycle"
	"github.com/elk-utilities/prism/internal/store/query"
)

const quickLogsTenant = "default"

type sqlAPIResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// TestQuickLogsEndToEnd drives the full logs path: real `prism run --quick logs`
// subprocess reads log lines on stdin, ships logs-summary parquet to an
// in-process prism-store, which buffers them as landing files, refreshes them
// into a searchable tier on the merge tick, and answers the advertised
// template→count query over the `logs` relation on /sql.
func TestQuickLogsEndToEnd(t *testing.T) {
	dataDir := t.TempDir()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, nil)
	t.Cleanup(func() { require.NoError(t, eng.Close()) })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ingestCfg := storeingest.Config{
		AllowedArtifacts: []string{"metrics-raw", "logs-summary"},
		MaxBodyBytes:     1 << 20,
		AuthMode:         storeingest.AuthNone,
	}
	sqlCfg := &query.SQLConfig{
		DataDir:      dataDir,
		MaxRows:      100_000,
		Timeout:      30 * time.Second,
		MaxBodyBytes: query.DefaultSQLMaxBodyBytes,
		RunJobs:      true,
	}
	mux := http.NewServeMux()
	mux.Handle(storeingest.IngestRoutePattern(""), storeingest.Handler(&ingestCfg, eng, logger))
	mux.Handle(query.SQLRoutePattern(""), query.SQLHandler(sqlCfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// Four lines collapse to two templates: the login line (x3) and the disk line (x1).
	logs := strings.Join([]string{
		"user 42 logged in from 10.0.0.1",
		"user 99 logged in from 10.0.0.9",
		"disk full on /dev/sda1",
		"user 7 logged in from 10.0.0.3",
	}, "\n") + "\n"

	runQuickLogsAgent(t, srv.URL, quickLogsTenant, logs)

	// The landed window is a non-searchable buffer until a refresh packs it into
	// a tier; a merge tick past the refresh interval opens it.
	refreshed := time.Now().Add(2 * time.Minute)
	runner := lifecycle.NewRunner(&lifecycle.Config{
		DataDir:             dataDir,
		SegmentsPerTier:     6,
		MaxSegmentBytes:     1 << 30,
		FloorBytes:          1 << 20,
		MaxTier:             8,
		LogsRefreshInterval: time.Minute,
		Logger:              logger,
	}, eng, func() time.Time { return refreshed })
	require.NoError(t, runner.TickMerge())

	// The store now holds a refreshed logs-summary segment; query it back.
	counts := queryTemplateCounts(t, srv.URL, quickLogsTenant)
	loginTemplate := ""
	for tmpl := range counts {
		if strings.Contains(tmpl, "logged in from") {
			loginTemplate = tmpl
		}
	}
	require.NotEmpty(t, loginTemplate, "no login template returned: %v", counts)
	require.Equal(t, int64(3), counts[loginTemplate], "login template count: %v", counts)

	var total int64
	for _, c := range counts {
		total += c
	}
	require.Equal(t, int64(4), total, "total counted lines across templates: %v", counts)
}

// runQuickLogsAgent runs the real agent binary via `go run`, feeding logs on
// stdin, and waits for it to drain and exit.
func runQuickLogsAgent(t *testing.T, store, tenant, logs string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/prism",
		"run", "--quick", "logs", "--store", store, "--tenant", tenant)
	cmd.Dir = "../.." // module root, so ./cmd/prism resolves
	cmd.Stdin = strings.NewReader(logs)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = io.Discard
	if err := cmd.Run(); err != nil {
		t.Fatalf("agent run: %v\nstderr:\n%s", err, stderr.String())
	}
}

func queryTemplateCounts(t *testing.T, base, tenant string) map[string]int64 {
	t.Helper()
	body := fmt.Sprintf(`{"sql":%q}`,
		"SELECT template, CAST(sum(count) AS BIGINT) AS count FROM logs GROUP BY template ORDER BY count DESC")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		base+"/"+tenant+"/sql", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "sql body: %s", raw)

	var out sqlAPIResponse
	require.NoError(t, json.Unmarshal(raw, &out))
	counts := make(map[string]int64, len(out.Rows))
	for _, row := range out.Rows {
		tmpl, _ := row[0].(string)
		counts[tmpl] = cellToInt64(t, row[1])
	}
	return counts
}

func cellToInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		i, err := n.Int64()
		require.NoError(t, err)
		return i
	case string:
		var i int64
		_, err := fmt.Sscan(n, &i)
		require.NoError(t, err)
		return i
	default:
		t.Fatalf("unexpected count cell type %T (%v)", v, v)
		return 0
	}
}
