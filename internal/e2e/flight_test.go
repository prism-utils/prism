// Package e2e — Arrow Flight transport round-trip: a pipeline encodes windows
// as Arrow IPC and ships them via the flight output (DoPut) to a `prism collect`
// Flight receiver, which persists them as time-range-named Parquet. This proves
// the columnar wire path end to end without a row-by-row re-parse on the server.
package e2e

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/collect"
	"github.com/elk-utilities/prism/internal/component"
	"github.com/elk-utilities/prism/internal/components"
	"github.com/elk-utilities/prism/internal/config"
	"github.com/elk-utilities/prism/internal/obs"
	"github.com/elk-utilities/prism/internal/pipeline"
)

const flightConfig = `
pipelines:
  - name: metrics
    input:
      type: prometheus
      options: { targets: ["${PRISM_METRICS_URL}"], interval: "40ms" }
    parser: { type: prometheus }
    buffer: { max_rows: 1 }
    branches:
      - name: wire
        encoder: { type: arrow }
        output: { type: flight, options: { addr: "${PRISM_FLIGHT_ADDR}" } }
`

// TestE2E_FlightToParquet stands up the receiver, runs the pipeline against a
// fake Prometheus target, and asserts the Flight-delivered window lands as a
// range-named Parquet file with the scraped rows intact.
func TestE2E_FlightToParquet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(metricsExposition))
	}))
	defer srv.Close()

	ingest := t.TempDir()
	logger := obs.NewLogger(os.Stderr, 0)
	receiver, err := collect.NewServer(ingest, logger)
	if err != nil {
		t.Fatalf("collect.NewServer: %v", err)
	}
	recCtx, recCancel := context.WithCancel(context.Background())
	defer recCancel()
	boundCh := make(chan string, 1)
	recDone := make(chan error, 1)
	go func() {
		recDone <- receiver.Serve(recCtx, "127.0.0.1:0", func(bound string) { boundCh <- bound })
	}()

	var bound string
	select {
	case bound = <-boundCh:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not bind within timeout")
	}

	t.Setenv("PRISM_METRICS_URL", srv.URL)
	t.Setenv("PRISM_FLIGHT_ADDR", bound)

	cfg, err := config.LoadConfig(strings.NewReader(flightConfig))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	reg, err := components.Default()
	if err != nil {
		t.Fatalf("Default: %v", err)
	}
	set, err := pipeline.Build(cfg, reg, component.Settings{Logger: logger})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- set.Run(ctx, obs.NewHost(logger)) }()

	waitForFiles(t, ingest, ".parquet")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
	recCancel()
	select {
	case <-recDone:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not stop after cancel")
	}

	newest := newestFile(t, ingest, ".parquet")
	assertParquetRows(t, newest, 2)
	assertRangeName(t, newest, "metrics", "wire")
	if strings.Contains(filepath.Base(newest), "unknown") {
		t.Fatalf("descriptor provenance lost: %s", filepath.Base(newest))
	}
}
