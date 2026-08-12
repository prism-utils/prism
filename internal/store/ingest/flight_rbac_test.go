package ingest_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prism-utils/prism/internal/store/ingest"
)

func TestFlightKeepsBearerAuthWhenHTTPUsesAuthNone(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	httpCfg := testConfig("s3cret", ingest.AuthNone)
	flightCfg := testConfig("s3cret", ingest.AuthBearer)
	if httpCfg.AuthMode != ingest.AuthNone {
		t.Fatalf("http cfg auth = %q, want none", httpCfg.AuthMode)
	}
	if flightCfg.AuthMode != ingest.AuthBearer {
		t.Fatalf("flight cfg auth = %q, want bearer", flightCfg.AuthMode)
	}

	addr, eng := startFlightReceiver(t, &flightCfg)
	ipcBytes := metricsIPCBlock(t, mem)
	if err := doPutWindow(t, addr, "", testTenant, "metrics-raw", ipcBytes); err == nil {
		t.Fatal("DoPut without bearer token should fail when flight uses AuthBearer")
	}
	if c, _ := eng.HotRowCount(testTenant); c != 0 {
		t.Fatalf("hot rows = %d, want 0", c)
	}
	if err := doPutWindow(t, addr, "s3cret", testTenant, "metrics-raw", ipcBytes); err != nil {
		t.Fatalf("DoPut with bearer: %v", err)
	}
	if c, _ := eng.HotRowCount(testTenant); c != 1 {
		t.Fatalf("hot rows = %d, want 1", c)
	}
	_ = httpCfg
}
