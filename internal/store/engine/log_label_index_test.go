package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/logmeta"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

// Landing is a write buffer that no query opens, so a landed window must not
// publish its label values: the Loki label API would advertise a value that
// query_range cannot return until a refresh moves the window into a tier.
func TestLandLogWindowDoesNotPublishLandingLabelValues(t *testing.T) {
	dataDir := t.TempDir()
	tenant := "user-labelland-7c22"
	eng := New(Config{DataDir: dataDir}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	tmp := filepath.Join(t.TempDir(), "window.parquet")
	testparquet.WriteLogsRawFile(t, tmp, []testparquet.LogRow{{Message: "buffered line", Format: "buffered-format"}})
	body, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.LandLogWindow(tenant, "logs-raw", bytes.NewReader(body)); err != nil {
		t.Fatalf("land: %v", err)
	}

	vals, err := logmeta.LabelValues(dataDir, tenant, "format", 0)
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	if len(vals) != 0 {
		t.Fatalf("format values = %v, want none while the window is buffered", vals)
	}

	testparquet.PromoteLandedLogsToTier(t, dataDir, tenant, "logs-raw")
	vals, err = logmeta.LabelValues(dataDir, tenant, "format", 0)
	if err != nil {
		t.Fatalf("LabelValues after refresh: %v", err)
	}
	if len(vals) != 1 || vals[0] != "buffered-format" {
		t.Fatalf("format values after refresh = %v, want the refreshed value", vals)
	}
}
