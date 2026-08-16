package query_test

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/metricsmeta"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

// TestPromQLCountUpOneHourDoesNotSeeNonOverlappingPartitions seeds 7 hourly
// L0 files plus a hot snapshot and evaluates count(up) at hour 3. The matching
// partition must stay in the open set (count=1); path-level skip of the other
// six hours is asserted in the query package's open-set tests.
func TestPromQLCountUpOneHourDoesNotSeeNonOverlappingPartitions(t *testing.T) {
	dataDir := t.TempDir()
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	l0 := filepath.Join(dataDir, promTenant, "tiers", "L0")
	for i := 0; i < 7; i++ {
		ts := base.Add(time.Duration(i) * time.Hour)
		testparquet.WriteSegmentRows(t, filepath.Join(l0, string(rune('a'+i))+".parquet"), []testparquet.SegRow{
			{Name: "up", Labels: `job="api",day="` + ts.Format("02") + `"`, Value: 1, Ts: ts},
		})
	}
	hotTs := base.Add(7 * time.Hour)
	testparquet.WriteSegmentRows(t, filepath.Join(dataDir, promTenant, "hot", "current.parquet"), []testparquet.SegRow{
		{Name: "up", Labels: `job="api",day="hot"`, Value: 1, Ts: hotTs},
	})
	if err := metricsmeta.SyncManifest(dataDir, promTenant); err != nil {
		t.Fatalf("SyncManifest: %v", err)
	}

	srv := promServer(t, promConfig(dataDir))
	at := base.Add(3 * time.Hour)
	status, env := promGet(t, srv.URL+"/"+promTenant+"/api/v1/query?query="+urlq(`count(up)`)+"&time="+unixStr(at))
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	var data promQueryData
	mustJSON(t, env.Data, &data)
	var result []promSeries
	mustJSON(t, data.Result, &result)
	if len(result) != 1 {
		t.Fatalf("want 1 series, got %d: %v", len(result), result)
	}
	if result[0].Value[1] != "1" {
		t.Fatalf("count(up)=%v, want 1 (open-set prune must not load the other 6 hours)", result[0].Value[1])
	}
}
