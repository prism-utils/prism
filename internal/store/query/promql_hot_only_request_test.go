package query_test

import (
	"net/http"
	"path/filepath"
	"testing"

	"github.com/elk-utilities/prism/internal/store/query"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

// seedHotAndCold writes one `up` series into the hot snapshot (instance="hot")
// and a different `up` series into a cold L0 tier segment (instance="cold"), so
// a full query sees both and a hot-only query sees only the hot one.
func seedHotAndCold(t *testing.T, dataDir string) {
	t.Helper()
	testparquet.WriteSegmentRows(t, filepath.Join(dataDir, promTenant, "hot", "current.parquet"),
		[]testparquet.SegRow{{Name: "up", Labels: `job="api",instance="hot"`, Value: 1, Ts: promBase}})
	testparquet.WriteSegmentRows(t, filepath.Join(dataDir, promTenant, "tiers", "L0", "seg.parquet"),
		[]testparquet.SegRow{{Name: "up", Labels: `job="api",instance="cold"`, Value: 1, Ts: promBase}})
}

type promSeries struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

func instantSeries(t *testing.T, srv, query string) []promSeries {
	t.Helper()
	status, env := promGet(t, srv+"/"+promTenant+"/api/v1/query?"+query)
	if status != http.StatusOK || env.Status != "success" {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	var data promQueryData
	mustJSON(t, env.Data, &data)
	var result []promSeries
	mustJSON(t, data.Result, &result)
	return result
}

// TestPromQLInstantHotOnlyRequestParam proves the `hot_only` request extension
// tightens a globally-full store to the hot snapshot only, per request.
func TestPromQLInstantHotOnlyRequestParam(t *testing.T) {
	dataDir := t.TempDir()
	seedHotAndCold(t, dataDir)
	srv := promServer(t, promConfig(dataDir)) // HotOnly=false globally

	full := instantSeries(t, srv.URL, "query=up&time="+unixStr(promBase))
	if len(full) != 2 {
		t.Fatalf("full query want 2 series (hot+cold), got %d: %v", len(full), full)
	}

	hot := instantSeries(t, srv.URL, "query=up&time="+unixStr(promBase)+"&hot_only=true")
	if len(hot) != 1 || hot[0].Metric["instance"] != "hot" {
		t.Fatalf("hot_only query want only instance=hot, got %v", hot)
	}
}

// TestPromQLHotOnlyGlobalCannotBeWidened proves a request cannot loosen a
// globally hot-only store: hot_only=false is ignored when the store enforces it.
func TestPromQLHotOnlyGlobalCannotBeWidened(t *testing.T) {
	dataDir := t.TempDir()
	seedHotAndCold(t, dataDir)
	srv := promServer(t, promConfig(dataDir, func(c *query.PromQLConfig) { c.HotOnly = true }))

	got := instantSeries(t, srv.URL, "query=up&time="+unixStr(promBase)+"&hot_only=false")
	if len(got) != 1 || got[0].Metric["instance"] != "hot" {
		t.Fatalf("global hot-only must ignore hot_only=false, got %v", got)
	}
}
