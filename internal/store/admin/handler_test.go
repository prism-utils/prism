package admin_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/admin"
	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/seed"
	"github.com/elk-utilities/prism/internal/store/stats"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const testTenant = "user-6f3a9c2b-apps"

func testAdminConfig(dataDir string) *admin.Config {
	return &admin.Config{
		DataDir:          dataDir,
		AllowedArtifacts: []string{"metrics-raw"},
	}
}

func testEngine(t *testing.T, dataDir string) *engine.Engine {
	t.Helper()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func testAdminMux(t *testing.T, cfg *admin.Config, eng *engine.Engine, token string) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	cfg.AdminToken = token
	mux.Handle(admin.EnsureRoutePattern(), admin.WithBearerAuth(cfg.AdminToken, admin.EnsureHandler(cfg, eng, logger)))
	mux.HandleFunc(admin.StatsRoutePattern(), admin.StatsHandler(cfg, eng))
	return mux
}

func TestEnsureUnknownTenant404(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/admin/tenants/not valid!/ensure", "application/json", nil)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestEnsureTenant204AndIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	url := srv.URL + "/admin/tenants/" + testTenant + "/ensure"
	for i := 0; i < 2; i++ {
		resp, err := http.Post(url, "application/json", nil)
		if err != nil {
			t.Fatalf("ensure pass %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("pass %d: want 204, got %d", i, resp.StatusCode)
		}
	}

	seedPath := dir + "/" + testTenant + "/metrics-raw/" + seed.SeedName
	info, err := os.Stat(seedPath)
	if err != nil {
		t.Fatalf("seed missing: %v", err)
	}
	firstSize := info.Size()

	resp, _ := http.Post(url, "application/json", nil)
	resp.Body.Close()
	info2, _ := os.Stat(seedPath)
	if info2.Size() != firstSize {
		t.Fatalf("repeat ensure changed seed size")
	}
}

func TestStatsUnknownTenant404(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/stats?ns=not-valid!")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestStatsGoldenJSONEmptyTenant(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := fetchStatsBody(t, srv.URL, testTenant)
	want := `{"artifacts":{"metrics-raw":{"windows":0,"latestUnixNanos":0}},"totalWindows":0}`
	if string(body) != want {
		t.Fatalf("tenant stats JSON mismatch\ngot:  %s\nwant: %s", body, want)
	}

	aggBody := fetchStatsBody(t, srv.URL, "")
	wantAgg := `{"artifacts":{"metrics-raw":{"windows":0,"latestUnixNanos":0}},"totalWindows":0}`
	if string(aggBody) != wantAgg {
		t.Fatalf("aggregate stats JSON mismatch\ngot:  %s\nwant: %s", aggBody, wantAgg)
	}
}

func TestStatsCountsLandedWindowsPerTenantExcludingSeed(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)

	if err := seed.EnsureMetricsRawSeedForTenant(dir, testTenant); err != nil {
		t.Fatalf("seed: %v", err)
	}

	mux := testAdminMux(t, cfg, eng, "")
	srv := httptest.NewServer(mux)
	defer srv.Close()

	if got := decodeStats(t, fetchStatsBody(t, srv.URL, testTenant)); got.TotalWindows != 0 {
		t.Fatalf("want 0 windows before ingest, got %d", got.TotalWindows)
	}

	parquetDir := t.TempDir()
	for i := 0; i < 2; i++ {
		path := testparquet.WriteWindow(t, parquetDir, "w.parquet", []testparquet.Row{
			{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
		})
		f, _ := os.Open(path)
		if _, err := eng.Ingest(testTenant, f); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		_ = f.Close()
	}
	otherPath := testparquet.WriteWindow(t, parquetDir, "other.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	f, _ := os.Open(otherPath)
	if _, err := eng.Ingest("user-99999999-apps", f); err != nil {
		t.Fatalf("ingest other: %v", err)
	}
	_ = f.Close()

	got := decodeStats(t, fetchStatsBody(t, srv.URL, testTenant))
	if got.TotalWindows != 2 {
		t.Fatalf("want 2 total windows for tenant, got %d", got.TotalWindows)
	}
	if a := got.Artifacts["metrics-raw"]; a.Windows != 2 {
		t.Fatalf("want 2 metrics-raw windows, got %d", a.Windows)
	}

	agg := decodeStats(t, fetchStatsBody(t, srv.URL, ""))
	if agg.TotalWindows != 3 {
		t.Fatalf("want 3 total windows aggregated, got %d", agg.TotalWindows)
	}
}

func TestStatsOnDiskBytesAndCompactionWhenTenantSet(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)

	tenantRoot := dir + "/" + testTenant + "/hot"
	if err := os.MkdirAll(tenantRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tenantRoot+"/current.parquet", []byte("hot-bytes"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := stats.AddCompactionCPUSeconds(dir, testTenant, 1.5); err != nil {
		t.Fatal(err)
	}

	mux := testAdminMux(t, cfg, eng, "")
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := fetchStatsBody(t, srv.URL, testTenant)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["onDiskBytes"]; !ok {
		t.Fatal("onDiskBytes missing when ns set")
	}
	if _, ok := raw["compactionCpuSeconds"]; !ok {
		t.Fatal("compactionCpuSeconds missing when ns set")
	}

	aggBody := fetchStatsBody(t, srv.URL, "")
	var aggRaw map[string]json.RawMessage
	if err := json.Unmarshal(aggBody, &aggRaw); err != nil {
		t.Fatal(err)
	}
	if _, ok := aggRaw["onDiskBytes"]; ok {
		t.Fatal("onDiskBytes must be omitted without ns")
	}
	if _, ok := aggRaw["compactionCpuSeconds"]; ok {
		t.Fatal("compactionCpuSeconds must be omitted without ns")
	}
}

func TestAdminTokenEnforced(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "s3cret")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/stats?ns=" + testTenant)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 without token, got %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/stats?ns="+testTenant, nil)
	req.Header.Set("Authorization", "Bearer wrong")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 with wrong token, got %d", resp2.StatusCode)
	}

	req3, _ := http.NewRequest(http.MethodGet, srv.URL+"/stats?ns="+testTenant, nil)
	req3.Header.Set("Authorization", "Bearer s3cret")
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("want 200 with token, got %d", resp3.StatusCode)
	}
}

func fetchStatsBody(t *testing.T, base, ns string) []byte {
	t.Helper()
	u := base + "/stats"
	if ns != "" {
		u += "?ns=" + ns
	}
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("get /stats: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 from /stats, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func decodeStats(t *testing.T, body []byte) admin.StatsResponse {
	t.Helper()
	var out admin.StatsResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v body=%s", err, strings.TrimSpace(string(body)))
	}
	return out
}
