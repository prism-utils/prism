package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/admin"
	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/seed"
	"github.com/prism-utils/prism/internal/store/stats"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const testTenant = "user-6f3a9c2b-apps"

func testAdminConfig(dataDir string) *admin.Config {
	return &admin.Config{
		DataDir:          dataDir,
		AllowedArtifacts: []string{"metrics-raw"},
		// Writer default matches process RUN_JOBS=true; replica tests set false.
		RunJobs: true,
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
	mux.Handle(admin.StatsRoutePattern(), admin.WithBearerAuth(cfg.AdminToken, admin.StatsHandler(cfg, eng)))
	return mux
}

func doAdminReq(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func postEnsure(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	return doAdminReq(t, req)
}

func TestEnsureUnknownTenant404(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := postEnsure(t, srv.URL+"/admin/tenants/not valid!/ensure")
	defer func() { _ = resp.Body.Close() }()
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
		resp := postEnsure(t, url)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("pass %d: want 204, got %d", i, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	seedPath := dir + "/" + testTenant + "/metrics-raw/" + seed.SeedName
	info, err := os.Stat(seedPath)
	if err != nil {
		t.Fatalf("seed missing: %v", err)
	}
	firstSize := info.Size()

	resp := postEnsure(t, url)
	_ = resp.Body.Close()
	info2, _ := os.Stat(seedPath)
	if info2.Size() != firstSize {
		t.Fatalf("repeat ensure changed seed size")
	}
}

func TestEnsureFailure500WhenDataDirNotWritable(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	cfg := testAdminConfig(dir)
	cfg.RunJobs = true
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := postEnsure(t, srv.URL+"/admin/tenants/"+testTenant+"/ensure")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "ensure failed") {
		t.Fatalf("body = %q, want ensure failed", string(body))
	}
}

// A RUN_JOBS=false replica must not open or create engine.duckdb (shared RO
// mounts fail writable open). Ensure is a no-op 204 after tenant validation.
func TestEnsureRunJobsFalseNoOpOnReadOnlyDataDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	cfg := testAdminConfig(dir)
	cfg.RunJobs = false
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := postEnsure(t, srv.URL+"/admin/tenants/"+testTenant+"/ensure")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204 on jobs-off ensure, got %d", resp.StatusCode)
	}
	enginePath := dir + "/" + testTenant + "/engine.duckdb"
	if _, err := os.Stat(enginePath); !os.IsNotExist(err) {
		t.Fatalf("jobs-off ensure must not create %s (stat err=%v)", enginePath, err)
	}
}

func TestEnsureRunJobsFalseUnknownTenantStill404(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	cfg.RunJobs = false
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := postEnsure(t, srv.URL+"/admin/tenants/not valid!/ensure")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestStatsUnknownTenant404(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats?ns=not-valid!", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp := doAdminReq(t, req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestStatsGoldenJSONContract(t *testing.T) {
	empty := admin.StatsResponse{
		Artifacts: map[string]admin.ArtifactStats{
			"metrics-raw": {Windows: 0, LatestUnixNanos: 0},
		},
	}
	body, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"artifacts":{"metrics-raw":{"windows":0,"latestUnixNanos":0}},"totalWindows":0}`
	if string(body) != want {
		t.Fatalf("empty stats JSON mismatch\ngot:  %s\nwant: %s", body, want)
	}

	withMetering := admin.StatsResponse{
		Artifacts: map[string]admin.ArtifactStats{
			"metrics-raw": {Windows: 2, LatestUnixNanos: 1700000000000000000},
		},
		TotalWindows:         2,
		OnDiskBytes:          4096,
		CompactionCpuSeconds: 1.5,
	}
	body2, err := json.Marshal(withMetering)
	if err != nil {
		t.Fatal(err)
	}
	want2 := `{"artifacts":{"metrics-raw":{"windows":2,"latestUnixNanos":1700000000000000000}},"totalWindows":2,"onDiskBytes":4096,"compactionCpuSeconds":1.5}`
	if string(body2) != want2 {
		t.Fatalf("metered stats JSON mismatch\ngot:  %s\nwant: %s", body2, want2)
	}
}

func TestStatsAggregateGoldenViaHTTP(t *testing.T) {
	dir := t.TempDir()
	cfg := testAdminConfig(dir)
	eng := testEngine(t, dir)
	mux := testAdminMux(t, cfg, eng, "")

	srv := httptest.NewServer(mux)
	defer srv.Close()

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

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats?ns="+testTenant, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp := doAdminReq(t, req)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 without token, got %d", resp.StatusCode)
	}

	req2, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats?ns="+testTenant, nil)
	if err != nil {
		t.Fatal(err)
	}
	req2.Header.Set("Authorization", "Bearer wrong")
	resp2 := doAdminReq(t, req2)
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 with wrong token, got %d", resp2.StatusCode)
	}

	req3, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/stats?ns="+testTenant, nil)
	if err != nil {
		t.Fatal(err)
	}
	req3.Header.Set("Authorization", "Bearer s3cret")
	resp3 := doAdminReq(t, req3)
	_ = resp3.Body.Close()
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
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp := doAdminReq(t, req)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 from /stats, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return bytesTrimSpace(body)
}

func bytesTrimSpace(b []byte) []byte {
	return []byte(strings.TrimSpace(string(b)))
}

func decodeStats(t *testing.T, body []byte) admin.StatsResponse {
	t.Helper()
	var out admin.StatsResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v body=%s", err, strings.TrimSpace(string(body)))
	}
	return out
}
