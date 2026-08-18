package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/admin"
	"github.com/prism-utils/prism/internal/store/merge"
)

type fakeCompactor struct {
	policy     map[string]merge.CompactSpec
	plan       merge.MergeAction
	planOK     bool
	planErr    error
	enqueued   []merge.MergeAction
	lastTenant string
	lastSpec   merge.CompactSpec
}

func (f *fakeCompactor) PlanCompact(tenant string, spec merge.CompactSpec) (merge.MergeAction, bool, error) { //nolint:gocritic // matches Compactor
	f.lastTenant = tenant
	f.lastSpec = spec
	return f.plan, f.planOK, f.planErr
}

func (f *fakeCompactor) EnqueueCompact(tenant string, action merge.MergeAction) {
	f.lastTenant = tenant
	f.enqueued = append(f.enqueued, action)
}

func (f *fakeCompactor) CompactPolicy(name string) (merge.CompactSpec, bool) {
	if f.policy == nil {
		return merge.CompactSpec{}, false
	}
	s, ok := f.policy[name]
	return s, ok
}

func compactMux(t *testing.T, cfg *admin.Config, c admin.Compactor, token string) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	cfg.AdminToken = token
	mux.Handle(admin.CompactRoutePattern(), admin.WithBearerAuth(cfg.AdminToken, admin.CompactHandler(cfg, c, logger)))
	return mux
}

func postCompact(t *testing.T, url, body, bearer string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	return doAdminReq(t, req)
}

func TestCompactUnknownTenant404(t *testing.T) {
	cfg := testAdminConfig(t.TempDir())
	c := &fakeCompactor{}
	srv := httptest.NewServer(compactMux(t, cfg, c, ""))
	defer srv.Close()

	resp := postCompact(t, srv.URL+"/admin/tenants/not valid!/compact", `{"policy":"recent"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestCompactMissingWindow400(t *testing.T) {
	cfg := testAdminConfig(t.TempDir())
	c := &fakeCompactor{}
	srv := httptest.NewServer(compactMux(t, cfg, c, ""))
	defer srv.Close()

	resp := postCompact(t, srv.URL+"/admin/tenants/"+testTenant+"/compact", `{}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

func TestCompactUnknownPolicy400(t *testing.T) {
	cfg := testAdminConfig(t.TempDir())
	c := &fakeCompactor{policy: map[string]merge.CompactSpec{}}
	srv := httptest.NewServer(compactMux(t, cfg, c, ""))
	defer srv.Close()

	resp := postCompact(t, srv.URL+"/admin/tenants/"+testTenant+"/compact", `{"policy":"recent"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

func TestCompactDryRun200NoEnqueue(t *testing.T) {
	cfg := testAdminConfig(t.TempDir())
	c := &fakeCompactor{
		policy: map[string]merge.CompactSpec{"recent": merge.DefaultCatchupSpec()},
		plan: merge.MergeAction{
			Sources: []merge.Segment{
				{Path: "/data/t/tiers/L0/a.parquet", Bytes: 10},
				{Path: "/data/t/tiers/L0/b.parquet", Bytes: 20},
			},
			DestTier: 1,
		},
		planOK: true,
	}
	srv := httptest.NewServer(compactMux(t, cfg, c, ""))
	defer srv.Close()

	resp := postCompact(t, srv.URL+"/admin/tenants/"+testTenant+"/compact", `{"policy":"recent","dryRun":true}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	var body struct {
		Sources  []string `json:"sources"`
		Bytes    int64    `json:"bytes"`
		DestTier int      `json:"destTier"`
		DryRun   bool     `json:"dryRun"`
		Queued   bool     `json:"queued"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if !body.DryRun {
		t.Fatal("dryRun must be true")
	}
	if body.Queued {
		t.Fatal("dryRun must not queue")
	}
	if body.DestTier != 1 || body.Bytes != 30 || len(body.Sources) != 2 {
		t.Fatalf("plan = %+v", body)
	}
	if len(c.enqueued) != 0 {
		t.Fatalf("dryRun enqueued %d actions", len(c.enqueued))
	}
}

func TestCompactEnqueue202(t *testing.T) {
	cfg := testAdminConfig(t.TempDir())
	c := &fakeCompactor{
		policy: map[string]merge.CompactSpec{"recent": merge.DefaultCatchupSpec()},
		plan: merge.MergeAction{
			Sources:  []merge.Segment{{Path: "a", Bytes: 1}, {Path: "b", Bytes: 2}},
			DestTier: 1,
		},
		planOK: true,
	}
	srv := httptest.NewServer(compactMux(t, cfg, c, ""))
	defer srv.Close()

	resp := postCompact(t, srv.URL+"/admin/tenants/"+testTenant+"/compact", `{"policy":"recent"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202, got %d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(raw, []byte(`"queued":true`)) {
		t.Fatalf("body = %s, want queued true", raw)
	}
	if len(c.enqueued) != 1 {
		t.Fatalf("enqueued %d, want 1", len(c.enqueued))
	}
	if c.lastTenant != testTenant {
		t.Fatalf("tenant = %s", c.lastTenant)
	}
}

func TestCompactInlineOlderThanDryRun(t *testing.T) {
	cfg := testAdminConfig(t.TempDir())
	c := &fakeCompactor{planOK: false}
	srv := httptest.NewServer(compactMux(t, cfg, c, ""))
	defer srv.Close()

	resp := postCompact(t, srv.URL+"/admin/tenants/"+testTenant+"/compact",
		`{"olderThan":"15m","dryRun":true,"maxSources":32,"maxBytes":"256Mi"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	if c.lastSpec.OlderThan != 15*time.Minute {
		t.Fatalf("olderThan = %s", c.lastSpec.OlderThan)
	}
}

func TestCompactRunJobsFalse204(t *testing.T) {
	cfg := testAdminConfig(t.TempDir())
	cfg.RunJobs = false
	c := &fakeCompactor{
		policy: map[string]merge.CompactSpec{"recent": merge.DefaultCatchupSpec()},
		planOK: true,
		plan:   merge.MergeAction{Sources: []merge.Segment{{Path: "a"}, {Path: "b"}}, DestTier: 1},
	}
	srv := httptest.NewServer(compactMux(t, cfg, c, ""))
	defer srv.Close()

	resp := postCompact(t, srv.URL+"/admin/tenants/"+testTenant+"/compact", `{"policy":"recent"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("want 204, got %d", resp.StatusCode)
	}
	if len(c.enqueued) != 0 {
		t.Fatal("jobs-off compact must not enqueue")
	}
}

func TestCompactBearerRequired(t *testing.T) {
	cfg := testAdminConfig(t.TempDir())
	c := &fakeCompactor{policy: map[string]merge.CompactSpec{"recent": merge.DefaultCatchupSpec()}}
	srv := httptest.NewServer(compactMux(t, cfg, c, "s3cret"))
	defer srv.Close()

	url := srv.URL + "/admin/tenants/" + testTenant + "/compact"
	resp := postCompact(t, url, `{"policy":"recent","dryRun":true}`, "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 without token, got %d", resp.StatusCode)
	}

	resp2 := postCompact(t, url, `{"policy":"recent","dryRun":true}`, "s3cret")
	_ = resp2.Body.Close()
	if resp2.StatusCode == http.StatusUnauthorized {
		t.Fatal("bearer must be accepted")
	}
}
