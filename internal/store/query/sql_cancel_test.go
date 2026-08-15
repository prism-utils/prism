package query_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/query"
)

const longGenerateSeriesSQL = "SELECT sum(x) FROM generate_series(1, 500000000) t(x)"

func sqlHandler(t *testing.T, dataDir string, cfg *query.SQLConfig, eng *engine.Engine) http.Handler {
	t.Helper()
	if cfg == nil {
		cfg = sqlConfig(dataDir)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return query.SQLHandler(cfg, eng, logger)
}

func serveSQL(t *testing.T, h http.Handler, ctx context.Context, tenant, sqlText, accept string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"sql":%q}`, sqlText)
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/"+tenant+"/sql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	req.SetPathValue("ns", tenant)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func assertClientClosed(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != 499 {
		t.Fatalf("status=%d want 499 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "client closed") {
		t.Fatalf("body=%q want client closed", rec.Body.String())
	}
}

func TestSQLClientCancelReturns499(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	h := sqlHandler(t, dataDir, sqlConfig(dataDir, func(c *query.SQLConfig) {
		c.Timeout = 30 * time.Second
	}), eng)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := serveSQL(t, h, ctx, tenantSQLA, "SELECT 1", "")
	assertClientClosed(t, rec)
}

func TestSQLClientCancelInterruptsLongQuery(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	h := sqlHandler(t, dataDir, sqlConfig(dataDir, func(c *query.SQLConfig) {
		c.Timeout = 30 * time.Second
	}), eng)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := fmt.Sprintf(`{"sql":%q}`, longGenerateSeriesSQL)
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/"+tenantSQLA+"/sql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("ns", tenantSQLA)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		h.ServeHTTP(rec, req)
		close(done)
	}()

	started := time.After(200 * time.Millisecond)
	<-started
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled query did not return within 5s")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("query hung for %v", elapsed)
	}
	assertClientClosed(t, rec)
}

func TestSQLCanceledSkipsSandboxAfterSnapshot(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	h := sqlHandler(t, dataDir, sqlConfig(dataDir, func(c *query.SQLConfig) {
		c.Timeout = 30 * time.Second
		c.RunJobs = true
	}), eng)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	rec := serveSQL(t, h, ctx, tenantSQLA, longGenerateSeriesSQL, "")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("cancelled request ran for %v, want skip of user SQL", elapsed)
	}
	assertClientClosed(t, rec)

	recOK := serveSQL(t, h, context.Background(), tenantSQLA, "SELECT 1 AS n", "")
	if recOK.Code != http.StatusOK {
		t.Fatalf("follow-up status=%d want 200 body=%s", recOK.Code, recOK.Body.String())
	}
}
