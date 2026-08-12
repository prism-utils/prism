//go:build !duckdb_arrow

package query_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prism-utils/prism/internal/store/query"
)

func TestSQLArrowStub406(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.Handle(query.SQLRoutePattern(""), query.SQLHandler(sqlConfig(dataDir), eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		sqlURL(srv.URL, tenantSQLA), strings.NewReader(`{"sql":"SELECT 1"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.apache.arrow.stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotAcceptable {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 406 body=%s", resp.StatusCode, body)
	}
}
