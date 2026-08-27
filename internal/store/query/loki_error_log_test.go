package query_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prism-utils/prism/internal/store/query"
)

func lokiServerWithLog(t *testing.T, cfg *query.LokiConfig, logger *slog.Logger) *httptest.Server {
	t.Helper()
	h := query.LokiHandler(cfg, logger)
	mux := http.NewServeMux()
	for _, p := range query.LokiRoutePatterns("") {
		mux.Handle(p, h)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestLokiQueryRange400LogsTruncatedLogQL(t *testing.T) {
	const queryLogCap = 512
	dataDir := t.TempDir()
	seedLokiLogs(t, dataDir)

	truncTail := "LokiTruncTailShouldNotAppear"
	longExpr := "rate(" + strings.Repeat("q", queryLogCap) + truncTail + ")"

	cases := []struct {
		name          string
		expr          string
		wantLogPart   string
		wantNoLogPart string
	}{
		{
			name:        "unsupported logql",
			expr:        `rate({job="prism"}[5m])`,
			wantLogPart: `rate({job="prism"}[5m])`,
		},
		{
			name:          "truncated logql",
			expr:          longExpr,
			wantLogPart:   "rate(",
			wantNoLogPart: truncTail,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
			srv := lokiServerWithLog(t, lokiConfig(dataDir), logger)

			status, env := lokiGet(t, lokiRangeURL(srv.URL, lokiTenant, tc.expr))
			if status != http.StatusBadRequest || env.Status != "error" || env.Error == "" {
				t.Fatalf("status=%d env=%+v, want 400 error", status, env)
			}

			recs := parseSlogJSON(t, buf.String())
			found := false
			for _, rec := range recs {
				if rec.Status != http.StatusBadRequest {
					continue
				}
				found = true
				if rec.Level != "WARN" {
					t.Fatalf("level=%q want WARN rec=%+v logs=%s", rec.Level, rec, buf.String())
				}
				if rec.NS != lokiTenant {
					t.Fatalf("ns=%q want %q", rec.NS, lokiTenant)
				}
				if rec.Query == "" {
					t.Fatalf("missing query in log: %+v", rec)
				}
				if rec.Err == "" {
					t.Fatalf("missing err in log: %+v", rec)
				}
				if tc.wantLogPart != "" && !strings.Contains(rec.Query, tc.wantLogPart) {
					t.Fatalf("query=%q want substr %q", rec.Query, tc.wantLogPart)
				}
				if tc.wantNoLogPart != "" && strings.Contains(rec.Query, tc.wantNoLogPart) {
					t.Fatalf("query log was not truncated; still contains %q (len=%d)", tc.wantNoLogPart, len(rec.Query))
				}
				if len(rec.Query) > queryLogCap {
					t.Fatalf("logged query len=%d exceeds cap %d", len(rec.Query), queryLogCap)
				}
			}
			if !found {
				t.Fatalf("no Warn log with status=400; logs=%s", buf.String())
			}
		})
	}
}
