package query_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/query"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

type sqlErrorJSON struct {
	Error string `json:"error"`
}

type slogRecord struct {
	Level  string `json:"level"`
	NS     string `json:"ns"`
	Status int    `json:"status"`
	SQL    string `json:"sql"`
	Query  string `json:"query"`
	Err    string `json:"err"`
}

func testSQLServerWithLog(t *testing.T, dataDir string, eng *engine.Engine, logger *slog.Logger) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(query.SQLRoutePattern(""), query.SQLHandler(sqlConfig(dataDir), eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func parseSlogJSON(t *testing.T, raw string) []slogRecord {
	t.Helper()
	var out []slogRecord
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec slogRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("slog json: %v line=%s", err, line)
		}
		out = append(out, rec)
	}
	return out
}

func postSQLRaw(t *testing.T, url, body string) (int, []byte, string) {
	t.Helper()
	resp := postSQL(t, url, body)
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw, resp.Header.Get("Content-Type")
}

func TestSQLErrorJSONAndLogs(t *testing.T) {
	const sqlLogCap = 512
	truncMarker := "SQL_TRUNC_HEAD"
	truncTail := "SQL_TRUNC_TAIL_SHOULD_NOT_APPEAR"
	longInsert := "INSERT INTO metrics VALUES ('" + truncMarker + strings.Repeat("x", sqlLogCap) + truncTail + "')"

	dataDir, eng := twoTenantFixture(t)

	cases := []struct {
		name           string
		tenant         string
		body           string
		wantStatus     int
		wantJSONError  bool
		wantErrSubstr  string
		wantLogLevel   string
		wantLogSQLPart string
		wantNoLogSQL   string
		want404Body    bool
	}{
		{
			name:           "validation non-select",
			tenant:         tenantSQLA,
			body:           `{"sql":"INSERT INTO metrics VALUES (1)"}`,
			wantStatus:     http.StatusBadRequest,
			wantJSONError:  true,
			wantErrSubstr:  "non-select",
			wantLogLevel:   "WARN",
			wantLogSQLPart: "INSERT INTO metrics",
		},
		{
			name:           "engine unknown relation",
			tenant:         tenantSQLA,
			body:           `{"sql":"SELECT * FROM nosuch_relation"}`,
			wantStatus:     http.StatusBadRequest,
			wantJSONError:  true,
			wantErrSubstr:  "nosuch_relation",
			wantLogLevel:   "WARN",
			wantLogSQLPart: "nosuch_relation",
		},
		{
			name:           "truncated sql in logs",
			tenant:         tenantSQLA,
			body:           `{"sql":` + jsonString(longInsert) + `}`,
			wantStatus:     http.StatusBadRequest,
			wantJSONError:  true,
			wantErrSubstr:  "non-select",
			wantLogLevel:   "WARN",
			wantLogSQLPart: truncMarker,
			wantNoLogSQL:   truncTail,
		},
		{
			name:          "unknown tenant",
			tenant:        "INVALID!",
			body:          `{"sql":"SELECT 1"}`,
			wantStatus:    http.StatusNotFound,
			wantJSONError: false,
			want404Body:   true,
			wantLogLevel:  "INFO",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
			srv := testSQLServerWithLog(t, dataDir, eng, logger)

			status, raw, ctype := postSQLRaw(t, sqlURL(srv.URL, tc.tenant), tc.body)
			if status != tc.wantStatus {
				t.Fatalf("status=%d want %d body=%s", status, tc.wantStatus, raw)
			}

			if tc.want404Body {
				if !strings.Contains(string(raw), storetenant.UnknownTenantBody) {
					t.Fatalf("body=%q want %q", raw, storetenant.UnknownTenantBody)
				}
			}
			if tc.wantJSONError {
				if !strings.Contains(ctype, "application/json") {
					t.Fatalf("Content-Type=%q want application/json", ctype)
				}
				var errBody sqlErrorJSON
				if err := json.Unmarshal(raw, &errBody); err != nil {
					t.Fatalf("json: %v body=%s", err, raw)
				}
				if errBody.Error == "" {
					t.Fatalf("empty error body: %s", raw)
				}
				if !strings.Contains(errBody.Error, tc.wantErrSubstr) {
					t.Fatalf("error=%q want substr %q", errBody.Error, tc.wantErrSubstr)
				}
				if len(errBody.Error) > 1024 {
					t.Fatalf("error length %d exceeds 1KiB cap", len(errBody.Error))
				}
			}

			recs := parseSlogJSON(t, buf.String())
			found := false
			for _, rec := range recs {
				if rec.Status != tc.wantStatus && rec.NS != tc.tenant {
					continue
				}
				if rec.NS != tc.tenant && tc.wantStatus != http.StatusNotFound {
					continue
				}
				if rec.Status != tc.wantStatus {
					continue
				}
				found = true
				if rec.Level != tc.wantLogLevel {
					t.Fatalf("level=%q want %q rec=%+v logs=%s", rec.Level, tc.wantLogLevel, rec, buf.String())
				}
				if rec.NS != tc.tenant {
					t.Fatalf("ns=%q want %q", rec.NS, tc.tenant)
				}
				if rec.Err == "" && tc.wantStatus != http.StatusNotFound {
					t.Fatalf("missing err in log: %+v", rec)
				}
				if tc.wantLogSQLPart != "" && !strings.Contains(rec.SQL, tc.wantLogSQLPart) {
					t.Fatalf("sql=%q want substr %q", rec.SQL, tc.wantLogSQLPart)
				}
				if tc.wantNoLogSQL != "" && strings.Contains(rec.SQL, tc.wantNoLogSQL) {
					t.Fatalf("sql log was not truncated; still contains %q (len=%d)", tc.wantNoLogSQL, len(rec.SQL))
				}
				if rec.SQL != "" && len(rec.SQL) > sqlLogCap {
					t.Fatalf("logged sql len=%d exceeds cap %d", len(rec.SQL), sqlLogCap)
				}
			}
			if !found {
				t.Fatalf("no log with ns=%s status=%d; logs=%s", tc.tenant, tc.wantStatus, buf.String())
			}
		})
	}
}

func TestSQLSuccess200Unchanged(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	srv := testSQLServerWithLog(t, dataDir, eng, logger)

	status, raw, ctype := postSQLRaw(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT COUNT(*) AS c FROM metrics"}`)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s", status, raw)
	}
	if !strings.Contains(ctype, "application/json") {
		t.Fatalf("Content-Type=%q", ctype)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("json: %v body=%s", err, raw)
	}
	if _, ok := payload["error"]; ok {
		t.Fatalf("200 body must not include error: %s", raw)
	}
	if _, ok := payload["columns"]; !ok {
		t.Fatalf("200 missing columns: %s", raw)
	}
	if _, ok := payload["rows"]; !ok {
		t.Fatalf("200 missing rows: %s", raw)
	}
}

func jsonString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
