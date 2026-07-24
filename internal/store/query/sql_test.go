package query_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elk-utilities/prism/internal/store/authtest"
	"github.com/elk-utilities/prism/internal/store/authz"
	"github.com/elk-utilities/prism/internal/store/cluster"
	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/query"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const (
	tenantSQLA = "user-6f3a9c2b-apps"
	tenantSQLB = "user-7a4b1c9d-web"
)

type sqlResponse struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
}

func sqlConfig(dataDir string, opts ...func(*query.SQLConfig)) *query.SQLConfig {
	cfg := &query.SQLConfig{
		DataDir:      dataDir,
		MaxRows:      100_000,
		Timeout:      30 * time.Second,
		MemoryLimit:  "",
		MaxBodyBytes: query.DefaultSQLMaxBodyBytes,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func testSQLServer(t *testing.T, dataDir string, cfg *query.SQLConfig, eng *engine.Engine) *httptest.Server {
	t.Helper()
	if cfg == nil {
		cfg = sqlConfig(dataDir)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.Handle(query.SQLRoutePattern(""), query.SQLHandler(cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func postSQL(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func postSQLAuth(t *testing.T, url, body, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decodeSQLResp(t *testing.T, resp *http.Response) sqlResponse {
	t.Helper()
	var out sqlResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("decode: %v body=%s", err, body)
	}
	return out
}

func seedTenantMetrics(t *testing.T, eng *engine.Engine, dataDir, tenant string, rows []testparquet.Row) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dataDir, tenant), 0o750); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", rows)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Ingest(tenant, f); err != nil {
		_ = f.Close()
		t.Fatalf("ingest %s: %v", tenant, err)
	}
	_ = f.Close()
	if err := eng.ExportHotSnapshot(tenant); err != nil {
		t.Fatalf("snapshot %s: %v", tenant, err)
	}
}

func twoTenantFixture(t *testing.T) (dataDir string, eng *engine.Engine) {
	t.Helper()
	dataDir = t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng = engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	seedTenantMetrics(t, eng, dataDir, tenantSQLA, []testparquet.Row{
		{Name: "metric_a", Labels: "{}", Value: 10, TimestampMs: 1},
		{Name: "metric_a", Labels: "{}", Value: 20, TimestampMs: 2},
		{Name: "metric_b", Labels: "{}", Value: 30, TimestampMs: 3},
	})
	seedTenantMetrics(t, eng, dataDir, tenantSQLB, []testparquet.Row{
		{Name: "other", Labels: "{}", Value: 999, TimestampMs: 1},
		{Name: "other", Labels: "{}", Value: 999, TimestampMs: 2},
		{Name: "other", Labels: "{}", Value: 999, TimestampMs: 3},
		{Name: "other", Labels: "{}", Value: 999, TimestampMs: 4},
		{Name: "other", Labels: "{}", Value: 999, TimestampMs: 5},
	})
	return dataDir, eng
}

func sqlURL(base, tenant string) string {
	return base + "/" + tenant + "/sql"
}

func execSQL(t *testing.T, srv *httptest.Server, tenant, sqlText string) (int, sqlResponse) {
	t.Helper()
	code, out, err := execSQLResult(srv, tenant, sqlText)
	if err != nil {
		t.Fatal(err)
	}
	return code, out
}

func execSQLResult(srv *httptest.Server, tenant, sqlText string) (int, sqlResponse, error) {
	body := fmt.Sprintf(`{"sql":%q}`, sqlText)
	resp := postSQLNoHelper(srv.URL+"/"+tenant+"/sql", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, sqlResponse{}, fmt.Errorf("status=%d body=%s", resp.StatusCode, b)
	}
	var out sqlResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return resp.StatusCode, sqlResponse{}, err
	}
	return resp.StatusCode, out, nil
}

func postSQLNoHelper(url, body string) *http.Response {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}

func execSQLExpect400(t *testing.T, srv *httptest.Server, tenant, sqlText string) {
	t.Helper()
	body := fmt.Sprintf(`{"sql":%q}`, sqlText)
	resp := postSQL(t, sqlURL(srv.URL, tenant), body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 400 sql=%q body=%s", resp.StatusCode, sqlText, b)
	}
	msg, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(msg), "bad query") {
		t.Fatalf("body=%q want bad query", msg)
	}
}

func TestSQLIsolationCrossTenant(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	bGlob := filepath.Join(dataDir, tenantSQLB, "tiers", "L0", "*.parquet")
	bEngine := filepath.Join(dataDir, tenantSQLB, "engine.duckdb")
	outPath := filepath.Join(dataDir, tenantSQLA, "escape.parquet")

	attacks := []string{
		fmt.Sprintf("SELECT * FROM read_parquet('%s')", filepath.ToSlash(bGlob)),
		fmt.Sprintf("ATTACH '%s' AS stolen", filepath.ToSlash(bEngine)),
		fmt.Sprintf("COPY (SELECT 1) TO '%s'", filepath.ToSlash(outPath)),
		"SELECT * FROM read_csv('/etc/passwd')",
		"SELECT * FROM read_parquet('/etc/passwd')",
		"SET enable_external_access=true",
		fmt.Sprintf("SELECT * FROM glob('%s')", filepath.ToSlash(filepath.Join(dataDir, tenantSQLB, "tiers", "L0", "*.parquet"))),
		fmt.Sprintf("SELECT * FROM parquet_metadata('%s')", filepath.ToSlash(filepath.Join(dataDir, tenantSQLB, "engine.duckdb"))),
		fmt.Sprintf("SELECT * FROM parquet_schema('%s')", filepath.ToSlash(filepath.Join(dataDir, tenantSQLB, "tiers", "L0", "*.parquet"))),
		"SELECT * FROM read_json('/etc/passwd')",
		"SELECT * FROM read_text('/etc/passwd')",
	}
	for _, sqlText := range attacks {
		t.Run(sqlText, func(t *testing.T) {
			execSQLExpect400(t, srv, tenantSQLA, sqlText)
		})
	}

	code, out := execSQL(t, srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
	if code != http.StatusOK {
		t.Fatalf("count status=%d", code)
	}
	if len(out.Rows) != 1 {
		t.Fatalf("rows=%v", out.Rows)
	}
	count := numericCell(t, out.Rows[0][0])
	if count != 3 {
		t.Fatalf("tenant A count=%v want 3", count)
	}
}

func TestSQLIsolationReadParquetOutsideTenantRoot(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	otherPath := filepath.ToSlash(filepath.Join(dataDir, tenantSQLB, "hot", "current.parquet"))
	if _, err := os.Stat(otherPath); err != nil {
		t.Fatalf("other tenant snapshot: %v", err)
	}

	attacks := []string{
		fmt.Sprintf("SELECT * FROM read_parquet('%s')", otherPath),
		"SELECT * FROM read_parquet('/etc/passwd')",
	}
	for _, sqlText := range attacks {
		t.Run(sqlText, func(t *testing.T) {
			execSQLExpect400(t, srv, tenantSQLA, sqlText)
		})
	}

	var extAccess string
	code, out := execSQL(t, srv, tenantSQLA, "SELECT current_setting('enable_external_access') AS v")
	if code != http.StatusOK {
		t.Fatalf("enable_external_access status=%d", code)
	}
	if len(out.Rows) != 1 {
		t.Fatalf("rows=%v", out.Rows)
	}
	switch v := out.Rows[0][0].(type) {
	case string:
		extAccess = v
	case bool:
		if v {
			extAccess = "true"
		} else {
			extAccess = "false"
		}
	default:
		t.Fatalf("enable_external_access type=%T val=%v", v, v)
	}
	if extAccess != "false" {
		t.Fatalf("enable_external_access=%q want false", extAccess)
	}
}

func numericCell(t *testing.T, v any) float64 {
	t.Helper()
	return numericCellValue(v)
}

func numericCellValue(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	case int64:
		return float64(n)
	default:
		panic(fmt.Sprintf("unexpected numeric type %T %v", v, v))
	}
}

func TestSQLReadOnlyRejected(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	rejected := []string{
		"INSERT INTO metrics VALUES ('x','{}',1,0,now())",
		"UPDATE metrics SET value=0",
		"DELETE FROM metrics",
		"CREATE TABLE evil (x INT)",
		"DROP VIEW metrics",
		"ATTACH ':memory:' AS x",
		"COPY metrics TO '/tmp/x.parquet'",
		"INSTALL httpfs",
		"LOAD httpfs",
		"PRAGMA table_info('metrics')",
		"SELECT 1; SELECT 2",
		"SELECT 1; DROP TABLE metrics",
	}
	for _, sqlText := range rejected {
		t.Run(sqlText, func(t *testing.T) {
			execSQLExpect400(t, srv, tenantSQLA, sqlText)
		})
	}
}

func TestSQLEphemeralSandbox(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	execSQLExpect400(t, srv, tenantSQLA, "CREATE TABLE ephemeral (n INT)")
	body := fmt.Sprintf(`{"sql":%q}`, "SELECT COUNT(*) AS c FROM ephemeral")
	resp := postSQL(t, sqlURL(srv.URL, tenantSQLA), body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 400 body=%s", resp.StatusCode, b)
	}
}

func TestSQLTimeoutInterrupts(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) { c.Timeout = 200 * time.Millisecond })
	srv := testSQLServer(t, dataDir, cfg, eng)

	start := time.Now()
	execSQLExpect400(t, srv, tenantSQLA, "SELECT sum(x) FROM generate_series(1, 500000000) t(x)")
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("query hung for %v", elapsed)
	}
}

func TestSQLRowCapTruncated(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	var rows []testparquet.Row
	for i := 0; i < 50; i++ {
		rows = append(rows, testparquet.Row{Name: "m", Labels: "{}", Value: float64(i), TimestampMs: int64(i)})
	}
	seedTenantMetrics(t, eng, dataDir, tenantSQLA, rows)

	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) { c.MaxRows = 10 })
	srv := testSQLServer(t, dataDir, cfg, eng)

	body := `{"sql":"SELECT * FROM metrics ORDER BY timestamp_ms","max_rows":10}`
	resp := postSQL(t, sqlURL(srv.URL, tenantSQLA), body)
	defer func() { _ = resp.Body.Close() }()
	out := decodeSQLResp(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if !out.Truncated {
		t.Fatal("truncated=false want true")
	}
	if out.RowCount != 10 {
		t.Fatalf("row_count=%d want 10", out.RowCount)
	}
	if len(out.Rows) != 10 {
		t.Fatalf("rows len=%d", len(out.Rows))
	}
}

func TestSQLAPIEnabledFalse404(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	// Route not registered when disabled — simulate by empty mux.
	_ = query.SQLHandler(sqlConfig(dataDir), eng, logger)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp := postSQL(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT 1"}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
}

func TestSQLUnknownTenant404(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	resp := postSQL(t, sqlURL(srv.URL, "INVALID!"), `{"sql":"SELECT 1"}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), storetenant.UnknownTenantBody) {
		t.Fatalf("body=%q", body)
	}
}

func TestSQLRBACReaderAllowed(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, env := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp := postSQLAuth(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT COUNT(*) FROM metrics"}`, tok)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	_ = resp.Body.Close()
}

func TestSQLRBACWriter403(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, env := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	tok, err := env.SignToken(authtest.WithSubject("writer-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp := postSQLAuth(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT 1"}`, tok)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d want 403", resp.StatusCode)
	}
}

func TestSQLRBACUnboundTenant404(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, env := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp := postSQLAuth(t, sqlURL(srv.URL, tenantSQLB), `{"sql":"SELECT 1"}`, tok)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
}

func TestSQLRBACMissingJWT401(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, _ := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	resp := postSQLAuth(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT 1"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", resp.StatusCode)
	}
}

func TestSQLRBACInvalidJWT401(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, _ := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	resp := postSQLAuth(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT 1"}`, "not.a.jwt")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", resp.StatusCode)
	}
}

func TestSQLAdminTokenWhenRBACOff(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	const token = "admin-secret"
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	h := query.SQLHandler(sqlConfig(dataDir), eng, logger)
	mux.Handle(query.SQLRoutePattern(""), adminWrapSQL(token, h))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp := postSQLAuth(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT 1"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token status=%d want 401", resp.StatusCode)
	}

	resp2 := postSQLAuth(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT 1"}`, token)
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("with token status=%d body=%s", resp2.StatusCode, body)
	}
}

func TestSQLCorrectnessParity(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	code, out := execSQL(t, srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
	if code != http.StatusOK {
		t.Fatalf("sql count status=%d", code)
	}
	sqlCount := numericCell(t, out.Rows[0][0])

	var engineCount int64
	start := time.Unix(1700000000, 0).UTC()
	err := eng.WithRead(tenantSQLA, func(db *sql.DB) error {
		b := query.Builder{DataDir: dataDir}
		sqlText, args, err := b.BuildSQLWithDB(context.Background(), &query.Request{
			Tenant: tenantSQLA,
			Start:  start.Add(-time.Hour),
			End:    start.Add(24 * time.Hour),
		}, db)
		if err != nil {
			return err
		}
		agg, err := query.AggregateSQL(sqlText)
		if err != nil {
			return err
		}
		return db.QueryRowContext(context.Background(), agg, args...).Scan(&engineCount, new(float64))
	})
	if err != nil {
		t.Fatalf("engine aggregate: %v", err)
	}
	if int64(sqlCount) != engineCount {
		t.Fatalf("count parity: sql=%v engine=%d", sqlCount, engineCount)
	}

	code, out = execSQL(t, srv, tenantSQLA,
		`SELECT "__name__", avg(value) AS av FROM metrics GROUP BY "__name__" ORDER BY "__name__"`)
	if code != http.StatusOK {
		t.Fatalf("group status=%d", code)
	}

	avgs := map[string]float64{}
	for _, row := range out.Rows {
		name, _ := row[0].(string)
		avgs[name] = numericCell(t, row[1])
	}
	if avgs["metric_a"] != 15 || avgs["metric_b"] != 30 {
		t.Fatalf("group avgs=%v", avgs)
	}
}

func TestClusterSQLRoutesToOwner(t *testing.T) {
	var hits int
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/sql") {
			t.Errorf("unexpected proxied request %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"columns":["x"],"rows":[[1]],"row_count":1,"truncated":false}`))
	}))
	t.Cleanup(up.Close)

	clients, err := cluster.ParseClients(tenantSQLA + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}
	mux := cluster.NewServeMux(clients, "", nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	resp := postSQL(t, srv.URL+"/"+tenantSQLA+"/sql", `{"sql":"SELECT 1"}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if hits != 1 {
		t.Fatalf("upstream hits=%d", hits)
	}
}

func TestClusterSQLRBACDenyBeforeProxy(t *testing.T) {
	var hits int
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(up.Close)

	policy, env := writeClusterSQLPolicy(t)
	clients, err := cluster.ParseClients(tenantSQLA + "=" + up.URL + "," + tenantSQLB + "=" + up.URL)
	if err != nil {
		t.Fatal(err)
	}
	mux := cluster.NewServeMux(clients, "", rbacSQLGuard(t, policy, env))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp := postSQLAuth(t, srv.URL+"/"+tenantSQLB+"/sql", `{"sql":"SELECT 1"}`, tok)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
	if hits != 0 {
		t.Fatalf("upstream hits=%d want 0", hits)
	}
}

func rbacPolicySQL() string {
	return `bindings:
  - subject: "reader-a"
    role: reader
    tenants: ["` + tenantSQLA + `"]
  - subject: "writer-a"
    role: writer
    tenants: ["` + tenantSQLA + `"]
`
}

func testSQLServerRBAC(t *testing.T, dataDir string, eng *engine.Engine, policy string) (*httptest.Server, *authtest.JWTEnv) {
	t.Helper()
	env := authtest.NewJWTEnv(t, "prism-store")
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(policy), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: path, ReloadSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	mw := authz.NewMiddleware(env.Verifier(t), a, slog.New(slog.NewTextHandler(io.Discard, nil)))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	h := mw.WrapSQL(query.SQLHandler(sqlConfig(dataDir), eng, logger))
	mux.Handle(query.SQLRoutePattern(""), h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, env
}

func adminWrapSQL(token string, h http.Handler) http.Handler {
	if token == "" {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		hdr := r.Header.Get("Authorization")
		if !strings.HasPrefix(hdr, prefix) || strings.TrimSpace(strings.TrimPrefix(hdr, prefix)) != token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func writeClusterSQLPolicy(t *testing.T) (string, *authtest.JWTEnv) {
	t.Helper()
	env := authtest.NewJWTEnv(t, "prism-store")
	body := `bindings:
  - subject: "reader-a"
    role: reader
    tenants: ["` + tenantSQLA + `"]
`
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, env
}

func rbacSQLGuard(t *testing.T, policyPath string, env *authtest.JWTEnv) func(http.Handler) http.Handler {
	t.Helper()
	a, err := authz.NewAuthorizer(context.Background(), authz.Config{PolicyFile: policyPath, ReloadSeconds: 0})
	if err != nil {
		t.Fatal(err)
	}
	mw := authz.NewMiddleware(env.Verifier(t), a, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mw.WrapSQL
}

func TestSQLMalformedJSON400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)
	resp := postSQL(t, sqlURL(srv.URL, tenantSQLA), `{not json`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestSQLEmptySQL400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)
	resp := postSQL(t, sqlURL(srv.URL, tenantSQLA), `{"sql":""}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestSQLResponseShape(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)
	resp := postSQL(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT \"__name__\", value FROM metrics LIMIT 1"}`)
	defer func() { _ = resp.Body.Close() }()
	out := decodeSQLResp(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if len(out.Columns) == 0 || len(out.Rows) == 0 {
		t.Fatalf("empty result: %+v", out)
	}
	if out.RowCount != len(out.Rows) {
		t.Fatalf("row_count=%d rows=%d", out.RowCount, len(out.Rows))
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte(`"truncated"`)) {
		t.Fatalf("missing truncated field: %s", raw)
	}
}

// A known tenant with no parquet sources answers read-only queries with an
// empty, correctly-typed result rather than a misleading 400 "bad query".
func TestSQLNoParquetTenantEmptyResult200(t *testing.T) {
	dataDir := t.TempDir()
	tenantRoot := filepath.Join(dataDir, tenantSQLA)
	if err := os.MkdirAll(tenantRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })

	srv := testSQLServer(t, dataDir, nil, eng)

	// Aggregate over the (empty) metrics view returns zero, not an error.
	code, out := execSQL(t, srv, tenantSQLA, "SELECT COUNT(*) FROM metrics")
	if code != http.StatusOK {
		t.Fatalf("count status=%d want 200", code)
	}
	if out.RowCount != 1 || len(out.Rows) != 1 {
		t.Fatalf("count result=%+v want single row", out)
	}

	// A non-table query also succeeds against the empty sandbox.
	code, out = execSQL(t, srv, tenantSQLA, "SELECT 1 AS ok")
	if code != http.StatusOK {
		t.Fatalf("select 1 status=%d want 200", code)
	}
	if out.RowCount != 1 {
		t.Fatalf("select 1 result=%+v want single row", out)
	}

	// Selecting the empty metrics view yields the right columns and no rows.
	code, out = execSQL(t, srv, tenantSQLA, "SELECT * FROM metrics")
	if code != http.StatusOK {
		t.Fatalf("select metrics status=%d want 200", code)
	}
	if out.RowCount != 0 || len(out.Rows) != 0 {
		t.Fatalf("select metrics rows=%d want empty", out.RowCount)
	}
	want := []string{"__name__", "labels", "value", "timestamp_ms", "ts"}
	if len(out.Columns) != len(want) {
		t.Fatalf("columns=%v want %v", out.Columns, want)
	}
	for i := range want {
		if out.Columns[i] != want[i] {
			t.Fatalf("column[%d]=%q want %q", i, out.Columns[i], want[i])
		}
	}
}

func TestSQLEmptyResult200(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	code, out := execSQL(t, srv, tenantSQLA, "SELECT * FROM metrics WHERE false")
	if code != http.StatusOK {
		t.Fatalf("status=%d want 200", code)
	}
	if out.RowCount != 0 || len(out.Rows) != 0 {
		t.Fatalf("row_count=%d rows=%d want empty", out.RowCount, len(out.Rows))
	}
	if out.Truncated {
		t.Fatal("truncated=true want false")
	}
	if out.Columns == nil {
		t.Fatal("columns should be present")
	}
}

func TestSQLUnknownRelation400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)
	execSQLExpect400(t, srv, tenantSQLA, "SELECT * FROM nosuch_relation")
}

func TestSQLConcurrentSameTenantIsolated(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	const workers = 12
	var wg sync.WaitGroup
	errCh := make(chan string, workers*2)
	var ddlRejected atomic.Int32

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if id == 0 {
				resp := postSQLNoHelper(sqlURL(srv.URL, tenantSQLA), `{"sql":"CREATE TABLE leak (id INT)"}`)
				defer func() { _ = resp.Body.Close() }()
				if resp.StatusCode != http.StatusBadRequest {
					errCh <- fmt.Sprintf("create status=%d want 400", resp.StatusCode)
					return
				}
				ddlRejected.Add(1)
				return
			}
			code, out, err := execSQLResult(srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
			if err != nil {
				errCh <- fmt.Sprintf("worker %d: %v", id, err)
				return
			}
			if code != http.StatusOK {
				errCh <- fmt.Sprintf("worker %d status=%d", id, code)
				return
			}
			if len(out.Rows) == 0 {
				errCh <- fmt.Sprintf("worker %d empty rows", id)
				return
			}
			if n := numericCellValue(out.Rows[0][0]); n != 3 {
				errCh <- fmt.Sprintf("worker %d count=%v want 3", id, n)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Error(msg)
	}
	if ddlRejected.Load() != 1 {
		t.Fatalf("ddlRejected=%d want 1", ddlRejected.Load())
	}

	resp := postSQLNoHelper(sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT COUNT(*) AS c FROM leak"}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("leak follow-up status=%d body=%s want 400", resp.StatusCode, b)
	}
}

func TestSQLAfterHotSnapshotAndFlush(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	var mu sync.Mutex
	clk := start
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: 50 * time.Millisecond}, func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clk
	})
	t.Cleanup(func() { _ = eng.Close() })

	seedTenantMetrics(t, eng, dataDir, tenantSQLA, []testparquet.Row{
		{Name: "flush_metric", Labels: "{}", Value: 42, TimestampMs: 1},
	})

	mu.Lock()
	clk = start.Add(time.Minute)
	mu.Unlock()
	if err := eng.FlushDue(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := eng.ExportHotSnapshot(tenantSQLA); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantSQLA, `SELECT COUNT(*) AS c FROM metrics WHERE "__name__" = 'flush_metric'`)
	if code != http.StatusOK {
		t.Fatalf("status=%d", code)
	}
	if numericCell(t, out.Rows[0][0]) != 1 {
		t.Fatalf("count=%v want 1 after flush+snapshot", out.Rows[0][0])
	}
}

func TestSQLBurstQueriesAfterFlush(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	clk := start
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: 50 * time.Millisecond}, func() time.Time { return clk })
	t.Cleanup(func() { _ = eng.Close() })

	seedTenantMetrics(t, eng, dataDir, tenantSQLA, []testparquet.Row{
		{Name: "race_metric", Labels: "{}", Value: 7, TimestampMs: 1},
	})
	clk = start.Add(time.Minute)
	if err := eng.FlushDue(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	srv := testSQLServer(t, dataDir, nil, eng)
	const workers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, out, err := execSQLResult(srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
			if err != nil {
				errCh <- err
				return
			}
			if numericCellValue(out.Rows[0][0]) < 1 {
				errCh <- fmt.Errorf("count=%v want >=1", out.Rows[0][0])
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestSQLWithSelect200(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantSQLA, "WITH x AS (SELECT \"__name__\", value FROM metrics) SELECT COUNT(*) AS c FROM x")
	if code != http.StatusOK {
		t.Fatalf("status=%d", code)
	}
	if numericCell(t, out.Rows[0][0]) != 3 {
		t.Fatalf("count=%v want 3", out.Rows[0][0])
	}
}

func TestSQLWithDMLRejected400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)
	cases := []string{
		"WITH x AS (SELECT 1) INSERT INTO metrics SELECT * FROM x",
		"WITH x AS (SELECT 1) DELETE FROM metrics",
		"WITH x AS (SELECT 1) COPY (SELECT 1) TO '/tmp/x.parquet'",
	}
	for _, sqlText := range cases {
		t.Run(sqlText, func(t *testing.T) {
			execSQLExpect400(t, srv, tenantSQLA, sqlText)
		})
	}
}

func TestSQLMaxBodyBytes400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) { c.MaxBodyBytes = 512 })
	srv := testSQLServer(t, dataDir, cfg, eng)
	payload := `{"sql":"` + strings.Repeat("x", 600) + `"}`
	resp := postSQL(t, sqlURL(srv.URL, tenantSQLA), payload)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 400 body=%s", resp.StatusCode, body)
	}
}

func TestSQLSymlinkParquetExcluded(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	linkDir := filepath.Join(dataDir, tenantSQLA, "tiers", "L0")
	if err := os.MkdirAll(linkDir, 0o750); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dataDir, tenantSQLB, "hot", "current.parquet")
	link := filepath.Join(linkDir, "evil-link.parquet")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	srv := testSQLServer(t, dataDir, nil, eng)
	code, out := execSQL(t, srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
	if code != http.StatusOK {
		t.Fatalf("status=%d", code)
	}
	if numericCell(t, out.Rows[0][0]) != 3 {
		t.Fatalf("count=%v want 3 (symlink data must not be included)", out.Rows[0][0])
	}
}

func TestSQLSymlinkedTenantRoot404(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	nsAPath := filepath.Join(dataDir, tenantSQLA)
	if err := os.RemoveAll(nsAPath); err != nil {
		t.Fatal(err)
	}
	nsBPath := filepath.Join(dataDir, tenantSQLB)
	if err := os.Symlink(nsBPath, nsAPath); err != nil {
		t.Fatal(err)
	}

	body := `{"sql":"SELECT COUNT(*) AS c FROM metrics"}`
	resp := postSQL(t, sqlURL(srv.URL, tenantSQLA), body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("cross-tenant symlink status=%d want 404 body=%s", resp.StatusCode, b)
	}
	msg, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(msg), storetenant.UnknownTenantBody) {
		t.Fatalf("body=%q want %q", msg, storetenant.UnknownTenantBody)
	}

	if err := os.Remove(nsAPath); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, nsAPath); err != nil {
		t.Fatal(err)
	}
	resp2 := postSQL(t, sqlURL(srv.URL, tenantSQLA), body)
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusNotFound {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("outside symlink status=%d want 404 body=%s", resp2.StatusCode, b)
	}
}

func TestSQLReadParquetPathTraversal400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	absRoot, err := filepath.Abs(filepath.Join(dataDir, tenantSQLA))
	if err != nil {
		t.Fatal(err)
	}
	traversal := filepath.ToSlash(filepath.Join(absRoot, "..", tenantSQLB, "hot", "current.parquet"))

	attacks := []string{
		fmt.Sprintf("SELECT * FROM read_parquet('%s')", traversal),
		fmt.Sprintf("SELECT * FROM read_parquet('../%s/hot/current.parquet')", tenantSQLB),
	}
	for _, sqlText := range attacks {
		t.Run(sqlText, func(t *testing.T) {
			execSQLExpect400(t, srv, tenantSQLA, sqlText)
		})
	}
}

func TestSQLResetRejected400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	execSQLExpect400(t, srv, tenantSQLA, "RESET enable_external_access")

	code, out := execSQL(t, srv, tenantSQLA, "SELECT current_setting('enable_external_access') AS v")
	if code != http.StatusOK {
		t.Fatalf("status=%d", code)
	}
	if len(out.Rows) != 1 {
		t.Fatalf("rows=%v", out.Rows)
	}
	switch v := out.Rows[0][0].(type) {
	case string:
		if v != "false" {
			t.Fatalf("enable_external_access=%q want false", v)
		}
	case bool:
		if v {
			t.Fatalf("enable_external_access=true want false")
		}
	default:
		t.Fatalf("unexpected type %T", v)
	}
}

func hotOnlySQLFixture(t *testing.T) (dataDir string, eng *engine.Engine, hotRows int) {
	t.Helper()
	dataDir = t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng = engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	hotRows = 3
	seedTenantMetrics(t, eng, dataDir, tenantSQLA, []testparquet.Row{
		{Name: "hot_a", Labels: "{}", Value: 1, TimestampMs: 1},
		{Name: "hot_b", Labels: "{}", Value: 2, TimestampMs: 2},
		{Name: "hot_c", Labels: "{}", Value: 3, TimestampMs: 3},
	})

	l0Dir := layout.TierDir(dataDir, tenantSQLA, 0)
	if err := os.MkdirAll(l0Dir, 0o750); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		testparquet.WriteSegmentWithTs(t, filepath.Join(l0Dir, fmt.Sprintf("tier_%d.parquet", i)),
			start.Add(time.Duration(i)*time.Minute), fmt.Sprintf("tier_%d", i), float64(i+10))
	}
	return dataDir, eng, hotRows
}

func TestSQLHotOnlyCountExcludesTiersJSON(t *testing.T) {
	dataDir, eng, hotRows := hotOnlySQLFixture(t)

	hotSrv := testSQLServer(t, dataDir, sqlConfig(dataDir, func(c *query.SQLConfig) { c.HotOnly = true }), eng)
	fullSrv := testSQLServer(t, dataDir, sqlConfig(dataDir), eng)

	hotCode, hotOut := execSQL(t, hotSrv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
	if hotCode != http.StatusOK {
		t.Fatalf("hot-only status=%d", hotCode)
	}
	if got := int(numericCell(t, hotOut.Rows[0][0])); got != hotRows {
		t.Fatalf("hot-only count=%d want %d (tiers must be skipped)", got, hotRows)
	}

	fullCode, fullOut := execSQL(t, fullSrv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
	if fullCode != http.StatusOK {
		t.Fatalf("full status=%d", fullCode)
	}
	if got := int(numericCell(t, fullOut.Rows[0][0])); got <= hotRows {
		t.Fatalf("full count=%d want > %d (hot + tiers)", got, hotRows)
	}
}

func TestSQLHotOnlyIsolationCrossTenantStill400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) { c.HotOnly = true })
	srv := testSQLServer(t, dataDir, cfg, eng)

	bGlob := filepath.Join(dataDir, tenantSQLB, "tiers", "L0", "*.parquet")
	execSQLExpect400(t, srv, tenantSQLA, fmt.Sprintf("SELECT * FROM read_parquet('%s')", filepath.ToSlash(bGlob)))

	code, out := execSQL(t, srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
	if code != http.StatusOK {
		t.Fatalf("count status=%d", code)
	}
	if numericCell(t, out.Rows[0][0]) != 3 {
		t.Fatalf("tenant A count=%v want 3", out.Rows[0][0])
	}
}
