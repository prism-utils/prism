//go:build duckdb_arrow

package query_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/elk-utilities/prism/internal/store/authtest"
	"github.com/elk-utilities/prism/internal/store/cluster"
	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/query"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
	"github.com/elk-utilities/prism/internal/store/testparquet"
)

const arrowStreamAccept = "application/vnd.apache.arrow.stream"

func postSQLArrow(t *testing.T, url, body, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", arrowStreamAccept)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func postSQLArrowNoAccept(t *testing.T, url, body string) *http.Response {
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

func decodeArrowStream(t *testing.T, r io.Reader) (schema *arrow.Schema, rows [][]any, truncated string) {
	t.Helper()
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("ipc reader: %v", err)
	}
	defer reader.Release()
	schema = reader.Schema()
	for reader.Next() {
		rec := reader.RecordBatch()
		for rowIdx := int64(0); rowIdx < rec.NumRows(); rowIdx++ {
			row := make([]any, rec.NumCols())
			for colIdx := int64(0); colIdx < rec.NumCols(); colIdx++ {
				row[colIdx] = arrowCellAt(rec, colIdx, rowIdx)
			}
			rows = append(rows, row)
		}
		rec.Release()
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("ipc read: %v", err)
	}
	return schema, rows, ""
}

func arrowCellAt(rec arrow.RecordBatch, col, row int64) any {
	colArr := rec.Column(int(col))
	if colArr.IsNull(int(row)) {
		return nil
	}
	switch arr := colArr.(type) {
	case *array.Int64:
		return arr.Value(int(row))
	case *array.Float64:
		return arr.Value(int(row))
	case *array.String:
		return arr.Value(int(row))
	case *array.Boolean:
		return arr.Value(int(row))
	case *array.Timestamp:
		return arr.Value(int(row))
	default:
		return fmt.Sprint(colArr)
	}
}

func execSQLArrow(t *testing.T, srv *httptest.Server, tenant, sqlText string) (int, *arrow.Schema, [][]any, bool) {
	t.Helper()
	body := fmt.Sprintf(`{"sql":%q}`, sqlText)
	resp := postSQLArrow(t, sqlURL(srv.URL, tenant), body, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, arrowStreamAccept) {
		t.Fatalf("content-type=%q want arrow stream", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	trunc := resp.Trailer.Get("X-Prism-Truncated")
	if trunc == "" {
		trunc = resp.Header.Get("X-Prism-Truncated")
	}
	schema, rows, _ := decodeArrowStream(t, bytes.NewReader(raw))
	truncated := trunc == "true"
	return resp.StatusCode, schema, rows, truncated
}

func TestSQLArrowJSONParityCount(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	_, jsonOut := execSQL(t, srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
	_, _, arrowRows, _ := execSQLArrow(t, srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")

	if len(jsonOut.Rows) != 1 || len(arrowRows) != 1 {
		t.Fatalf("json rows=%d arrow rows=%d", len(jsonOut.Rows), len(arrowRows))
	}
	jsonCount := numericCell(t, jsonOut.Rows[0][0])
	arrowCount := numericCell(t, arrowRows[0][0])
	if jsonCount != arrowCount {
		t.Fatalf("count json=%v arrow=%v", jsonCount, arrowCount)
	}
	if int64(jsonCount) != 3 {
		t.Fatalf("count=%v want 3", jsonCount)
	}
}

func TestSQLArrowJSONParityGroupBy(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	sqlText := `SELECT "__name__", avg(value) AS av FROM metrics GROUP BY "__name__" ORDER BY "__name__"`
	_, jsonOut := execSQL(t, srv, tenantSQLA, sqlText)
	_, _, arrowRows, _ := execSQLArrow(t, srv, tenantSQLA, sqlText)

	if len(jsonOut.Rows) != len(arrowRows) {
		t.Fatalf("row count json=%d arrow=%d", len(jsonOut.Rows), len(arrowRows))
	}
	jsonAvgs := map[string]float64{}
	for _, row := range jsonOut.Rows {
		name, _ := row[0].(string)
		jsonAvgs[name] = numericCell(t, row[1])
	}
	arrowAvgs := map[string]float64{}
	for _, row := range arrowRows {
		name, _ := row[0].(string)
		arrowAvgs[name] = numericCell(t, row[1])
	}
	if jsonAvgs["metric_a"] != arrowAvgs["metric_a"] || jsonAvgs["metric_b"] != arrowAvgs["metric_b"] {
		t.Fatalf("json=%v arrow=%v", jsonAvgs, arrowAvgs)
	}
	if arrowAvgs["metric_a"] != 15 || arrowAvgs["metric_b"] != 30 {
		t.Fatalf("avgs=%v", arrowAvgs)
	}
}

func TestSQLArrowJSONParityMultiRowScan(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	sqlText := `SELECT "__name__", value, timestamp_ms FROM metrics ORDER BY timestamp_ms`
	_, jsonOut := execSQL(t, srv, tenantSQLA, sqlText)
	_, _, arrowRows, _ := execSQLArrow(t, srv, tenantSQLA, sqlText)

	if len(jsonOut.Rows) != len(arrowRows) {
		t.Fatalf("row count json=%d arrow=%d", len(jsonOut.Rows), len(arrowRows))
	}
	for i := range jsonOut.Rows {
		if fmt.Sprint(jsonOut.Rows[i][0]) != fmt.Sprint(arrowRows[i][0]) {
			t.Fatalf("row %d name json=%v arrow=%v", i, jsonOut.Rows[i][0], arrowRows[i][0])
		}
		if numericCell(t, jsonOut.Rows[i][1]) != numericCell(t, arrowRows[i][1]) {
			t.Fatalf("row %d value json=%v arrow=%v", i, jsonOut.Rows[i][1], arrowRows[i][1])
		}
	}
}

func TestSQLArrowEmptyResultSchemaOnly(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	body := `{"sql":"SELECT * FROM metrics WHERE false"}`
	resp := postSQLArrow(t, sqlURL(srv.URL, tenantSQLA), body, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	schema, rows, _ := decodeArrowStream(t, bytes.NewReader(raw))
	if schema == nil || len(schema.Fields()) == 0 {
		t.Fatal("expected schema with columns")
	}
	if len(rows) != 0 {
		t.Fatalf("rows=%d want 0", len(rows))
	}
}

func TestSQLArrowRowCapTruncated(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engineNewForSQLTest(t, dataDir, start)

	var rows []testparquet.Row
	for i := 0; i < 50; i++ {
		rows = append(rows, testparquet.Row{Name: "m", Labels: "{}", Value: float64(i), TimestampMs: int64(i)})
	}
	seedTenantMetrics(t, eng, dataDir, tenantSQLA, rows)

	cfg := sqlConfig(dataDir, func(c *query.SQLConfig) { c.MaxRows = 10 })
	srv := testSQLServer(t, dataDir, cfg, eng)

	body := `{"sql":"SELECT * FROM metrics ORDER BY timestamp_ms","max_rows":10}`
	resp := postSQLArrow(t, sqlURL(srv.URL, tenantSQLA), body, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	_, arrowRows, _ := decodeArrowStream(t, bytes.NewReader(raw))
	trunc := resp.Trailer.Get("X-Prism-Truncated")
	if trunc == "" {
		trunc = resp.Header.Get("X-Prism-Truncated")
	}
	if trunc != "true" {
		t.Fatalf("X-Prism-Truncated=%q want true", trunc)
	}
	if len(arrowRows) != 10 {
		t.Fatalf("row count=%d want 10", len(arrowRows))
	}
}

func TestSQLArrowDefaultJSONWithoutAccept(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	body := `{"sql":"SELECT COUNT(*) AS c FROM metrics"}`
	resp := postSQLArrowNoAccept(t, sqlURL(srv.URL, tenantSQLA), body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Fatalf("content-type=%q want json", ct)
	}
	var out sqlResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
}

func TestSQLArrowRBACReader200(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, env := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp := postSQLArrow(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT COUNT(*) FROM metrics"}`, tok)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
}

func TestSQLArrowRBACWriter403(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, env := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	tok, err := env.SignToken(authtest.WithSubject("writer-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp := postSQLArrow(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT 1"}`, tok)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d want 403", resp.StatusCode)
	}
}

func TestSQLArrowRBACUnbound404(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, env := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	tok, err := env.SignToken(authtest.WithSubject("reader-a"))
	if err != nil {
		t.Fatal(err)
	}
	resp := postSQLArrow(t, sqlURL(srv.URL, tenantSQLB), `{"sql":"SELECT 1"}`, tok)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
}

func TestSQLArrowRBACMissingJWT401(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv, _ := testSQLServerRBAC(t, dataDir, eng, rbacPolicySQL())
	resp := postSQLArrow(t, sqlURL(srv.URL, tenantSQLA), `{"sql":"SELECT 1"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", resp.StatusCode)
	}
}

func TestSQLArrowIsolationCrossTenant(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	bGlob := filepath.Join(dataDir, tenantSQLB, "tiers", "L0", "*.parquet")
	bEngine := filepath.Join(dataDir, tenantSQLB, "engine.duckdb")
	outPath := filepath.Join(dataDir, tenantSQLA, "escape.parquet")
	otherPath := filepath.ToSlash(filepath.Join(dataDir, tenantSQLB, "hot", "current.parquet"))

	attacks := []string{
		fmt.Sprintf("SELECT * FROM read_parquet('%s')", filepath.ToSlash(bGlob)),
		fmt.Sprintf("SELECT * FROM read_parquet('%s')", otherPath),
		fmt.Sprintf("ATTACH '%s' AS stolen", filepath.ToSlash(bEngine)),
		fmt.Sprintf("COPY (SELECT 1) TO '%s'", filepath.ToSlash(outPath)),
		"SELECT * FROM read_csv('/etc/passwd')",
		"SELECT * FROM read_parquet('/etc/passwd')",
	}
	for _, sqlText := range attacks {
		t.Run(sqlText, func(t *testing.T) {
			execSQLArrowExpect400(t, srv, tenantSQLA, sqlText)
		})
	}

	code, _, rows, _ := execSQLArrow(t, srv, tenantSQLA, "SELECT COUNT(*) AS c FROM metrics")
	if code != http.StatusOK {
		t.Fatalf("count status=%d", code)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%v", rows)
	}
	if numericCell(t, rows[0][0]) != 3 {
		t.Fatalf("tenant A count=%v want 3", rows[0][0])
	}
}

func execSQLArrowExpect400(t *testing.T, srv *httptest.Server, tenant, sqlText string) {
	t.Helper()
	body := fmt.Sprintf(`{"sql":%q}`, sqlText)
	resp := postSQLArrow(t, sqlURL(srv.URL, tenant), body, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 400 sql=%q body=%s", resp.StatusCode, sqlText, b)
	}
	raw, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(raw), "PK") || looksLikeArrowIPC(raw) {
		t.Fatalf("unexpected arrow stream body for sql=%q", sqlText)
	}
	if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, arrowStreamAccept) {
		t.Fatalf("content-type=%q want non-arrow", ct)
	}
}

func looksLikeArrowIPC(raw []byte) bool {
	if len(raw) < 4 {
		return false
	}
	// Arrow IPC stream magic: continuation marker 0xFFFFFFFF then schema/message markers.
	return raw[0] == 0xff && raw[1] == 0xff && raw[2] == 0xff && raw[3] == 0xff
}

func TestClusterSQLArrowRBACDenyBeforeProxy(t *testing.T) {
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
	resp := postSQLArrow(t, srv.URL+"/"+tenantSQLB+"/sql", `{"sql":"SELECT 1"}`, tok)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
	if hits != 0 {
		t.Fatalf("upstream hits=%d want 0", hits)
	}
}

func TestSQLArrowPreStreamError400(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	body := `{"sql":"SELECT * FROM nosuch_relation"}`
	resp := postSQLArrow(t, sqlURL(srv.URL, tenantSQLA), body, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
}

func TestSQLArrowUnknownTenant404(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	resp := postSQLArrow(t, sqlURL(srv.URL, "INVALID!"), `{"sql":"SELECT 1"}`, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	msg, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(msg), storetenant.UnknownTenantBody) {
		t.Fatalf("body=%q", msg)
	}
}

func TestSQLArrowTrailerDeclared(t *testing.T) {
	dataDir, eng := twoTenantFixture(t)
	srv := testSQLServer(t, dataDir, nil, eng)

	body := `{"sql":"SELECT COUNT(*) AS c FROM metrics"}`
	resp := postSQLArrow(t, sqlURL(srv.URL, tenantSQLA), body, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	trailers := resp.Header.Get("Trailer")
	if trailers != "" && !strings.Contains(trailers, "X-Prism-Truncated") {
		t.Fatalf("Trailer header=%q want X-Prism-Truncated", trailers)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = decodeArrowStream(t, bytes.NewReader(raw))
	trunc := resp.Trailer.Get("X-Prism-Truncated")
	if trunc == "" {
		trunc = resp.Header.Get("X-Prism-Truncated")
	}
	if trunc != "false" {
		t.Fatalf("X-Prism-Truncated=%q want false", trunc)
	}
}

// engineNewForSQLTest wraps engine.New with cleanup for arrow-only tests.
func engineNewForSQLTest(t *testing.T, dataDir string, start time.Time) *engine.Engine {
	t.Helper()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}
