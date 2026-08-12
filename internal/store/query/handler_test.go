package query_test

import (
	"context"
	"database/sql"
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

	"github.com/prism-utils/prism/internal/store/engine"
	"github.com/prism-utils/prism/internal/store/query"
	"github.com/prism-utils/prism/internal/store/testparquet"
)

const testTenant = "user-6f3a9c2b-apps"

func testQueryServer(t *testing.T, dataDir string, exposeSQL bool) (*httptest.Server, *engine.Engine) {
	t.Helper()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: 50 * time.Millisecond}, time.Now)
	t.Cleanup(func() { _ = eng.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	cfg := &query.Config{DataDir: dataDir, ExposeSQL: exposeSQL}
	mux.Handle(query.QueryRoutePattern(""), query.Handler(cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, eng
}

func queryURL(base, tenant, start, end string) string {
	return base + "/" + tenant + "/query?start=" + start + "&end=" + end
}

func doQueryReq(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func TestHandlerMissingStartEnd400(t *testing.T) {
	srv, _ := testQueryServer(t, t.TempDir(), false)
	resp := doQueryReq(t, srv.URL+"/"+testTenant+"/query")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d want 400", resp.StatusCode)
	}
}

func TestHandlerInvalidRFC3339400(t *testing.T) {
	dataDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dataDir, testTenant), 0o750)
	srv, _ := testQueryServer(t, dataDir, false)
	resp := doQueryReq(t, queryURL(srv.URL, testTenant, "not-a-time", "2024-01-01T00:00:00Z"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d want 400", resp.StatusCode)
	}
}

func TestHandlerUnknownTenant404(t *testing.T) {
	srv, _ := testQueryServer(t, t.TempDir(), false)
	resp := doQueryReq(t, queryURL(srv.URL, "INVALID_TENANT!", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d want 404", resp.StatusCode)
	}
}

func TestHandlerMissingTenantRoot400(t *testing.T) {
	srv, _ := testQueryServer(t, t.TempDir(), false)
	start := "2024-01-01T00:00:00Z"
	end := "2024-01-02T00:00:00Z"
	resp := doQueryReq(t, queryURL(srv.URL, testTenant, start, end))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d want 400 bad query", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "bad query") {
		t.Fatalf("body: %s", body)
	}
}

func TestHandlerSuccess200JSON(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	f, _ := os.Open(path)
	if _, err := eng.Ingest(testTenant, f); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	_ = f.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	cfg := &query.Config{DataDir: dataDir, ExposeSQL: true}
	mux.Handle(query.QueryRoutePattern(""), query.Handler(cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	startS := start.Format(time.RFC3339)
	endS := start.Add(time.Hour).Format(time.RFC3339)
	resp := doQueryReq(t, queryURL(srv.URL, testTenant, startS, endS))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d body %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("content-type: %s", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"rows"`) {
		t.Fatalf("expected rows in body: %s", body)
	}
	if !strings.Contains(string(body), `"sql"`) {
		t.Fatalf("expected sql when ExposeSQL true: %s", body)
	}
}

func TestHandlerExecError500(t *testing.T) {
	dataDir := t.TempDir()
	start := time.Unix(1700000000, 0).UTC()
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: time.Hour}, func() time.Time { return start })
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	path := testparquet.WriteWindow(t, dir, "w.parquet", []testparquet.Row{
		{Name: "up", Labels: "{}", Value: 1, TimestampMs: 0},
	})
	f, _ := os.Open(path)
	if _, err := eng.Ingest(testTenant, f); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	_ = f.Close()

	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		_, err := db.ExecContext(context.Background(), "DROP TABLE hot_current")
		return err
	}); err != nil {
		t.Fatalf("drop hot_current: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	cfg := &query.Config{DataDir: dataDir}
	mux.Handle(query.QueryRoutePattern(""), query.Handler(cfg, eng, logger))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	startS := start.Format(time.RFC3339)
	endS := start.Add(time.Hour).Format(time.RFC3339)
	resp := doQueryReq(t, queryURL(srv.URL, testTenant, startS, endS))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d want 500 body %s", resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "query failed") {
		t.Fatalf("body: %s", body)
	}
}

func TestConcurrentQueryAndFlush(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	dataDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataDir, testTenant), 0o750); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	clk := start
	eng := engine.New(engine.Config{DataDir: dataDir, HotWindow: 50 * time.Millisecond}, func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clk
	})
	t.Cleanup(func() { _ = eng.Close() })

	dir := t.TempDir()
	const workers = 8
	const ingestsPerWorker = 10
	paths := make([]string, workers*ingestsPerWorker)
	for i := range paths {
		paths[i] = testparquet.WriteWindow(t, dir, fmt.Sprintf("w%d.parquet", i), []testparquet.Row{
			{Name: "up", Labels: "{}", Value: float64(i), TimestampMs: int64(i)},
		})
	}

	var errCount atomic.Int32
	var lastErr atomic.Value
	var wg sync.WaitGroup
	stopAdvance := make(chan struct{})

	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopAdvance:
				return
			case <-tick.C:
				mu.Lock()
				clk = clk.Add(15 * time.Millisecond)
				mu.Unlock()
			}
		}
	}()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < ingestsPerWorker; i++ {
				f, err := os.Open(paths[base*ingestsPerWorker+i])
				if err != nil {
					errCount.Add(1)
					return
				}
				if _, err := eng.Ingest(testTenant, f); err != nil {
					errCount.Add(1)
					lastErr.Store(err.Error())
					_ = f.Close()
					return
				}
				_ = f.Close()
			}
		}(w)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := eng.FlushDue(); err != nil {
				errCount.Add(1)
				lastErr.Store(err.Error())
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(2 * time.Second)
		b := query.Builder{DataDir: dataDir}
		for time.Now().Before(deadline) {
			if err := eng.WithRead(testTenant, func(db *sql.DB) error {
				sqlText, args, buildErr := b.BuildSQLWithDB(context.Background(), &query.Request{
					Tenant: testTenant,
					Start:  start.Add(-time.Hour),
					End:    start.Add(24 * time.Hour),
				}, db)
				if buildErr != nil {
					return buildErr
				}
				_, execErr := query.Execute(context.Background(), db, sqlText, args)
				return execErr
			}); err != nil {
				errCount.Add(1)
				lastErr.Store(err.Error())
				return
			}
			time.Sleep(3 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(stopAdvance)

	mu.Lock()
	clk = start.Add(time.Hour)
	mu.Unlock()
	if err := eng.FlushDue(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	if n := errCount.Load(); n != 0 {
		t.Fatalf("concurrent query/flush errors: %d last: %v", n, lastErr.Load())
	}

	wantRows := int64(workers * ingestsPerWorker)
	var rows []query.Row
	if err := eng.WithRead(testTenant, func(db *sql.DB) error {
		var err error
		qb := query.Builder{DataDir: dataDir}
		sqlText, args, err := qb.BuildSQLWithDB(context.Background(), &query.Request{
			Tenant: testTenant,
			Start:  start.Add(-time.Hour),
			End:    start.Add(24 * time.Hour),
		}, db)
		if err != nil {
			return err
		}
		rows, err = query.Execute(context.Background(), db, sqlText, args)
		return err
	}); err != nil {
		t.Fatalf("final query: %v", err)
	}
	if int64(len(rows)) != wantRows {
		t.Fatalf("gap-free row count: got %d want %d", len(rows), wantRows)
	}
}
