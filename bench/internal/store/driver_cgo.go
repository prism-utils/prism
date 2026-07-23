//go:build cgo

package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/elk-utilities/prism/bench/internal/gen"
	"github.com/elk-utilities/prism/internal/store/engine"
	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/lifecycle"
	"github.com/elk-utilities/prism/internal/store/seed"
)

type cgoDriver struct {
	cfg    Config
	eng    *engine.Engine
	runner *lifecycle.Runner
	cmd    *exec.Cmd
	logger *slog.Logger
}

func newDriver(cfg Config) (Driver, error) { //nolint:gocritic // Config matches existing driver constructor style.
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("store: empty data dir")
	}
	if cfg.Tenant == "" {
		cfg.Tenant = "bench-tenant"
	}
	if cfg.ListenAddr == "" {
		port, err := freePort(context.Background())
		if err != nil {
			return nil, fmt.Errorf("store: pick port: %w", err)
		}
		cfg.ListenAddr = fmt.Sprintf("127.0.0.1:%d", port)
	}
	return &cgoDriver{cfg: cfg, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil
}

func freePort(ctx context.Context) (int, error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func (d *cgoDriver) Start(ctx context.Context) error {
	if err := os.MkdirAll(d.cfg.DataDir, 0o750); err != nil {
		return fmt.Errorf("store: mkdir data: %w", err)
	}
	if err := seed.EnsureTieredLayoutForTenant(d.cfg.DataDir, d.cfg.Tenant); err != nil { //nolint:contextcheck // seed API has no context parameter.
		return fmt.Errorf("store: seed tenant: %w", err)
	}
	return d.startServerProcess(ctx)
}

func (d *cgoDriver) StartServer(ctx context.Context) error {
	if err := d.closeEngine(); err != nil {
		return err
	}
	return d.startServerProcess(ctx)
}

func (d *cgoDriver) StopServer(ctx context.Context) error {
	if err := d.stopServer(ctx); err != nil {
		return err
	}
	return d.closeEngine()
}

func (d *cgoDriver) BaseURL() string {
	return "http://" + d.cfg.ListenAddr
}

func (d *cgoDriver) startServerProcess(ctx context.Context) error {
	if d.cmd != nil && d.cmd.Process != nil {
		return fmt.Errorf("store: prism-store already running")
	}
	bin := d.cfg.StoreBin
	if bin == "" {
		bin = "prism-store"
	}
	d.cmd = exec.CommandContext(ctx, bin, "serve") //nolint:gosec // operator-built binary path
	d.cmd.Env = d.serverEnv()
	d.cmd.Stdout = os.Stderr
	d.cmd.Stderr = os.Stderr
	if err := d.cmd.Start(); err != nil {
		return fmt.Errorf("store: start prism-store: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- d.cmd.Wait() }()
	if err := waitHTTP(ctx, d.BaseURL()+"/healthz", 30*time.Second); err != nil {
		return err
	}
	select {
	case err := <-exited:
		return fmt.Errorf("store: prism-store exited during startup: %w", err)
	default:
	}
	return nil
}

func (d *cgoDriver) serverEnv() []string {
	env := append(os.Environ(),
		"LISTEN_ADDR="+d.cfg.ListenAddr,
		"DATA_DIR="+d.cfg.DataDir,
		"AUTH_MODE=none",
		"ALLOWED_ARTIFACTS=metrics-raw",
		"HOT_WINDOW_SECONDS=3600",
		"FLUSH_TICK_SECONDS=3600",
		"MERGE_TICK_SECONDS=3600",
	)
	if d.cfg.RBAC != nil {
		env = append(env,
			"AUTHZ_POLICY_FILE="+d.cfg.RBAC.PolicyFile,
			"OIDC_ISSUER="+d.cfg.RBAC.Issuer,
			"OIDC_JWKS_FILE="+d.cfg.RBAC.JWKSFile,
			"OIDC_AUDIENCE="+d.cfg.RBAC.Audience,
		)
	}
	if d.cfg.Budget.IsSet() {
		env = append(env,
			"GOMAXPROCS="+strconv.Itoa(d.cfg.Budget.Threads()),
			"GOMEMLIMIT="+d.cfg.Budget.GoMemLimit(),
			"DUCKDB_THREADS="+strconv.Itoa(d.cfg.Budget.Threads()),
			"DUCKDB_MEMORY_LIMIT="+d.cfg.Budget.DuckDBMemoryLimit(),
		)
	}
	return env
}

func (d *cgoDriver) closeEngine() error {
	if d.eng == nil {
		return nil
	}
	err := d.eng.Close()
	d.eng = nil
	d.runner = nil
	return err
}

func (d *cgoDriver) Stop(ctx context.Context) error {
	if err := d.stopServer(ctx); err != nil {
		return err
	}
	return d.closeEngine()
}

func (d *cgoDriver) openEngine() error {
	if d.eng != nil {
		return nil
	}
	engCfg := engine.Config{
		DataDir:   d.cfg.DataDir,
		HotWindow: time.Hour,
	}
	if d.cfg.Budget.IsSet() {
		engCfg.Threads = d.cfg.Budget.Threads()
		engCfg.MemoryLimit = d.cfg.Budget.DuckDBMemoryLimit()
	}
	d.eng = engine.New(engCfg, time.Now)
	d.runner = lifecycle.NewRunner(lifecycle.Config{
		DataDir:         d.cfg.DataDir,
		SegmentsPerTier: 6,
		MaxSegmentBytes: 512 << 20,
		FloorBytes:      lifecycle.FloorBytesFromHotWindow(time.Hour),
		RetentionDays:   15,
		RollupSteps:     "1m,5m,1h",
		MaxTier:         8,
	}, d.eng, time.Now)
	return nil
}

func (d *cgoDriver) IngestMetricsHTTP(ctx context.Context, windows []string) error {
	base := "http://" + d.cfg.ListenAddr + "/" + d.cfg.Tenant + "/ingest/metrics-raw"
	client := &http.Client{Timeout: 10 * time.Minute}
	for _, path := range windows {
		f, err := os.Open(path) //nolint:gosec // bench-generated parquet paths
		if err != nil {
			return fmt.Errorf("store: open window: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base, f)
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("store: ingest request: %w", err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		if d.cfg.Token != "" {
			req.Header.Set("Authorization", "Bearer "+d.cfg.Token)
		}
		resp, err := client.Do(req)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("store: ingest post: %w", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			return fmt.Errorf("store: ingest status %d at %s", resp.StatusCode, path)
		}
	}
	return nil
}

func (d *cgoDriver) Compact(ctx context.Context) error {
	if d.cmd != nil {
		stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		if err := d.stopServer(stopCtx); err != nil {
			cancel()
			return err
		}
		cancel()
	}
	if err := d.openEngine(); err != nil {
		return err
	}
	if err := d.eng.FlushDue(); err != nil { //nolint:contextcheck // engine flush API has no context parameter.
		return fmt.Errorf("store: flush: %w", err)
	}
	for round := 0; round < 32; round++ {
		if err := d.runner.TickMerge(); err != nil {
			return fmt.Errorf("store: merge: %w", err)
		}
		segs, err := countTierSegments(d.cfg.DataDir, d.cfg.Tenant)
		if err != nil {
			return err
		}
		if segs <= 1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (d *cgoDriver) stopServer(ctx context.Context) error {
	if d.cmd == nil || d.cmd.Process == nil {
		return nil
	}
	_ = d.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- d.cmd.Wait() }()
	select {
	case <-ctx.Done():
		_ = d.cmd.Process.Kill()
	case <-done:
	}
	d.cmd = nil
	time.Sleep(200 * time.Millisecond)
	return nil
}

func countTierSegments(dataDir, tenant string) (int, error) {
	total := 0
	for tier := 0; tier < 8; tier++ {
		glob := filepath.Join(dataDir, tenant, "tiers", fmt.Sprintf("L%d", tier), "*.parquet")
		matches, err := filepath.Glob(glob)
		if err != nil {
			return 0, err
		}
		for _, m := range matches {
			if strings.HasSuffix(filepath.Base(m), seed.SeedName) {
				continue
			}
			total++
		}
	}
	return total, nil
}

func (d *cgoDriver) WriteLogsTier(_ context.Context, path string, rows []gen.LogRow) error {
	return gen.WriteLogsTier(path, rows)
}

func (d *cgoDriver) CountMetrics(ctx context.Context) (int64, error) {
	if err := d.openEngine(); err != nil {
		return 0, err
	}
	var count int64
	err := d.eng.WithRead(d.cfg.Tenant, func(db *sql.DB) error { //nolint:contextcheck // WithRead opens tenant DB without request ctx.
		inner, err := d.buildFullMetricsUnionInner(ctx, db)
		if err != nil {
			return err
		}
		q := "SELECT COUNT(*) FROM (" + inner + ")"
		return db.QueryRowContext(ctx, q).Scan(&count)
	})
	return count, err
}

func (d *cgoDriver) AggregateMetrics(ctx context.Context) error {
	if err := d.openEngine(); err != nil {
		return err
	}
	return d.eng.WithRead(d.cfg.Tenant, func(db *sql.DB) error { //nolint:contextcheck // WithRead opens tenant DB without request ctx.
		inner, err := d.buildFullMetricsUnionInner(ctx, db)
		if err != nil {
			return err
		}
		//nolint:gosec // G202: inner SQL unions hot and tier parquet sources built by this driver, not user text.
		q := `SELECT "__name__", avg(value), min(value), max(value), count(*)
		      FROM (` + inner + `) GROUP BY "__name__"`
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name string
			var avg, min, max float64
			var cnt int64
			if err := rows.Scan(&name, &avg, &min, &max, &cnt); err != nil {
				return err
			}
		}
		return rows.Err()
	})
}

func (d *cgoDriver) buildFullMetricsUnionInner(ctx context.Context, db *sql.DB) (string, error) {
	tenantRoot := filepath.Join(d.cfg.DataDir, d.cfg.Tenant)
	parts := []string{"SELECT * FROM hot_current"}
	if hotTableExists(ctx, db, "hot_prev") {
		parts = append(parts, "SELECT * FROM hot_prev")
	}
	for tier := 0; tier < 8; tier++ {
		glob := filepath.Join(tenantRoot, "tiers", fmt.Sprintf("L%d", tier), "*.parquet")
		if matches, _ := filepath.Glob(glob); len(matches) > 0 {
			parts = append(parts, fmt.Sprintf(
				"SELECT * FROM read_parquet('%s')",
				layout.ToSlash(glob),
			))
		}
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("store: no metrics sources for union")
	}
	return strings.Join(parts, " UNION ALL "), nil
}

func hotTableExists(ctx context.Context, db *sql.DB, table string) bool {
	//nolint:gosec // G201: table name is a fixed hot-catalog identifier, not user input.
	_, err := db.ExecContext(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", table))
	return err == nil
}

// CountLogsLike runs an engine-level DuckDB read_parquet scan over a logs-shaped
// tier. The shipping store has no logs ingest API; this measures the columnar
// engine approach only.
func (d *cgoDriver) CountLogsLike(ctx context.Context, logsGlob string, start, end time.Time) (int64, error) {
	if err := d.openEngine(); err != nil {
		return 0, err
	}
	var count int64
	err := d.eng.WithRead(d.cfg.Tenant, func(db *sql.DB) error { //nolint:contextcheck // WithRead opens tenant DB without request ctx.
		//nolint:gosec // G201: bounds and substring are harness-owned constants, not user input.
		q := fmt.Sprintf(`
			SELECT count(*) FROM read_parquet('%s')
			WHERE ts >= TIMESTAMPTZ '%s'
			  AND ts < TIMESTAMPTZ '%s'
			  AND message LIKE '%%%s%%'
		`, layout.ToSlash(logsGlob), duckTS(start), duckTS(end), gen.DeadlineSubstring)
		return db.QueryRowContext(ctx, q).Scan(&count)
	})
	if err != nil {
		return 0, fmt.Errorf("store: logs like: %w", err)
	}
	return count, nil
}

func (d *cgoDriver) DuckDBVersion(ctx context.Context) (string, error) {
	if err := d.openEngine(); err != nil {
		return "", err
	}
	var v string
	err := d.eng.WithRead(d.cfg.Tenant, func(db *sql.DB) error { //nolint:contextcheck // WithRead opens tenant DB without request ctx.
		return db.QueryRowContext(ctx, "SELECT version()").Scan(&v)
	})
	return v, err
}

func (d *cgoDriver) Pid() int {
	if d.cmd != nil && d.cmd.Process != nil {
		return d.cmd.Process.Pid
	}
	return 0
}

func (d *cgoDriver) CountMetricsAPI(ctx context.Context) (int64, error) {
	out, err := d.postSQL(ctx, `SELECT COUNT(*) AS n FROM metrics`)
	if err != nil {
		return 0, err
	}
	if len(out.Rows) == 0 || len(out.Rows[0]) == 0 {
		return 0, fmt.Errorf("store: sql count: empty rows")
	}
	n, err := sqlCellInt64(out.Rows[0][0])
	if err != nil {
		return 0, fmt.Errorf("store: sql count cell: %w", err)
	}
	return n, nil
}

func (d *cgoDriver) AggregateMetricsAPI(ctx context.Context) error {
	out, err := d.postSQL(ctx, `SELECT "__name__", avg(value), min(value), max(value), count(*) FROM metrics GROUP BY "__name__"`)
	if err != nil {
		return err
	}
	for _, row := range out.Rows {
		if len(row) < 5 {
			return fmt.Errorf("store: sql aggregate: short row")
		}
	}
	return nil
}

func (d *cgoDriver) CountMetricsArrowAPI(ctx context.Context) (int64, error) {
	rows, err := d.postSQLArrow(ctx, `SELECT COUNT(*) AS n FROM metrics`)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return 0, fmt.Errorf("store: arrow count: empty rows")
	}
	return arrowCellInt64(rows[0][0])
}

func (d *cgoDriver) AggregateMetricsArrowAPI(ctx context.Context) error {
	rows, err := d.postSQLArrow(ctx, `SELECT "__name__", avg(value), min(value), max(value), count(*) FROM metrics GROUP BY "__name__"`)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if len(row) < 5 {
			return fmt.Errorf("store: arrow aggregate: short row")
		}
	}
	return nil
}

func (d *cgoDriver) ScanMetricsArrowAPI(ctx context.Context, sqlText string) (int64, error) {
	rows, err := d.postSQLArrow(ctx, sqlText)
	if err != nil {
		return 0, err
	}
	return int64(len(rows)), nil
}

func (d *cgoDriver) postSQLArrow(ctx context.Context, sqlText string) ([][]any, error) {
	url := d.BaseURL() + "/" + d.cfg.Tenant + "/sql"
	body, err := json.Marshal(map[string]string{"sql": sqlText})
	if err != nil {
		return nil, fmt.Errorf("store: marshal sql request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("store: sql request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.apache.arrow.stream")
	if d.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+d.cfg.Token)
	}
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("store: sql post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("store: sql status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("store: arrow read body: %w", err)
	}
	return decodeArrowRows(bytes.NewReader(raw))
}

func decodeArrowRows(r io.Reader) ([][]any, error) {
	reader, err := ipc.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("store: arrow reader: %w", err)
	}
	defer reader.Release()
	var rows [][]any
	for reader.Next() {
		rec := reader.RecordBatch()
		for rowIdx := int64(0); rowIdx < rec.NumRows(); rowIdx++ {
			row := make([]any, rec.NumCols())
			for colIdx := int64(0); colIdx < rec.NumCols(); colIdx++ {
				row[colIdx] = arrowBenchCellAt(rec, colIdx, rowIdx)
			}
			rows = append(rows, row)
		}
		rec.Release()
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("store: arrow read: %w", err)
	}
	return rows, nil
}

func arrowBenchCellAt(rec arrow.RecordBatch, col, row int64) any {
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
	default:
		return fmt.Sprint(colArr)
	}
}

func arrowCellInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

type sqlAPIResponse struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
}

func (d *cgoDriver) postSQL(ctx context.Context, sqlText string) (*sqlAPIResponse, error) {
	url := d.BaseURL() + "/" + d.cfg.Tenant + "/sql"
	body, err := json.Marshal(map[string]string{"sql": sqlText})
	if err != nil {
		return nil, fmt.Errorf("store: marshal sql request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("store: sql request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if d.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+d.cfg.Token)
	}
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("store: sql post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("store: sql read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("store: sql status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out sqlAPIResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("store: sql decode: %w", err)
	}
	if len(out.Columns) == 0 {
		return nil, fmt.Errorf("store: sql response missing columns")
	}
	return &out, nil
}

func sqlCellInt64(v any) (int64, error) {
	switch n := v.(type) {
	case float64:
		return int64(n), nil
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			f, err := n.Float64()
			if err != nil {
				return 0, err
			}
			return int64(f), nil
		}
		return i, nil
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

func duckTS(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.999999Z")
}

func waitHTTP(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("store: %s not ready", url)
}
