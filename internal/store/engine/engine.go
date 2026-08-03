package engine

import (
	"container/list"
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

const (
	hotCurrentTable = "hot_current"
	hotPrevTable    = "hot_prev"
	defaultLRUSize  = 32
)

// Config holds per-tenant DuckDB engine settings.
type Config struct {
	DataDir        string
	HotWindow      time.Duration
	MaxOpenTenants int
	RowGroupSize   int
	Threads        int
	MemoryLimit    string
}

// Engine manages lazy-open DuckDB databases per tenant namespace.
type Engine struct {
	cfg   Config
	clock func() time.Time

	mu      sync.Mutex
	lru     *tenantLRU
	flushAt map[string]time.Time // next scheduled flush instant per tenant
}

// New builds an Engine. now defaults to time.Now when nil.
func New(cfg Config, now func() time.Time) *Engine {
	if cfg.HotWindow <= 0 {
		cfg.HotWindow = 10 * time.Minute
	}
	if cfg.MaxOpenTenants <= 0 {
		cfg.MaxOpenTenants = defaultLRUSize
	}
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 1_000_000
	}
	if now == nil {
		now = time.Now
	}
	return &Engine{
		cfg:     cfg,
		clock:   now,
		lru:     newTenantLRU(cfg.MaxOpenTenants),
		flushAt: make(map[string]time.Time),
	}
}

// Ingest inserts a parquet window body into hot_current, stamping ts=now().
// Empty bodies are a no-op. Returns rows inserted (0 for empty).
func (e *Engine) Ingest(tenant string, body io.Reader) (int64, error) {
	tmp, n, err := writeTempParquet(body)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	defer func() { _ = os.Remove(tmp) }()

	if err := e.maybeFlushDue(tenant); err != nil {
		return 0, err
	}

	te, err := e.open(tenant)
	if err != nil {
		return 0, err
	}
	ts := e.clock().UTC()
	// DuckDB binds scalar query parameters; read_parquet file paths must be literal strings.
	//nolint:gosec // G201: hot table name is a package const; parquet path is server-controlled and cannot be bound.
	q := fmt.Sprintf(`
		INSERT INTO %s
		SELECT "__name__", labels, value, timestamp_ms, CAST(? AS TIMESTAMP) AS ts
		FROM read_parquet('%s')
	`, hotCurrentTable, escapePath(tmp))
	te.mu.Lock()
	res, err := te.db.ExecContext(context.Background(), q, ts)
	te.mu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("engine: insert: %w", err)
	}
	affected, _ := res.RowsAffected()
	e.scheduleFlush(tenant)
	return affected, nil
}

// logArtifactPattern guards the artifact segment used in a landing path so a
// crafted artifact name cannot escape <tenant>/logs/. It admits the whole
// logs-* family (logs-raw, logs-template, logs-summary, and hyphenated variants)
// while rejecting path separators, dots, and other unsafe characters.
var logArtifactPattern = regexp.MustCompile(`^logs-[a-z0-9-]+$`)

// LandLogWindow persists a logs-* artifact window as an immutable parquet file
// under <tenant>/logs/<artifact>/, bypassing the metrics hot catalog. Logs carry
// a variable, per-format schema, so they are stored as files and read back with
// union_by_name at query time rather than inserted into the fixed metrics hot
// table. Empty bodies are a no-op. Returns bytes written (0 for empty).
func (e *Engine) LandLogWindow(tenant, artifact string, body io.Reader) (int64, error) {
	if !storetenant.TenantAllowed(tenant) {
		return 0, fmt.Errorf("engine: invalid tenant %q", tenant)
	}
	if !logArtifactPattern.MatchString(artifact) {
		return 0, fmt.Errorf("engine: invalid log artifact %q", artifact)
	}
	dir := filepath.Join(e.cfg.DataDir, tenant, "logs", artifact)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(dir, ".window-*.parquet.tmp")
	if err != nil {
		return 0, err
	}
	tmpPath := tmp.Name()
	n, copyErr := io.Copy(tmp, body)
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return 0, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return 0, closeErr
	}
	if n == 0 {
		_ = os.Remove(tmpPath)
		return 0, nil
	}
	final := filepath.Join(dir, segmentName(e.clock()))
	if err := os.Rename(tmpPath, final); err != nil {
		_ = os.Remove(tmpPath)
		return 0, fmt.Errorf("engine: land log window: %w", err)
	}
	return n, nil
}

// FlushDue rolls hot tables and writes L0 segments for tenants whose hot window elapsed.
func (e *Engine) FlushDue() error {
	e.mu.Lock()
	tenants := make([]string, 0, len(e.flushAt))
	now := e.clock()
	for ns, due := range e.flushAt {
		if !now.Before(due) {
			tenants = append(tenants, ns)
		}
	}
	e.mu.Unlock()

	for _, ns := range tenants {
		if err := e.flushTenant(ns); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) maybeFlushDue(tenant string) error {
	e.mu.Lock()
	due, ok := e.flushAt[tenant]
	now := e.clock()
	should := ok && !now.Before(due)
	e.mu.Unlock()
	if should {
		return e.flushTenant(tenant)
	}
	return nil
}

func (e *Engine) scheduleFlush(tenant string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.flushAt[tenant]; !ok {
		e.flushAt[tenant] = e.clock().Add(e.cfg.HotWindow)
	}
}

func (e *Engine) flushTenant(tenant string) error {
	te, err := e.open(tenant)
	if err != nil {
		return err
	}
	te.mu.Lock()
	defer te.mu.Unlock()
	if _, err := te.db.ExecContext(context.Background(), fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		ALTER TABLE %s RENAME TO %s;
	`, hotPrevTable, hotCurrentTable, hotPrevTable)); err != nil {
		if !tableMissing(err.Error()) {
			return fmt.Errorf("engine: roll hot: %w", err)
		}
	}
	if err := te.ensureHotCurrent(); err != nil {
		return err
	}

	var count int
	if err := te.db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", hotPrevTable)).Scan(&count); err != nil {
		if tableMissing(err.Error()) {
			e.clearFlushSchedule(tenant)
			return nil
		}
		return fmt.Errorf("engine: count hot_prev: %w", err)
	}
	if count == 0 {
		_, _ = te.db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", hotPrevTable))
		e.clearFlushSchedule(tenant)
		return nil
	}

	l0Dir := tierDir(e.cfg.DataDir, tenant, 0)
	if err := os.MkdirAll(l0Dir, 0o750); err != nil {
		return err
	}
	final := filepath.Join(l0Dir, segmentName(e.clock()))
	selectSQL := fmt.Sprintf("SELECT * FROM %s ORDER BY ts", hotPrevTable)
	if err := atomicCopyTo(te.db, selectSQL, final, e.cfg.RowGroupSize); err != nil {
		return fmt.Errorf("engine: copy L0: %w", err)
	}
	if _, err := te.db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", hotPrevTable)); err != nil {
		return fmt.Errorf("engine: drop hot_prev: %w", err)
	}
	e.clearFlushSchedule(tenant)
	return nil
}

func (e *Engine) clearFlushSchedule(tenant string) {
	e.mu.Lock()
	delete(e.flushAt, tenant)
	e.mu.Unlock()
}

// HotRowCount returns rows in hot_current for tests and stats.
func (e *Engine) HotRowCount(tenant string) (int64, error) {
	te, err := e.open(tenant)
	if err != nil {
		return 0, err
	}
	te.mu.RLock()
	defer te.mu.RUnlock()
	var n int64
	err = te.db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", hotCurrentTable)).Scan(&n)
	if tableMissing(errString(err)) {
		return 0, nil
	}
	return n, err
}

// QueryHotTs returns ts values from hot_current ordered by ts (tests).
func (e *Engine) QueryHotTs(tenant string) ([]time.Time, error) {
	te, err := e.open(tenant)
	if err != nil {
		return nil, err
	}
	te.mu.RLock()
	defer te.mu.RUnlock()
	rows, err := te.db.QueryContext(context.Background(), fmt.Sprintf("SELECT ts FROM %s ORDER BY ts", hotCurrentTable))
	if err != nil {
		if tableMissing(err.Error()) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []time.Time
	for rows.Next() {
		var ts time.Time
		if err := rows.Scan(&ts); err != nil {
			return nil, err
		}
		out = append(out, ts.UTC())
	}
	return out, rows.Err()
}

// DB returns the tenant DuckDB handle for read/query integration (caller must not close).
// The handle is unsynchronized: a read issued directly on it while the background
// lifecycle is flushing can observe the hot table mid-rename. Reads that run
// concurrently with writes should instead use the read-locked accessor.
func (e *Engine) DB(tenant string) (*sql.DB, error) {
	te, err := e.open(tenant)
	if err != nil {
		return nil, err
	}
	return te.db, nil
}

// WithRead runs fn against the tenant database under a shared read lock, so a
// query is serialized against writes (ingest and the flush rename dance) while
// still allowing other concurrent readers.
func (e *Engine) WithRead(tenant string, fn func(*sql.DB) error) error {
	te, err := e.open(tenant)
	if err != nil {
		return err
	}
	te.mu.RLock()
	defer te.mu.RUnlock()
	return fn(te.db)
}

// Close evicts all open tenant databases.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lru.closeAll()
}

func (e *Engine) open(tenant string) (*tenantEntry, error) {
	if !storetenant.TenantAllowed(tenant) {
		return nil, fmt.Errorf("engine: invalid tenant %q", tenant)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if ent, ok := e.lru.get(tenant); ok {
		return ent, nil
	}
	ent, err := openTenant(e.cfg.DataDir, tenant, e.cfg)
	if err != nil {
		return nil, err
	}
	if err := e.importLegacyMetricsRaw(tenant); err != nil {
		_ = ent.db.Close()
		return nil, err
	}
	if err := ent.ensureHotCurrent(); err != nil {
		_ = ent.db.Close()
		return nil, err
	}
	e.lru.add(tenant, ent)
	return ent, nil
}

type tenantEntry struct {
	db   *sql.DB
	path string
	// mu serializes access to the embedded database: a flush is a multi-statement
	// catalog sequence (rename the hot table aside, recreate it), so a write must
	// hold this exclusively while reads take it shared. Overlapping a write with
	// any other operation lets a statement observe the table mid-rename.
	mu sync.RWMutex
}

// segmentName builds a collision-free L0 filename. The ingest instant orders
// segments by time, and a random suffix keeps two flushes that land within the
// same clock resolution from overwriting each other's output on rename.
func segmentName(now time.Time) string {
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	return fmt.Sprintf("%d-%s.parquet", now.UnixNano(), hex.EncodeToString(buf[:]))
}

func openTenant(dataDir, tenant string, cfg Config) (*tenantEntry, error) {
	dir := filepath.Join(dataDir, tenant)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "engine.duckdb")
	var initFn func(driver.ExecerContext) error
	if cfg.Threads > 0 || cfg.MemoryLimit != "" {
		threads := cfg.Threads
		memLimit := cfg.MemoryLimit
		initFn = func(exec driver.ExecerContext) error {
			ctx := context.Background()
			if threads > 0 {
				q := fmt.Sprintf("SET threads=%d", threads)
				if _, err := exec.ExecContext(ctx, q, nil); err != nil {
					return fmt.Errorf("engine: set threads: %w", err)
				}
			}
			if memLimit != "" {
				q := fmt.Sprintf("SET memory_limit='%s'", strings.ReplaceAll(memLimit, "'", "''"))
				if _, err := exec.ExecContext(ctx, q, nil); err != nil {
					return fmt.Errorf("engine: set memory_limit: %w", err)
				}
			}
			return nil
		}
	}
	connector, err := duckdb.NewConnector(path, initFn)
	if err != nil {
		return nil, fmt.Errorf("engine: open duckdb: %w", err)
	}
	db := sql.OpenDB(connector)
	// Pin the tenant store to a single connection: it is a single writer, and
	// routing statements across pooled connections lets a reader observe a
	// catalog snapshot from before a committed write on another connection.
	db.SetMaxOpenConns(1)
	return &tenantEntry{db: db, path: path}, nil
}

func (te *tenantEntry) ensureHotCurrent() error {
	schema := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			"__name__" VARCHAR,
			labels VARCHAR,
			value DOUBLE,
			timestamp_ms BIGINT,
			ts TIMESTAMP
		)
	`, hotCurrentTable)
	if _, err := te.db.ExecContext(context.Background(), schema); err != nil {
		return err
	}
	prev := strings.Replace(schema, hotCurrentTable, hotPrevTable, 1)
	_, err := te.db.ExecContext(context.Background(), prev)
	return err
}

func tierDir(dataDir, tenant string, tier int) string {
	return filepath.Join(dataDir, tenant, "tiers", fmt.Sprintf("L%d", tier))
}

func writeTempParquet(body io.Reader) (path string, n int64, err error) {
	f, err := os.CreateTemp("", "prism-window-*.parquet")
	if err != nil {
		return "", 0, err
	}
	path = f.Name()
	n, copyErr := io.Copy(f, body)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(path)
		return "", 0, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return "", 0, closeErr
	}
	if n == 0 {
		_ = os.Remove(path)
		return "", 0, nil
	}
	return path, n, nil
}

func escapePath(p string) string {
	return filepath.ToSlash(p)
}

func tableMissing(msg string) bool {
	return msg != "" &&
		(strings.Contains(msg, "does not exist") || strings.Contains(msg, "Catalog Error"))
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// atomicCopyTo runs COPY (selectSQL) TO finalPath via a temp file and atomic rename.
func atomicCopyTo(db *sql.DB, selectSQL, finalPath string, rowGroupSize int, args ...any) error {
	// A per-call random suffix keeps concurrent writers to the same finalPath
	// (e.g. simultaneous hot-snapshot exports for one tenant) from clobbering a
	// shared temp file; the atomic rename then publishes a complete file.
	var suffix [4]byte
	_, _ = rand.Read(suffix[:])
	tmp := fmt.Sprintf("%s.%s.tmp", finalPath, hex.EncodeToString(suffix[:]))
	copySQL := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE %d)`,
		selectSQL, escapePath(tmp), rowGroupSize,
	)
	if _, err := db.ExecContext(context.Background(), copySQL, args...); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, finalPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("engine: rename: %w", err)
	}
	return nil
}

// tenantLRU bounds open DuckDB connections.
type tenantLRU struct {
	max   int
	items map[string]*list.Element
	order *list.List
}

type lruItem struct {
	tenant string
	entry  *tenantEntry
}

func newTenantLRU(max int) *tenantLRU {
	return &tenantLRU{max: max, items: make(map[string]*list.Element), order: list.New()}
}

func (l *tenantLRU) get(tenant string) (*tenantEntry, bool) {
	el, ok := l.items[tenant]
	if !ok {
		return nil, false
	}
	l.order.MoveToFront(el)
	return el.Value.(*lruItem).entry, true
}

func (l *tenantLRU) add(tenant string, ent *tenantEntry) {
	if el, ok := l.items[tenant]; ok {
		el.Value.(*lruItem).entry = ent
		l.order.MoveToFront(el)
		return
	}
	el := l.order.PushFront(&lruItem{tenant: tenant, entry: ent})
	l.items[tenant] = el
	for l.order.Len() > l.max {
		l.evictOldest()
	}
}

func (l *tenantLRU) evictOldest() {
	el := l.order.Back()
	if el == nil {
		return
	}
	item := el.Value.(*lruItem)
	_ = item.entry.db.Close()
	delete(l.items, item.tenant)
	l.order.Remove(el)
}

func (l *tenantLRU) closeAll() error {
	for l.order.Len() > 0 {
		l.evictOldest()
	}
	return nil
}

// ListL0 returns L0 segment paths for a tenant.
func ListL0(dataDir, tenant string) ([]string, error) {
	dir := tierDir(dataDir, tenant, 0)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".parquet" && !strings.HasPrefix(e.Name(), ".") {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}
	return paths, nil
}
