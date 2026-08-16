package merge

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/metricsmeta"
	"github.com/prism-utils/prism/internal/store/segformat"
)

// ExecutorConfig holds merge execution parameters.
type ExecutorConfig struct {
	DataDir              string
	Tenant               string
	RowGroupSize         int
	Threads              int
	MemoryLimit          string
	SegmentFormat        segformat.Format // parquet (default) or duckdb
	DuckDBStorageVersion string
	// DeleteGrace holds a merged-away source at its original path for this long
	// instead of unlinking it, so a reader that resolved the path before the
	// merge can still open it. Zero deletes as soon as the output is durable.
	DeleteGrace time.Duration
}

// Executor runs planned merges via DuckDB COPY / ATTACH export.
type Executor struct {
	cfg       ExecutorConfig
	db        *sql.DB
	connector *duckdb.Connector
}

// NewExecutor opens a temporary in-process DuckDB for merge COPY operations.
func NewExecutor(cfg ExecutorConfig) (*Executor, error) { //nolint:gocritic // Config options bag copied once at construction.
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 1_000_000
	}
	if cfg.SegmentFormat == "" {
		cfg.SegmentFormat = segformat.Parquet
	}
	if cfg.DuckDBStorageVersion == "" {
		cfg.DuckDBStorageVersion = segformat.DefaultStorageVersion
	}
	connector, err := newInMemoryConnector(DuckDBCaps{Threads: cfg.Threads, MemoryLimit: cfg.MemoryLimit})
	if err != nil {
		return nil, err
	}
	return &Executor{cfg: cfg, db: sql.OpenDB(connector), connector: connector}, nil
}

// Close releases the DuckDB connection.
func (x *Executor) Close() error {
	var err error
	if x.db != nil {
		err = x.db.Close()
		x.db = nil
	}
	if x.connector != nil {
		if cerr := x.connector.Close(); err == nil {
			err = cerr
		}
		x.connector = nil
	}
	return err
}

// DB exposes the executor DuckDB handle for tests and diagnostics.
func (x *Executor) DB() *sql.DB {
	return x.db
}

// ExecuteMerge merges sources into L{DestTier} with rows ordered by ts.
func (x *Executor) ExecuteMerge(action MergeAction, now time.Time) (Segment, error) {
	if len(action.Sources) == 0 {
		return Segment{}, fmt.Errorf("merge: no sources")
	}
	destTier := action.DestTier
	destDir := layout.TierDir(x.cfg.DataDir, x.cfg.Tenant, destTier)
	if err := os.MkdirAll(destDir, 0o750); err != nil {
		return Segment{}, err
	}
	final := filepath.Join(destDir, layout.SegmentNameFormat(now, x.cfg.SegmentFormat.Ext()))
	tmp := final + ".tmp"

	fromParts, cleanup, err := x.sourcesSelectSQL(action.Sources, segformat.MetricsTable)
	if err != nil {
		return Segment{}, err
	}
	defer cleanup()

	union := fromParts[0]
	for _, p := range fromParts[1:] {
		union += " UNION ALL " + p
	}
	selectSQL := fmt.Sprintf("SELECT * FROM (%s) ORDER BY ts", union)

	switch x.cfg.SegmentFormat {
	case segformat.DuckDB:
		if err := segformat.AtomicExportDuckDB(x.db, selectSQL, final, x.cfg.DuckDBStorageVersion, segformat.MetricsTable); err != nil {
			return Segment{}, fmt.Errorf("merge duckdb export: %w", err)
		}
	default:
		// DuckDB read_parquet paths must be literal strings; server-owned paths only.
		//nolint:gosec // G201: parquet paths are server-owned literals; DuckDB cannot bind file paths.
		copySQL := fmt.Sprintf(`
			COPY (%s) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE %d)
		`, selectSQL, layout.ToSlash(tmp), x.cfg.RowGroupSize)
		if _, err := x.db.ExecContext(context.Background(), copySQL); err != nil {
			_ = os.Remove(tmp)
			return Segment{}, fmt.Errorf("merge copy: %w", err)
		}
		if err := os.Rename(tmp, final); err != nil {
			_ = os.Remove(tmp)
			return Segment{}, err
		}
	}

	seg, err := StatSegment(final, destTier, DuckDBCaps{Threads: x.cfg.Threads, MemoryLimit: x.cfg.MemoryLimit})
	if err != nil {
		_ = os.Remove(final)
		return Segment{}, err
	}
	if err := retireSources(action.Sources, now, x.cfg.DeleteGrace); err != nil {
		return Segment{}, err
	}
	if err := metricsmeta.SyncAfterChange(x.cfg.DataDir, x.cfg.Tenant); err != nil {
		return Segment{}, fmt.Errorf("merge: metrics catalog: %w", err)
	}
	return seg, nil
}

func (x *Executor) sourcesSelectSQL(sources []Segment, duckTable string) ([]string, func(), error) {
	return x.sourcesSelectSQLWithTable(sources, func(string) string { return duckTable })
}

// sourcesSelectSQLLogs attaches duckdb log sources using LogsRelationForPath
// so agent landing windows (table "data") and tier segments (table "logs") both merge.
// Each arm projects __prism_ts_ns from the source window (or keeps an existing column).
func (x *Executor) sourcesSelectSQLLogs(sources []Segment) ([]string, func(), error) {
	var aliases []string
	cleanup := func() {
		for _, a := range aliases {
			_, _ = x.db.ExecContext(context.Background(), "DETACH "+a)
		}
	}
	parts := make([]string, len(sources))
	for i, s := range sources {
		ingestNs := s.MinTs.UTC().UnixNano()
		var fromSQL string
		switch {
		case segformat.IsDuckDB(s.Path):
			alias := fmt.Sprintf("msrc_%d", i)
			q := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", layout.ToSlash(s.Path), alias)
			if _, err := x.db.ExecContext(context.Background(), q); err != nil {
				cleanup()
				return nil, func() {}, fmt.Errorf("merge: attach source %s: %w", s.Path, err)
			}
			aliases = append(aliases, alias)
			fromSQL = fmt.Sprintf("SELECT * FROM %s.%s", alias, segformat.LogsRelationForPath(s.Path))
		default:
			fromSQL = fmt.Sprintf("SELECT * FROM read_parquet('%s')", layout.ToSlash(s.Path))
		}
		hasTS, err := x.relationHasColumn(fromSQL, logIngestTSColumn)
		if err != nil {
			cleanup()
			return nil, func() {}, fmt.Errorf("merge: describe source %s: %w", s.Path, err)
		}
		parts[i] = projectLogIngestTSSQL(fromSQL, ingestNs, hasTS)
	}
	return parts, cleanup, nil
}

// projectLogIngestTSSQL wraps a FROM-select so every row carries __prism_ts_ns.
// When the source already has the column, existing values win (COALESCE); otherwise
// the source window's ingest ns is stamped onto every row.
func projectLogIngestTSSQL(fromSQL string, ingestNs int64, hasTS bool) string {
	if hasTS {
		return fmt.Sprintf(
			`SELECT s.* EXCLUDE (%s), COALESCE(s.%s, %d::BIGINT) AS %s FROM (%s) AS s`,
			logIngestTSColumn, logIngestTSColumn, ingestNs, logIngestTSColumn, fromSQL,
		)
	}
	return fmt.Sprintf(
		`SELECT *, %d::BIGINT AS %s FROM (%s)`,
		ingestNs, logIngestTSColumn, fromSQL,
	)
}

// relationHasColumn reports whether a SELECT-shaped relation exposes column name.
func (x *Executor) relationHasColumn(fromSQL, column string) (bool, error) {
	// fromSQL is built from server-owned segment paths; column is a package constant.
	//nolint:gosec // G201: DuckDB DESCRIBE cannot use bound identifiers for relation SQL.
	q := fmt.Sprintf(`SELECT COUNT(*) > 0 FROM (DESCRIBE %s) WHERE column_name = '%s'`, fromSQL, column)
	var ok bool
	err := x.db.QueryRowContext(context.Background(), q).Scan(&ok)
	return ok, err
}

func (x *Executor) sourcesSelectSQLWithTable(sources []Segment, duckTable func(path string) string) ([]string, func(), error) {
	var aliases []string
	cleanup := func() {
		for _, a := range aliases {
			_, _ = x.db.ExecContext(context.Background(), "DETACH "+a)
		}
	}
	parts := make([]string, len(sources))
	for i, s := range sources {
		switch {
		case segformat.IsDuckDB(s.Path):
			alias := fmt.Sprintf("msrc_%d", i)
			q := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", layout.ToSlash(s.Path), alias)
			if _, err := x.db.ExecContext(context.Background(), q); err != nil {
				cleanup()
				return nil, func() {}, fmt.Errorf("merge: attach source %s: %w", s.Path, err)
			}
			aliases = append(aliases, alias)
			parts[i] = fmt.Sprintf("SELECT * FROM %s.%s", alias, duckTable(s.Path))
		default:
			parts[i] = fmt.Sprintf("SELECT * FROM read_parquet('%s')", layout.ToSlash(s.Path))
		}
	}
	return parts, cleanup, nil
}

// StatSegment reads segment metadata for min/max ts and byte size.
func StatSegment(path string, tier int, caps DuckDBCaps) (Segment, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Segment{}, err
	}
	connector, err := newInMemoryConnector(caps)
	if err != nil {
		return Segment{}, err
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	var minTs, maxTs time.Time
	ctx := context.Background()
	if segformat.IsDuckDB(path) {
		alias := "stat"
		attach := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", layout.ToSlash(path), alias)
		if _, err := db.ExecContext(ctx, attach); err != nil {
			return Segment{}, err
		}
		err = db.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT MIN(ts), MAX(ts) FROM %s.%s`, alias, segformat.MetricsTable,
		)).Scan(&minTs, &maxTs)
		_, _ = db.ExecContext(ctx, "DETACH "+alias)
	} else {
		err = db.QueryRowContext(ctx, fmt.Sprintf(`
			SELECT MIN(ts), MAX(ts) FROM read_parquet('%s')
		`, layout.ToSlash(path))).Scan(&minTs, &maxTs)
	}
	if err != nil {
		return Segment{}, err
	}
	return Segment{
		Tier:  tier,
		Path:  path,
		Bytes: info.Size(),
		MinTs: minTs.UTC(),
		MaxTs: maxTs.UTC(),
	}, nil
}

func isSegmentFile(name string) bool {
	if name == "" || name[0] == '.' {
		return false
	}
	ext := filepath.Ext(name)
	return ext == ".parquet" || ext == ".duckdb"
}

// ScanTier lists segments in a tier directory with stats.
func ScanTier(dataDir, tenant string, tier int, caps DuckDBCaps) ([]Segment, error) {
	dir := layout.TierDir(dataDir, tenant, tier)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	retired := layout.CompactedSet(entries)
	var out []Segment
	for _, e := range entries {
		if e.IsDir() || !isSegmentFile(e.Name()) {
			continue
		}
		if _, held := retired[e.Name()]; held {
			continue
		}
		path := filepath.Join(dir, e.Name())
		seg, err := StatSegment(path, tier, caps)
		if err != nil {
			return nil, err
		}
		out = append(out, seg)
	}
	return out, nil
}

// ScanAllTiers returns segments from L0..Lmax present on disk.
func ScanAllTiers(dataDir, tenant string, maxTier int, caps DuckDBCaps) ([]Segment, error) {
	var all []Segment
	for tier := 0; tier <= maxTier; tier++ {
		segs, err := ScanTier(dataDir, tenant, tier, caps)
		if err != nil {
			return nil, err
		}
		all = append(all, segs...)
	}
	return all, nil
}
