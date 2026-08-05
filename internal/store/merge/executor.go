package merge

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// ExecutorConfig holds merge execution parameters.
type ExecutorConfig struct {
	DataDir      string
	Tenant       string
	RowGroupSize int
	Threads      int
	MemoryLimit  string
}

// Executor runs planned merges via DuckDB COPY.
type Executor struct {
	cfg       ExecutorConfig
	db        *sql.DB
	connector *duckdb.Connector
}

// NewExecutor opens a temporary in-process DuckDB for merge COPY operations.
func NewExecutor(cfg ExecutorConfig) (*Executor, error) {
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 1_000_000
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
	final := filepath.Join(destDir, layout.SegmentName(now))
	tmp := final + ".tmp"

	fromParts := make([]string, len(action.Sources))
	for i, s := range action.Sources {
		fromParts[i] = fmt.Sprintf("SELECT * FROM read_parquet('%s')", layout.ToSlash(s.Path))
	}
	union := fromParts[0]
	for _, p := range fromParts[1:] {
		union += " UNION ALL " + p
	}
	// DuckDB read_parquet paths must be literal strings; server-owned paths only.
	//nolint:gosec // G201: parquet paths are server-owned literals; DuckDB cannot bind file paths.
	copySQL := fmt.Sprintf(`
		COPY (SELECT * FROM (%s) ORDER BY ts) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE %d)
	`, union, layout.ToSlash(tmp), x.cfg.RowGroupSize)
	if _, err := x.db.ExecContext(context.Background(), copySQL); err != nil {
		_ = os.Remove(tmp)
		return Segment{}, fmt.Errorf("merge copy: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return Segment{}, err
	}

	seg, err := StatSegment(final, destTier, DuckDBCaps{Threads: x.cfg.Threads, MemoryLimit: x.cfg.MemoryLimit})
	if err != nil {
		_ = os.Remove(final)
		return Segment{}, err
	}
	for _, s := range action.Sources {
		if err := os.Remove(s.Path); err != nil && !os.IsNotExist(err) {
			return Segment{}, fmt.Errorf("merge: delete %s: %w", s.Path, err)
		}
	}
	return seg, nil
}

// StatSegment reads parquet metadata for min/max ts and byte size.
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
	err = db.QueryRowContext(context.Background(), fmt.Sprintf(`
		SELECT MIN(ts), MAX(ts) FROM read_parquet('%s')
	`, layout.ToSlash(path))).Scan(&minTs, &maxTs)
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
	var out []Segment
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".parquet" {
			continue
		}
		if e.Name()[0] == '.' {
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
