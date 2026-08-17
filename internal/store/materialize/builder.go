package materialize

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/metrics"
)

// RunConfig is one merge's materialization pass.
type RunConfig struct {
	DataDir     string
	Tenant      string
	DestPath    string
	SourcePaths []string
	DestTier    int
	Plane       Plane
	Items       []Item
	RunJobs     bool
	Now         time.Time
	Threads     int
	MemoryLimit string
	DeleteGrace time.Duration
	Logger      *slog.Logger
}

// Run writes configured materializations for one merge dest. Item SQL errors
// are logged and skipped; the merge still succeeds (this function returns nil
// unless DuckDB itself cannot start).
func Run(ctx context.Context, cfg *RunConfig) error {
	if cfg == nil || !cfg.RunJobs || len(cfg.Items) == 0 {
		return nil
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.Now.IsZero() {
		cfg.Now = time.Now().UTC()
	}
	if cfg.Plane == "" {
		cfg.Plane = PlaneMetrics
	}

	b, err := newBuilder(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = b.Close() }()

	if err := b.bindMergeViews(ctx, cfg); err != nil {
		return err
	}

	for _, item := range cfg.Items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.plane() != cfg.Plane {
			continue
		}
		if cfg.DestTier < item.MinTier {
			continue
		}
		if err := compactMatching(cfg, item.Name); err != nil {
			log.Error("materialize compact", "name", item.Name, "err", err)
			continue
		}
		if err := b.writeItem(ctx, cfg, item); err != nil {
			log.Error("materialize sql", "name", item.Name, "tenant", cfg.Tenant, "err", err)
			continue
		}
	}
	return nil
}

type builder struct {
	db        *sql.DB
	connector *duckdb.Connector
}

func newBuilder(ctx context.Context, cfg *RunConfig) (*builder, error) {
	connector, err := duckdb.NewConnector("", initFn(ctx, cfg))
	if err != nil {
		return nil, err
	}
	metrics.DuckDBOpen(metrics.RoleMaterialize)
	return &builder{db: sql.OpenDB(connector), connector: connector}, nil
}

func (b *builder) Close() error {
	var err error
	if b.db != nil {
		err = b.db.Close()
		b.db = nil
	}
	if b.connector != nil {
		if cerr := b.connector.Close(); err == nil {
			err = cerr
		}
		b.connector = nil
		metrics.DuckDBClose(metrics.RoleMaterialize)
	}
	return err
}

func (b *builder) bindMergeViews(ctx context.Context, cfg *RunConfig) error {
	if cfg.DestPath == "" {
		return fmt.Errorf("materialize: empty dest path")
	}
	//nolint:gosec // G201: dest path is a server-owned merge output.
	outSQL := fmt.Sprintf("CREATE VIEW merge_output AS SELECT * FROM read_parquet(%s)", quotePath(cfg.DestPath))
	if _, err := b.db.ExecContext(ctx, outSQL); err != nil {
		return fmt.Errorf("materialize: merge_output: %w", err)
	}
	inSQL := inputViewSQL(cfg.SourcePaths)
	if _, err := b.db.ExecContext(ctx, "CREATE VIEW merge_input AS "+inSQL); err != nil {
		return fmt.Errorf("materialize: merge_input: %w", err)
	}
	return nil
}

func inputViewSQL(paths []string) string {
	if len(paths) == 0 {
		return "SELECT * FROM merge_output WHERE 1=0"
	}
	parts := make([]string, 0, len(paths))
	for _, p := range paths {
		if strings.HasSuffix(p, ".duckdb") {
			continue
		}
		parts = append(parts, fmt.Sprintf("SELECT * FROM read_parquet(%s)", quotePath(p)))
	}
	if len(parts) == 0 {
		return "SELECT * FROM merge_output WHERE 1=0"
	}
	return strings.Join(parts, " UNION ALL ")
}

func (b *builder) writeItem(ctx context.Context, cfg *RunConfig, item Item) error {
	dir := layout.MaterializationDir(cfg.DataDir, cfg.Tenant, item.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	final := filepath.Join(dir, filepath.Base(cfg.DestPath))
	if !strings.HasSuffix(final, ".parquet") {
		stem := strings.TrimSuffix(filepath.Base(cfg.DestPath), filepath.Ext(cfg.DestPath))
		final = filepath.Join(dir, stem+".parquet")
	}
	tmp := final + ".tmp"
	//nolint:gosec // G201: operator SQL already validated as SELECT; paths are server-owned.
	q := fmt.Sprintf("COPY (%s) TO %s (FORMAT parquet)", strings.TrimSuffix(strings.TrimSpace(item.SQL), ";"), quotePath(tmp))
	if _, err := b.db.ExecContext(ctx, q); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func compactMatching(cfg *RunConfig, name string) error {
	dir := layout.MaterializationDir(cfg.DataDir, cfg.Tenant, name)
	for _, src := range cfg.SourcePaths {
		base := filepath.Base(src)
		if !strings.HasSuffix(base, ".parquet") {
			base = strings.TrimSuffix(base, filepath.Ext(base)) + ".parquet"
		}
		path := filepath.Join(dir, base)
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		deadline := cfg.Now
		if cfg.DeleteGrace > 0 {
			deadline = cfg.Now.Add(cfg.DeleteGrace)
		}
		if err := writeCompactedMarker(path, deadline); err != nil {
			return err
		}
		if cfg.DeleteGrace <= 0 {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func writeCompactedMarker(segmentPath string, deleteAfter time.Time) error {
	tmp := layout.CompactedMarkerTemp(segmentPath)
	body := strconv.FormatInt(deleteAfter.UTC().Unix(), 10) + "\n"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		return fmt.Errorf("materialize: mark compacted: %w", err)
	}
	if err := os.Rename(tmp, layout.CompactedMarker(segmentPath)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("materialize: mark compacted: %w", err)
	}
	return nil
}

func initFn(ctx context.Context, cfg *RunConfig) func(driver.ExecerContext) error {
	if cfg.Threads <= 0 && cfg.MemoryLimit == "" {
		return nil
	}
	threads := cfg.Threads
	memLimit := cfg.MemoryLimit
	return func(exec driver.ExecerContext) error {
		if threads > 0 {
			q := fmt.Sprintf("SET threads=%d", threads)
			if _, err := exec.ExecContext(ctx, q, nil); err != nil {
				return fmt.Errorf("materialize: set threads: %w", err)
			}
		}
		if memLimit != "" {
			q := fmt.Sprintf("SET memory_limit='%s'", strings.ReplaceAll(memLimit, "'", "''"))
			if _, err := exec.ExecContext(ctx, q, nil); err != nil {
				return fmt.Errorf("materialize: set memory_limit: %w", err)
			}
		}
		return nil
	}
}
