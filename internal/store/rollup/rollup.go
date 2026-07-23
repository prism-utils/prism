package rollup

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	duckdb "github.com/marcboeker/go-duckdb"
)

// Step is a downsampling interval (e.g. 1m, 5m, 1h).
type Step struct {
	Name     string
	Interval string
}

// ParseSteps parses ROLLUP_STEPS env form "1m,5m,1h".
func ParseSteps(raw string) []Step {
	if raw == "" {
		raw = "1m,5m,1h"
	}
	var out []Step
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, Step{Name: p, Interval: toInterval(p)})
	}
	return out
}

func toInterval(s string) string {
	switch s {
	case "1m":
		return "1 minute"
	case "5m":
		return "5 minutes"
	case "1h":
		return "1 hour"
	default:
		if strings.HasSuffix(s, "m") {
			return strings.TrimSuffix(s, "m") + " minutes"
		}
		if strings.HasSuffix(s, "h") {
			return strings.TrimSuffix(s, "h") + " hours"
		}
		return s
	}
}

// Builder materializes rollup parquet files from merged tier inputs.
type Builder struct {
	dataDir   string
	tenant    string
	steps     []Step
	db        *sql.DB
	connector *duckdb.Connector
}

// NewBuilder creates a rollup builder.
func NewBuilder(dataDir, tenant string, steps []Step) (*Builder, error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		steps = ParseSteps("")
	}
	return &Builder{
		dataDir:   dataDir,
		tenant:    tenant,
		steps:     steps,
		db:        sql.OpenDB(connector),
		connector: connector,
	}, nil
}

// Close releases the DuckDB connection.
func (b *Builder) Close() error {
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
	}
	return err
}

// BuildFromMerge writes rollup segments for each step from the merged source paths.
func (b *Builder) BuildFromMerge(sourcePaths []string, now time.Time) error {
	if len(sourcePaths) == 0 {
		return nil
	}
	fromParts := make([]string, len(sourcePaths))
	for i, p := range sourcePaths {
		fromParts[i] = fmt.Sprintf("SELECT * FROM read_parquet('%s')", layout.ToSlash(p))
	}
	union := strings.Join(fromParts, " UNION ALL ")

	for _, step := range b.steps {
		dir := layout.RollupDir(b.dataDir, b.tenant, step.Name)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
		final := filepath.Join(dir, layout.SegmentName(now))
		tmp := final + ".tmp"
		//nolint:gosec // G201: parquet paths are server-owned literals; DuckDB cannot bind file paths.
		q := fmt.Sprintf(`
			COPY (
				SELECT
					time_bucket(INTERVAL '%s', ts) AS bucket,
					"__name__",
					AVG(value)::DOUBLE AS avg,
					MIN(value)::DOUBLE AS min,
					MAX(value)::DOUBLE AS max,
					COUNT(*)::BIGINT AS count,
					SUM(value)::DOUBLE AS sum
				FROM (%s)
				GROUP BY 1, 2
				ORDER BY bucket, "__name__"
			) TO '%s' (FORMAT parquet)
		`, step.Interval, union, layout.ToSlash(tmp))
		if _, err := b.db.ExecContext(context.Background(), q); err != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("rollup %s: %w", step.Name, err)
		}
		if err := os.Rename(tmp, final); err != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	return nil
}

// StatRollupMaxBucket returns the latest bucket timestamp in a rollup parquet file.
func StatRollupMaxBucket(path string) (time.Time, error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	var maxBucket time.Time
	err = db.QueryRowContext(context.Background(), fmt.Sprintf(`
		SELECT MAX(bucket) FROM read_parquet('%s')
	`, layout.ToSlash(path))).Scan(&maxBucket)
	if err != nil {
		return time.Time{}, err
	}
	return maxBucket.UTC(), nil
}

// AggregateRaw computes reference aggregates over raw rows for tests.
func AggregateRaw(db *sql.DB, paths []string, interval string) (map[string]AggRow, error) {
	fromParts := make([]string, len(paths))
	for i, p := range paths {
		fromParts[i] = fmt.Sprintf("SELECT * FROM read_parquet('%s')", layout.ToSlash(p))
	}
	union := strings.Join(fromParts, " UNION ALL ")
	//nolint:gosec // G201: parquet paths are server-owned literals; DuckDB cannot bind file paths.
	q := fmt.Sprintf(`
		SELECT
			time_bucket(INTERVAL '%s', ts) AS bucket,
			"__name__",
			AVG(value)::DOUBLE AS avg,
			MIN(value)::DOUBLE AS min,
			MAX(value)::DOUBLE AS max,
			COUNT(*)::BIGINT AS count,
			SUM(value)::DOUBLE AS sum
		FROM (%s)
		GROUP BY 1, 2
	`, interval, union)
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]AggRow{}
	for rows.Next() {
		var r AggRow
		if err := rows.Scan(&r.Bucket, &r.Name, &r.Avg, &r.Min, &r.Max, &r.Count, &r.Sum); err != nil {
			return nil, err
		}
		out[r.Key()] = r
	}
	return out, rows.Err()
}

// AggRow is one rollup aggregate bucket for tests.
type AggRow struct {
	Bucket time.Time
	Name   string
	Avg    float64
	Min    float64
	Max    float64
	Count  int64
	Sum    float64
}

// Key returns a stable map key for an aggregate row.
func (r *AggRow) Key() string {
	return r.Bucket.UTC().Format(time.RFC3339Nano) + "|" + r.Name
}

// ReadRollup reads a rollup parquet into an AggRow map.
func ReadRollup(db *sql.DB, path string) (map[string]AggRow, error) {
	rows, err := db.QueryContext(context.Background(), fmt.Sprintf(`
		SELECT bucket, "__name__", avg, min, max, count, sum FROM read_parquet('%s')
	`, layout.ToSlash(path)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]AggRow{}
	for rows.Next() {
		var r AggRow
		if err := rows.Scan(&r.Bucket, &r.Name, &r.Avg, &r.Min, &r.Max, &r.Count, &r.Sum); err != nil {
			return nil, err
		}
		out[r.Key()] = r
	}
	return out, rows.Err()
}
