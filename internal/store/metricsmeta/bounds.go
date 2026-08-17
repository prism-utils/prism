package metricsmeta

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/metrics"
	"github.com/prism-utils/prism/internal/store/segformat"
)

const (
	boundsDuckDBMemoryLimit = "128MB"
	boundsDuckDBThreads     = 1
)

// FileBounds returns [min,max] ingest timestamps for a metrics segment.
// Parquet footer column stats are preferred. Files that cannot be read are
// reported as not-ok so callers skip them instead of treating them as
// "everything".
func FileBounds(ctx context.Context, path string) (minNs, maxNs int64, ok bool) {
	minTs, maxTs, err := statMinMax(ctx, path)
	if err != nil {
		slog.Warn("metrics catalog skipped file without bounds", "path", path, "err", err)
		return 0, 0, false
	}
	if minTs.IsZero() && maxTs.IsZero() {
		return 0, 0, true
	}
	return minTs.UTC().UnixNano(), maxTs.UTC().UnixNano(), true
}

func openBoundsDB(ctx context.Context) (*sql.DB, func(), error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, nil, err
	}
	metrics.DuckDBOpen(metrics.RoleBounds)
	db := sql.OpenDB(connector)
	cleanup := func() {
		_ = db.Close()
		_ = connector.Close()
		metrics.DuckDBClose(metrics.RoleBounds)
	}
	if _, err := db.ExecContext(ctx, "SET memory_limit='"+boundsDuckDBMemoryLimit+"'"); err != nil { //nolint:gosec // G201: constant memory cap.
		cleanup()
		return nil, nil, err
	}
	if _, err := db.ExecContext(ctx, "SET threads="+strconv.Itoa(boundsDuckDBThreads)); err != nil {
		cleanup()
		return nil, nil, err
	}
	return db, cleanup, nil
}

func statMinMax(ctx context.Context, path string) (minTs, maxTs time.Time, err error) {
	db, cleanup, err := openBoundsDB(ctx)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	defer cleanup()

	if !segformat.IsDuckDB(path) {
		if minTs, maxTs, ok, ferr := parquetFooterBoundsWithDB(ctx, db, path); ferr != nil {
			return time.Time{}, time.Time{}, ferr
		} else if ok {
			return minTs, maxTs, nil
		}
	}

	var minN, maxN sql.NullTime
	if segformat.IsDuckDB(path) {
		alias := "mstat"
		//nolint:gosec // G201: path is a server-owned tenant segment.
		attach := "ATTACH '" + layout.ToSlash(path) + "' AS " + alias + " (READ_ONLY)"
		if _, err := db.ExecContext(ctx, attach); err != nil {
			return time.Time{}, time.Time{}, err
		}
		err = db.QueryRowContext(ctx, "SELECT MIN(ts), MAX(ts) FROM "+alias+"."+segformat.MetricsTable).Scan(&minN, &maxN)
		_, _ = db.ExecContext(ctx, "DETACH "+alias)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		return nullTimes(minN, maxN)
	}
	//nolint:gosec // G201: path is a server-owned tenant segment.
	q := "SELECT MIN(ts), MAX(ts) FROM read_parquet('" + layout.ToSlash(path) + "')"
	err = db.QueryRowContext(ctx, q).Scan(&minN, &maxN)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return nullTimes(minN, maxN)
}

func parquetFooterBounds(ctx context.Context, path string) (minTs, maxTs time.Time, ok bool, err error) {
	db, cleanup, err := openBoundsDB(ctx)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	defer cleanup()
	return parquetFooterBoundsWithDB(ctx, db, path)
}

func parquetFooterBoundsWithDB(ctx context.Context, db *sql.DB, path string) (minTs, maxTs time.Time, ok bool, err error) {
	//nolint:gosec // G201: path is a server-owned tenant segment.
	q := `SELECT MIN(TRY_CAST(stats_min_value AS TIMESTAMP)), MAX(TRY_CAST(stats_max_value AS TIMESTAMP))
FROM parquet_metadata('` + layout.ToSlash(path) + `')
WHERE path_in_schema = 'ts'`
	var minN, maxN sql.NullTime
	err = db.QueryRowContext(ctx, q).Scan(&minN, &maxN)
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	if !minN.Valid || !maxN.Valid {
		return time.Time{}, time.Time{}, false, nil
	}
	return minN.Time, maxN.Time, true, nil
}

func nullTimes(minN, maxN sql.NullTime) (time.Time, time.Time, error) {
	if !minN.Valid || !maxN.Valid {
		return time.Time{}, time.Time{}, nil
	}
	return minN.Time, maxN.Time, nil
}
