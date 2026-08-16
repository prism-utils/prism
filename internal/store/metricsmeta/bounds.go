package metricsmeta

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
)

// FileBounds returns [min,max] ingest timestamps for a metrics segment.
// Parquet/DuckDB stats are preferred. Files that cannot be read are reported
// as not-ok so callers skip them instead of treating them as "everything".
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

func statMinMax(ctx context.Context, path string) (minTs, maxTs time.Time, err error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
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

func nullTimes(minN, maxN sql.NullTime) (time.Time, time.Time, error) {
	if !minN.Valid || !maxN.Valid {
		return time.Time{}, time.Time{}, nil
	}
	return minN.Time, maxN.Time, nil
}
