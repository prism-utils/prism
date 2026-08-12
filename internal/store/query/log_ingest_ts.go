package query

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/prism-utils/prism/internal/store/segformat"
)

// logSegmentHasIngestTS reports whether a parquet log segment already carries
// per-row __prism_ts_ns (written at merge). Legacy landing files lack it.
// .duckdb segments cannot be probed from a path alone — HasIngestTS stays
// false here until DESCRIBE runs on an ATTACHed connection.
func logSegmentHasIngestTS(path string) bool {
	if segformat.IsDuckDB(path) {
		return false
	}
	return parquetHasColumn(path, lokiTSColumn)
}

func parquetHasColumn(path, name string) bool {
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return false
	}
	defer func() { _ = rdr.Close() }()
	sc := rdr.MetaData().Schema
	for i := 0; i < sc.NumColumns(); i++ {
		if sc.Column(i).Name() == name {
			return true
		}
	}
	return false
}

// annotateDuckLogIngestTS sets HasIngestTS on attached .duckdb log segments by
// DESCRIBE on the sandbox connection (already ATTACHed).
func annotateDuckLogIngestTS(ctx context.Context, conn *sql.Conn, files []logFileMeta) error {
	for i := range files {
		f := &files[i]
		if !segformat.IsDuckDB(f.Path) || f.duckAlias == "" {
			continue
		}
		rel := segformat.LogsRelationForPath(f.Path)
		//nolint:gosec // G201: alias assigned by attachLogsDuckDB; column is a package constant.
		q := fmt.Sprintf(
			"SELECT COUNT(*) > 0 FROM (DESCRIBE SELECT * FROM %s.%s) WHERE column_name = '%s'",
			f.duckAlias, rel, lokiTSColumn,
		)
		var ok bool
		if err := conn.QueryRowContext(ctx, q).Scan(&ok); err != nil {
			return fmt.Errorf("describe log ingest ts %s: %w", f.Path, err)
		}
		f.HasIngestTS = ok
	}
	return nil
}
