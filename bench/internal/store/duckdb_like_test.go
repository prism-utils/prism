//go:build cgo

package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	duckdb "github.com/marcboeker/go-duckdb/v2"
	"github.com/prism-utils/prism/bench/internal/gen"
	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/stretchr/testify/require"
)

func TestDuckDB_readParquetLike_fullScale(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	cfg := gen.ScaleConfig(1)
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "segment.parquet")
	require.NoError(t, gen.WriteLogsTier(path, ds.Logs))

	connector, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	var total int64
	require.NoError(t, db.QueryRowContext(context.Background(), "SELECT count(*) FROM read_parquet('"+layout.ToSlash(path)+"')").Scan(&total))
	require.Equal(t, cfg.LogsRows, total)

	var likeAll int64
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM read_parquet('`+layout.ToSlash(path)+`') WHERE message LIKE '%deadline exceeded%'`).Scan(&likeAll))
	require.Equal(t, gen.ExpectedDeadlineCount(cfg.LogsRows), likeAll)

	logStart, logEnd := ds.LogsQueryRange()
	startTS := logStart.UTC().Format("2006-01-02T15:04:05.999999Z")
	endTS := logEnd.UTC().Format("2006-01-02T15:04:05.999999Z")
	var inRange int64
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM read_parquet('`+layout.ToSlash(path)+`')
		WHERE ts >= TIMESTAMPTZ '`+startTS+`'
		  AND ts < TIMESTAMPTZ '`+endTS+`'`).Scan(&inRange))
	require.Equal(t, cfg.LogsRows, inRange)

	var like int64
	q := `SELECT count(*) FROM read_parquet('` + layout.ToSlash(path) + `')
		WHERE ts >= TIMESTAMPTZ '` + startTS + `'
		  AND ts < TIMESTAMPTZ '` + endTS + `'
		  AND message LIKE '%deadline exceeded%'`
	require.NoError(t, db.QueryRowContext(context.Background(), q).Scan(&like))
	require.Equal(t, gen.ExpectedDeadlineCount(cfg.LogsRows), like)
}
