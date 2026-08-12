package gen_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/prism-utils/prism/bench/internal/gen"
	"github.com/stretchr/testify/require"
)

func TestWriteLogsTier_fullScaleRowCount(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	cfg := gen.ScaleConfig(1)
	ds, err := gen.Generate(cfg)
	require.NoError(t, err)

	path := t.TempDir() + "/logs.parquet"
	require.NoError(t, gen.WriteLogsTier(path, ds.Logs))

	r, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	require.Equal(t, cfg.LogsRows, r.MetaData().NumRows)
}
