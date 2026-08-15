package seed

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

//go:embed metrics-raw-seed.parquet
var metricsRawSeed []byte

// SeedName is the reserved seed filename excluded from billing and legacy import.
const SeedName = "_seed.parquet"

// EnsureMetricsRawSeedForTenant writes the embedded zero-row seed into
// <dataDir>/<tenant>/metrics-raw/_seed.parquet when absent.
func EnsureMetricsRawSeedForTenant(dataDir, tenant string) error {
	return ensureSeedDir(filepath.Join(dataDir, tenant, "metrics-raw"))
}

// EnsureTieredLayoutForTenant writes zero-row parquet seeds so read_parquet globs
// match at least one file before the first flush or hot snapshot.
func EnsureTieredLayoutForTenant(dataDir, tenant string) error {
	root := filepath.Join(dataDir, tenant)
	if err := writeZeroRowHotParquetIfMissing(filepath.Join(root, "tiers", SeedName)); err != nil {
		return err
	}
	if err := writeZeroRowHotParquetIfMissing(filepath.Join(root, "hot", "current.parquet")); err != nil {
		return err
	}
	for _, step := range []string{"1m", "5m", "1h"} {
		dir := filepath.Join(root, "rollups", step)
		if err := writeZeroRowRollupParquetIfMissing(filepath.Join(dir, SeedName)); err != nil {
			return err
		}
	}
	return nil
}

// EnsureLogsLayoutForTenant writes zero-row parquet seeds so read_parquet globs
// under logs/{logs-raw,logs-template,logs-summary} match at least one file
// before the tenant ships any logs.
func EnsureLogsLayoutForTenant(dataDir, tenant string) error {
	type artifactSeed struct {
		name string
		sql  string
	}
	seeds := []artifactSeed{
		{
			name: "logs-raw",
			sql: `
		COPY (
			SELECT
				CAST(NULL AS VARCHAR) AS message,
				CAST(NULL AS VARCHAR) AS format,
				CAST(NULL AS VARCHAR) AS stream,
				CAST(NULL AS VARCHAR) AS logtag,
				CAST(NULL AS BIGINT) AS __prism_ts_ns
			WHERE false
		) TO '%s' (FORMAT parquet)
	`,
		},
		{
			name: "logs-template",
			sql: `
		COPY (
			SELECT
				CAST(NULL AS VARCHAR) AS message,
				CAST(NULL AS VARCHAR) AS format,
				CAST(NULL AS VARCHAR) AS template,
				CAST(NULL AS BIGINT) AS __prism_ts_ns
			WHERE false
		) TO '%s' (FORMAT parquet)
	`,
		},
		{
			name: "logs-summary",
			sql: `
		COPY (
			SELECT
				CAST(NULL AS VARCHAR) AS template,
				CAST(NULL AS BIGINT) AS count,
				CAST(NULL AS BIGINT) AS __prism_ts_ns
			WHERE false
		) TO '%s' (FORMAT parquet)
	`,
		},
	}
	for _, item := range seeds {
		dir := filepath.Join(dataDir, tenant, "logs", item.name)
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("seed: mkdir %s: %w", dir, err)
		}
		path := filepath.Join(dir, SeedName)
		if _, err := os.Stat(path); err == nil {
			continue
		}
		if err := writeEmptyParquet(path, item.sql); err != nil {
			return err
		}
	}
	return nil
}

func ensureSeedDir(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("seed: mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, SeedName)
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	return writeAtomicFile(path, metricsRawSeed)
}

func writeZeroRowHotParquetIfMissing(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("seed: mkdir %s: %w", filepath.Dir(path), err)
	}
	return writeEmptyParquet(path, `
		COPY (
			SELECT
				CAST(NULL AS VARCHAR) AS "__name__",
				CAST(NULL AS VARCHAR) AS labels,
				CAST(NULL AS DOUBLE) AS value,
				CAST(NULL AS BIGINT) AS timestamp_ms,
				CAST(NULL AS TIMESTAMP) AS ts
			WHERE false
		) TO '%s' (FORMAT parquet)
	`)
}

func writeZeroRowRollupParquetIfMissing(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("seed: mkdir %s: %w", filepath.Dir(path), err)
	}
	return writeEmptyParquet(path, `
		COPY (
			SELECT
				CAST(NULL AS TIMESTAMP) AS bucket,
				CAST(NULL AS VARCHAR) AS "__name__",
				CAST(NULL AS DOUBLE) AS avg,
				CAST(NULL AS DOUBLE) AS min,
				CAST(NULL AS DOUBLE) AS max,
				CAST(NULL AS BIGINT) AS count,
				CAST(NULL AS DOUBLE) AS sum
			WHERE false
		) TO '%s' (FORMAT parquet)
	`)
}

func writeEmptyParquet(path, copyTemplate string) error {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return fmt.Errorf("seed: duckdb: %w", err)
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	tmp := path + ".tmp"
	q := fmt.Sprintf(copyTemplate, filepath.ToSlash(tmp))
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("seed: copy %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func writeAtomicFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o640); err != nil { //nolint:gosec // G306: 0640 is the store's intentional group-readable seed permission.
		return fmt.Errorf("seed: write %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("seed: rename %s: %w", path, err)
	}
	return nil
}
