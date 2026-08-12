package segformat

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/prism-utils/prism/internal/store/layout"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// ConvertDuckDBToParquet rewrites one checkpointed .duckdb segment to .parquet
// by projecting the named table through COPY … FORMAT parquet.
func ConvertDuckDBToParquet(src, dst, table string) error {
	if table == "" {
		return fmt.Errorf("segformat: convert: empty table name")
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return err
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	alias := "cvt"
	srcSlash := layout.ToSlash(src)
	dstTmp := dst + ".tmp"
	_ = os.Remove(dstTmp)

	attach := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", srcSlash, alias)
	if _, err := db.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("segformat: attach %s: %w", src, err)
	}
	//nolint:gosec // G201: alias/table are package consts; dst path is server-owned.
	copySQL := fmt.Sprintf(
		`COPY (SELECT * FROM %s.%s) TO '%s' (FORMAT parquet)`,
		alias, table, layout.ToSlash(dstTmp),
	)
	if _, err := db.ExecContext(ctx, copySQL); err != nil {
		_ = os.Remove(dstTmp)
		_, _ = db.ExecContext(ctx, "DETACH "+alias)
		return fmt.Errorf("segformat: copy to parquet: %w", err)
	}
	if _, err := db.ExecContext(ctx, "DETACH "+alias); err != nil {
		_ = os.Remove(dstTmp)
		return err
	}
	if err := os.Rename(dstTmp, dst); err != nil {
		_ = os.Remove(dstTmp)
		return err
	}
	return nil
}

// ConvertTenantDuckDBToParquet walks hot + metrics tiers + logs tiers under a
// tenant and rewrites every .duckdb segment to a sibling .parquet, then removes
// the source .duckdb (and any leftover .wal). Used before upgrading DuckDB
// storage major versions.
func ConvertTenantDuckDBToParquet(dataDir, tenant string) (int, error) {
	root := filepath.Join(dataDir, tenant)
	var converted int
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasSuffix(name, ".wal") {
			return nil
		}
		if filepath.Ext(name) != ".duckdb" {
			return nil
		}
		table := tableForPath(path)
		dst := strings.TrimSuffix(path, ".duckdb") + ".parquet"
		if err := ConvertDuckDBToParquet(path, dst, table); err != nil {
			return fmt.Errorf("segformat: convert %s: %w", path, err)
		}
		//nolint:gosec // G122: path is under dataDir/tenant from ConvertTenantDuckDBToParquet WalkDir root.
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		_ = os.Remove(path + ".wal") //nolint:gosec // G122: same WalkDir-scoped path.
		converted++
		return nil
	})
	return converted, err
}

func tableForPath(path string) string {
	slash := filepath.ToSlash(path)
	if strings.Contains(slash, "/logs/") {
		return LogsTable
	}
	return MetricsTable
}

// MkdirAllForTest creates dir with store permissions.
func MkdirAllForTest(dir string) error {
	return os.MkdirAll(dir, 0o750)
}

// FileExistsForTest reports whether path exists.
func FileExistsForTest(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
