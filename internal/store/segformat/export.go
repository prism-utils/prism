package segformat

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/elk-utilities/prism/internal/store/layout"
)

// AtomicExportDuckDB materializes selectSQL into a checkpointed single-file
// .duckdb at finalPath (temp + rename). After a successful checkpoint there is
// no required sibling .wal for a consistent open. table is the relation name
// inside the exported database (metrics / logs).
func AtomicExportDuckDB(db *sql.DB, selectSQL, finalPath, storageVersion, table string, args ...any) error {
	if storageVersion == "" {
		storageVersion = DefaultStorageVersion
	}
	if table == "" {
		return fmt.Errorf("segformat: export duckdb: empty table")
	}
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	// The temp file and the ATTACH alias both name a resource that DuckDB allows
	// to exist once per connection, so a per-call random token keeps two exports
	// racing to the same finalPath from colliding on either of them.
	var token [4]byte
	if _, err := rand.Read(token[:]); err != nil {
		return fmt.Errorf("segformat: export token: %w", err)
	}
	id := hex.EncodeToString(token[:])
	tmp := fmt.Sprintf("%s.%s.tmp", finalPath, id)
	// The temp database is scratch in every outcome: on failure it is a partial
	// file, and on success the rename has already published it.
	defer func() {
		_ = os.Remove(tmp)
		_ = os.Remove(tmp + ".wal")
	}()

	alias := "exp_" + id
	ctx := context.Background()
	attach := fmt.Sprintf(
		"ATTACH '%s' AS %s (STORAGE_VERSION '%s')",
		layout.ToSlash(tmp), alias, strings.ReplaceAll(storageVersion, "'", "''"),
	)
	if _, err := db.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("segformat: attach export: %w", err)
	}
	create := fmt.Sprintf("CREATE OR REPLACE TABLE %s.%s AS %s", alias, table, selectSQL)
	if _, err := db.ExecContext(ctx, create, args...); err != nil {
		_, _ = db.ExecContext(ctx, "DETACH "+alias)
		return fmt.Errorf("segformat: create export table: %w", err)
	}
	if _, err := db.ExecContext(ctx, "CHECKPOINT "+alias); err != nil {
		_, _ = db.ExecContext(ctx, "DETACH "+alias)
		return fmt.Errorf("segformat: checkpoint export: %w", err)
	}
	if _, err := db.ExecContext(ctx, "DETACH "+alias); err != nil {
		return fmt.Errorf("segformat: detach export: %w", err)
	}
	_ = os.Remove(tmp + ".wal")
	if err := os.Rename(tmp, finalPath); err != nil {
		return fmt.Errorf("segformat: rename export: %w", err)
	}
	_ = os.Remove(finalPath + ".wal")
	return nil
}

// IsDuckDB reports whether path has a .duckdb extension.
func IsDuckDB(path string) bool {
	return filepath.Ext(path) == ".duckdb"
}

// IsParquet reports whether path has a .parquet extension.
func IsParquet(path string) bool {
	return filepath.Ext(path) == ".parquet"
}
