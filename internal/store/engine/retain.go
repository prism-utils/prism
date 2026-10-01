package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

// RetainHot drops hot table rows whose ts is strictly before cutoff and
// checkpoints the catalog so the file can shrink. The catalog file itself is
// never unlinked. A published snapshot is removed when nothing remains, or
// rewritten when some rows stay.
func (e *Engine) RetainHot(tenant string, cutoff time.Time) error {
	if !storetenant.TenantAllowed(tenant) {
		return fmt.Errorf("engine: invalid tenant %q", tenant)
	}
	if _, err := os.Stat(filepath.Join(e.cfg.DataDir, tenant, "engine.duckdb")); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	te, err := e.open(tenant)
	if err != nil {
		return err
	}

	te.mu.Lock()
	deleted, remaining, err := retainHotLocked(te, cutoff)
	te.mu.Unlock()
	if err != nil {
		return err
	}
	if remaining == 0 {
		return unlinkPublishedHotSnapshot(e.cfg.DataDir, tenant)
	}
	if deleted > 0 {
		return e.ExportHotSnapshot(tenant)
	}
	return nil
}

func retainHotLocked(te *tenantEntry, cutoff time.Time) (deleted, remaining int64, err error) {
	ctx := context.Background()
	d1, err := deleteExpiredHot(ctx, te.db, hotCurrentTable, cutoff)
	if err != nil {
		return 0, 0, err
	}
	d2, err := deleteExpiredHot(ctx, te.db, hotPrevTable, cutoff)
	if err != nil {
		return 0, 0, err
	}
	deleted = d1 + d2
	remaining, err = countHotRows(ctx, te.db)
	if err != nil {
		return deleted, 0, err
	}
	if deleted == 0 {
		return 0, remaining, nil
	}
	if remaining == 0 {
		//nolint:gosec // G201: table names are package consts.
		if _, err := te.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s; DROP TABLE IF EXISTS %s", hotCurrentTable, hotPrevTable)); err != nil {
			return deleted, remaining, fmt.Errorf("engine: drop expired hot: %w", err)
		}
		if err := te.ensureHotCurrent(); err != nil {
			return deleted, remaining, err
		}
	}
	if _, err := te.db.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return deleted, remaining, fmt.Errorf("engine: checkpoint: %w", err)
	}
	return deleted, remaining, nil
}

func deleteExpiredHot(ctx context.Context, db *sql.DB, table string, cutoff time.Time) (int64, error) {
	//nolint:gosec // G201: table name is a package const.
	res, err := db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE ts < ?", table), cutoff.UTC())
	if err != nil {
		if tableMissing(err.Error()) {
			return 0, nil
		}
		return 0, fmt.Errorf("engine: delete expired %s: %w", table, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func countHotRows(ctx context.Context, db *sql.DB) (int64, error) {
	var total int64
	for _, table := range []string{hotCurrentTable, hotPrevTable} {
		var n int64
		//nolint:gosec // G201: table name is a package const.
		err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n)
		if err != nil {
			if tableMissing(errString(err)) {
				continue
			}
			return 0, err
		}
		total += n
	}
	return total, nil
}

func unlinkPublishedHotSnapshot(dataDir, tenant string) error {
	hotDir := filepath.Join(dataDir, tenant, hotDirName)
	for _, name := range []string{hotSnapshotParquet, hotSnapshotDuckDB, hotSnapshotDuckDB + ".wal"} {
		path := filepath.Join(hotDir, name)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
