package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/elk-utilities/prism/internal/store/layout"
	"github.com/elk-utilities/prism/internal/store/logmeta"
	duckdb "github.com/marcboeker/go-duckdb/v2"
)

type logCoalesceKey struct {
	tenant   string
	artifact string
}

type logCoalesceBuf struct {
	dir        string
	paths      []string
	totalBytes int64
	startedAt  time.Time
}

func (e *Engine) coalesceEnabled() bool {
	return e.cfg.LogCoalesceMaxAge > 0 || e.cfg.LogCoalesceMaxBytes > 0
}

func (e *Engine) landLogCoalesced(tenant, artifact string, body []byte) (int64, error) {
	key := logCoalesceKey{tenant: tenant, artifact: artifact}
	e.coalesceMu.Lock()
	buf := e.coalesce[key]
	if buf == nil {
		pending := filepath.Join(e.cfg.DataDir, tenant, "logs", artifact, ".pending")
		if err := os.MkdirAll(pending, 0o750); err != nil {
			e.coalesceMu.Unlock()
			return 0, err
		}
		buf = &logCoalesceBuf{dir: pending, startedAt: e.clock()}
		e.coalesce[key] = buf
	}
	e.coalesceMu.Unlock()

	chunk := filepath.Join(buf.dir, layout.SegmentName(e.clock()))
	//nolint:gosec // G703: pending dir is under tenant/logs/<artifact>/.pending; name is SegmentName
	if err := os.WriteFile(chunk, body, 0o600); err != nil {
		return 0, err
	}
	n := int64(len(body))

	e.coalesceMu.Lock()
	buf.paths = append(buf.paths, chunk)
	buf.totalBytes += n
	shouldSeal := e.shouldSealCoalesce(buf)
	e.coalesceMu.Unlock()

	if shouldSeal {
		if err := e.sealLogCoalesce(tenant, artifact); err != nil {
			return n, err
		}
	}
	return n, nil
}

func (e *Engine) shouldSealCoalesce(buf *logCoalesceBuf) bool {
	if e.cfg.LogCoalesceMaxBytes > 0 && buf.totalBytes >= e.cfg.LogCoalesceMaxBytes {
		return true
	}
	if e.cfg.LogCoalesceMaxAge > 0 && !e.clock().Before(buf.startedAt.Add(e.cfg.LogCoalesceMaxAge)) {
		return true
	}
	return false
}

// FlushLogCoalesce seals all coalesce buffers whose max age has elapsed.
func (e *Engine) FlushLogCoalesce() error {
	if !e.coalesceEnabled() {
		return nil
	}
	e.coalesceMu.Lock()
	var keys []logCoalesceKey
	now := e.clock()
	for key, buf := range e.coalesce {
		if len(buf.paths) == 0 {
			continue
		}
		if e.cfg.LogCoalesceMaxAge > 0 && !now.Before(buf.startedAt.Add(e.cfg.LogCoalesceMaxAge)) {
			keys = append(keys, key)
		}
	}
	e.coalesceMu.Unlock()
	for _, key := range keys {
		if err := e.sealLogCoalesce(key.tenant, key.artifact); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) sealLogCoalesce(tenant, artifact string) error {
	key := logCoalesceKey{tenant: tenant, artifact: artifact}
	e.coalesceMu.Lock()
	buf := e.coalesce[key]
	if buf == nil || len(buf.paths) == 0 {
		e.coalesceMu.Unlock()
		return nil
	}
	paths := append([]string(nil), buf.paths...)
	pendingDir := buf.dir
	delete(e.coalesce, key)
	e.coalesceMu.Unlock()

	landing := layout.LogsLandingDir(e.cfg.DataDir, tenant, artifact)
	if err := os.MkdirAll(landing, 0o750); err != nil {
		return err
	}
	final := filepath.Join(landing, segmentName(e.clock()))
	tmp := final + ".tmp"
	if err := mergeParquetUnion(context.Background(), paths, tmp, e.cfg.RowGroupSize); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	for _, p := range paths {
		_ = os.Remove(p)
	}
	_ = os.Remove(pendingDir)

	if err := e.finishLogLand(tenant, artifact, final); err != nil {
		return err
	}
	return nil
}

func (e *Engine) finishLogLand(tenant, artifact, finalPath string) error {
	if err := logmeta.Bump(e.cfg.DataDir, tenant); err != nil {
		return err
	}
	if err := logmeta.SyncManifest(e.cfg.DataDir, tenant, artifact); err != nil {
		return err
	}
	if err := logmeta.MergeLabelIndexFromParquet(e.cfg.DataDir, tenant, finalPath); err != nil {
		return err
	}
	return nil
}

func mergeParquetUnion(ctx context.Context, sources []string, dest string, rowGroupSize int) error {
	if len(sources) == 0 {
		return fmt.Errorf("engine: coalesce seal: no sources")
	}
	if rowGroupSize <= 0 {
		rowGroupSize = 1_000_000
	}
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return err
	}
	defer func() { _ = connector.Close() }()
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()

	quoted := make([]string, len(sources))
	for i, p := range sources {
		quoted[i] = "'" + strings.ReplaceAll(layout.ToSlash(p), "'", "''") + "'"
	}
	//nolint:gosec // G201: paths are server-owned literals.
	q := fmt.Sprintf(
		`COPY (SELECT * FROM read_parquet([%s], union_by_name=true)) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE %d)`,
		strings.Join(quoted, ", "), layout.ToSlash(dest), rowGroupSize,
	)
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("engine: coalesce copy: %w", err)
	}
	return nil
}
