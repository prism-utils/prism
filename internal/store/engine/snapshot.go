package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
)

const (
	hotDirName          = "hot"
	hotSnapshotName     = "current.parquet"
	legacyImportMarker  = ".legacy-import-done"
	legacyMetricsRawDir = "metrics-raw"
)

var legacyIngestNanosRe = regexp.MustCompile(`^metrics-raw-(\d+)-`)

// ExportHotSnapshots writes hot/current.parquet for every tenant with data on disk.
func (e *Engine) ExportHotSnapshots() error {
	tenants, err := listDataTenants(e.cfg.DataDir)
	if err != nil {
		return err
	}
	for _, tenant := range tenants {
		if err := e.exportHotSnapshot(tenant); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) exportHotSnapshot(tenant string) error {
	if !storetenant.TenantAllowed(tenant) {
		return fmt.Errorf("engine: invalid tenant %q", tenant)
	}
	te, err := e.open(tenant)
	if err != nil {
		return err
	}
	hotDir := filepath.Join(e.cfg.DataDir, tenant, hotDirName)
	if err := os.MkdirAll(hotDir, 0o750); err != nil {
		return err
	}
	final := filepath.Join(hotDir, hotSnapshotName)
	selectSQL := fmt.Sprintf("SELECT * FROM %s ORDER BY ts", hotCurrentTable)
	te.mu.RLock()
	defer te.mu.RUnlock()
	if err := atomicCopyTo(te.db, selectSQL, final, e.cfg.RowGroupSize); err != nil {
		return fmt.Errorf("engine: hot snapshot copy: %w", err)
	}
	return nil
}

func (e *Engine) importLegacyMetricsRaw(tenant string) error {
	if !storetenant.TenantAllowed(tenant) {
		return fmt.Errorf("engine: invalid tenant %q", tenant)
	}
	marker := filepath.Join(e.cfg.DataDir, tenant, legacyImportMarker)
	if _, err := os.Stat(marker); err == nil {
		return nil
	}

	legacyDir := filepath.Join(e.cfg.DataDir, tenant, legacyMetricsRawDir)
	entries, err := os.ReadDir(legacyDir)
	if err != nil {
		if os.IsNotExist(err) {
			return e.writeLegacyImportMarker(tenant)
		}
		return err
	}

	te, err := openTenant(e.cfg.DataDir, tenant, e.cfg)
	if err != nil {
		return err
	}
	defer func() { _ = te.db.Close() }()
	l0Dir := tierDir(e.cfg.DataDir, tenant, 0)
	if err := os.MkdirAll(l0Dir, 0o750); err != nil {
		return err
	}

	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".parquet" {
			continue
		}
		if ent.Name() == "_seed.parquet" || strings.HasPrefix(ent.Name(), ".") {
			continue
		}
		nanos, ok := legacyIngestNanosFromName(ent.Name())
		if !ok {
			continue
		}
		src := filepath.Join(legacyDir, ent.Name())
		final := filepath.Join(l0Dir, fmt.Sprintf("legacy-%d.parquet", nanos))
		if _, err := os.Stat(final); err == nil {
			continue
		}
		ts := time.Unix(0, nanos).UTC()
		selectSQL := fmt.Sprintf(
			`SELECT "__name__", labels, value, timestamp_ms, CAST(? AS TIMESTAMP) AS ts FROM read_parquet('%s')`,
			escapePath(src),
		)
		if err := atomicCopyTo(te.db, selectSQL, final, e.cfg.RowGroupSize, ts); err != nil {
			return fmt.Errorf("engine: legacy import %s: %w", ent.Name(), err)
		}
	}

	return e.writeLegacyImportMarker(tenant)
}

func (e *Engine) writeLegacyImportMarker(tenant string) error {
	marker := filepath.Join(e.cfg.DataDir, tenant, legacyImportMarker)
	//nolint:gosec // G306: 0640 is the store's intentional group-readable marker permission.
	return os.WriteFile(marker, []byte("done\n"), 0o640)
}

func legacyIngestNanosFromName(name string) (int64, bool) {
	m := legacyIngestNanosRe.FindStringSubmatch(name)
	if len(m) != 2 {
		return 0, false
	}
	var nanos int64
	if _, err := fmt.Sscanf(m[1], "%d", &nanos); err != nil {
		return 0, false
	}
	return nanos, nanos > 0
}

func listDataTenants(dataDir string) ([]string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var tenants []string
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			tenants = append(tenants, e.Name())
		}
	}
	sort.Strings(tenants)
	return tenants, nil
}
