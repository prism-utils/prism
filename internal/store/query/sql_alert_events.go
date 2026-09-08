package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/prism-utils/prism/internal/store/layout"
)

const alertEventsArtifact = "alert-events"

const alertEventsSelectCols = "ts, fingerprint, alertname, severity, status, summary, description, recommendation, starts_at, ends_at, labels"

// bindAlertEventsView exposes the alert_events sandbox relation. QUERY_HOT_ONLY
// does not apply: every landed parquet window is opened. Zero files yields a
// typed empty stub so SELECT does not 400.
func bindAlertEventsView(ctx context.Context, conn *sql.Conn, tenantRoot, coldDir string) error {
	files, err := listAlertEventFiles(tenantRoot, coldDir)
	if err != nil {
		return err
	}
	body := emptyAlertEventsViewSQL
	if len(files) > 0 {
		parts := make([]string, 0, len(files))
		for _, p := range files {
			parts = append(parts, fmt.Sprintf(
				"SELECT %s FROM read_parquet(%s)",
				alertEventsSelectCols, quoteSQLPath(layout.ToSlash(p)),
			))
		}
		body = strings.Join(parts, " UNION ALL ")
	}
	//nolint:gosec // G202: view name is a package constant; body is listing-built SQL.
	q := "CREATE VIEW " + sandboxAlertEventsView + " AS " + body
	if _, err := conn.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("create %s view: %w", sandboxAlertEventsView, err)
	}
	return nil
}

func listAlertEventFiles(tenantRoot, coldDir string) ([]string, error) {
	absRoot, err := filepath.Abs(tenantRoot)
	if err != nil {
		return nil, err
	}
	if resolved, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = resolved
	}
	absRoot = filepath.Clean(absRoot)
	tenant := filepath.Base(absRoot)
	dataDir := filepath.Dir(absRoot)
	roots := layout.AllowedTenantRoots(dataDir, coldDir, tenant)

	var out []string
	for _, root := range roots {
		dir := filepath.Join(root, "alerts", alertEventsArtifact)
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".tmp") {
				continue
			}
			if !strings.HasSuffix(name, ".parquet") {
				continue
			}
			p := filepath.Join(dir, name)
			ok, err := safeTenantParquetInRoots(roots, p)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out, nil
}
