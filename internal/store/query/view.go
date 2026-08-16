package query

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/prism-utils/prism/internal/store/layout"
	"github.com/prism-utils/prism/internal/store/segformat"
	storetenant "github.com/prism-utils/prism/internal/store/tenant"
)

// ViewSQL emits CREATE OR REPLACE VIEW … AS a fixed-schema union of the tenant
// hot snapshot and present tier segments for Grafana DuckDB initSQL wiring.
// Parquet sources use read_parquet; .duckdb sources use ATTACH + table scan
// (operators must ATTACH those paths before the view is usable, or prefer
// converting segments to Parquet for Grafana datasources).
func ViewSQL(dataDir, tenant string) (string, error) {
	if !storetenant.TenantAllowed(tenant) {
		return "", fmt.Errorf("query: invalid tenant %q", tenant)
	}
	tenantRoot := filepath.Join(dataDir, tenant)
	if _, err := os.Stat(tenantRoot); err != nil {
		return "", fmt.Errorf("query: tenant root: %w", err)
	}

	sources, err := collectMetricsSources(tenantRoot, metricsOpenOpts{})
	if err != nil {
		return "", err
	}
	if len(sources) == 0 {
		return "", fmt.Errorf("query: no segment sources for tenant %q", tenant)
	}

	var parts []string
	var attach []string
	for i, s := range sources {
		if segformat.IsDuckDB(s.Path) {
			alias := fmt.Sprintf("view_mseg_%d", i)
			attach = append(attach, fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY);", layout.ToSlash(s.Path), alias))
			parts = append(parts, fmt.Sprintf(
				`SELECT "__name__", labels, value, timestamp_ms, ts FROM %s.%s`,
				alias, segformat.MetricsTable,
			))
			continue
		}
		parts = append(parts, fmt.Sprintf(
			`SELECT "__name__", labels, value, timestamp_ms, ts FROM read_parquet('%s')`,
			layout.ToSlash(s.Path),
		))
	}

	union := strings.Join(parts, " UNION ALL ")
	name := viewName(tenant)
	sqlText := strings.Join(attach, "\n")
	if sqlText != "" {
		sqlText += "\n"
	}
	sqlText += fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", name, union)
	if !AssertNoUnionByName(sqlText) {
		return "", fmt.Errorf("query: view SQL must not use union_by_name or filename")
	}
	return sqlText, nil
}

func viewName(tenant string) string {
	var b strings.Builder
	b.WriteString("prism_")
	for _, r := range tenant {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '.' || r == '_':
			b.WriteRune('_')
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
