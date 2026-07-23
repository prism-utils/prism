package query

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/elk-utilities/prism/internal/store/layout"
	storetenant "github.com/elk-utilities/prism/internal/store/tenant"
)

const hotSnapshotRel = "hot/current.parquet"

// ViewSQL emits CREATE OR REPLACE VIEW … AS a fixed-schema union of the tenant
// hot snapshot and present tier parquet globs for Grafana DuckDB initSQL wiring.
func ViewSQL(dataDir, tenant string) (string, error) {
	if !storetenant.TenantAllowed(tenant) {
		return "", fmt.Errorf("query: invalid tenant %q", tenant)
	}
	tenantRoot := filepath.Join(dataDir, tenant)
	if _, err := os.Stat(tenantRoot); err != nil {
		return "", fmt.Errorf("query: tenant root: %w", err)
	}

	var parts []string
	snapshot := filepath.Join(tenantRoot, hotSnapshotRel)
	if _, err := os.Stat(snapshot); err == nil {
		parts = append(parts, fmt.Sprintf(
			`SELECT "__name__", labels, value, timestamp_ms, ts FROM read_parquet('%s')`,
			layout.ToSlash(snapshot),
		))
	}

	for tier := 0; tier < maxTier; tier++ {
		glob := filepath.Join(tenantRoot, "tiers", fmt.Sprintf("L%d", tier), "*.parquet")
		if matches, _ := filepath.Glob(glob); len(matches) > 0 {
			parts = append(parts, fmt.Sprintf(
				`SELECT "__name__", labels, value, timestamp_ms, ts FROM read_parquet('%s')`,
				layout.ToSlash(glob),
			))
		}
	}

	if len(parts) == 0 {
		return "", fmt.Errorf("query: no parquet sources for tenant %q", tenant)
	}

	union := strings.Join(parts, " UNION ALL ")
	name := viewName(tenant)
	sqlText := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", name, union)
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
